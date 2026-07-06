// Package worker is the consumer SDK: it wraps the gRPC client in a
// dequeue → handle → ack/nack loop so applications only write a handler.
package worker

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/skkompella/distqueue/gen/queuepb"
)

// Handler processes one task. Returning nil acks the task; returning an
// error nacks it (the broker retries it up to its MaxRetries, then
// dead-letters it). Delivery is at-least-once: handlers must be idempotent.
type Handler func(ctx context.Context, task *queuepb.Task) error

type Config struct {
	// Concurrency is the starting number of goroutines pulling tasks.
	// Default 1. With AutoScale it becomes the initial pool size only.
	Concurrency int
	// AutoScale makes the worker poll the broker's Stats and resize its
	// pool to advised_worker_count — the adaptive control plane's
	// recommendation, relayed by the broker. 0 from the broker means "no
	// advice" and the pool keeps its current size.
	AutoScale bool
	// MaxWorkers caps the pool regardless of advice. Default 64.
	MaxWorkers int
	// PollInterval is how often the advice is polled. Default 5s.
	PollInterval time.Duration
	// MinBackoff/MaxBackoff bound the exponential backoff used when the
	// queue is empty or the broker is unreachable. Defaults 50ms / 5s.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Logger for transport errors; nil silences them.
	Logger *log.Logger
}

type Worker struct {
	client  queuepb.TaskQueueClient
	handler Handler
	cfg     Config
}

func New(client queuepb.TaskQueueClient, handler Handler, cfg Config) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = 64
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 50 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	return &Worker{client: client, handler: handler, cfg: cfg}
}

// Run pulls and processes tasks until ctx is cancelled. It blocks.
//
// With AutoScale, Run also supervises the pool: it polls Stats every
// PollInterval and grows or shrinks the set of puller goroutines to the
// broker's advised_worker_count (clamped to [1, MaxWorkers]). Shrinking is
// graceful — a retiring goroutine finishes its in-flight task first,
// because the stop signal is only checked between iterations.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	var stops []chan struct{} // one per live goroutine; owned by this function

	spawn := func(n int) {
		for i := 0; i < n; i++ {
			stop := make(chan struct{})
			stops = append(stops, stop)
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.loop(ctx, stop)
			}()
		}
	}
	spawn(w.cfg.Concurrency)

	if w.cfg.AutoScale {
		ticker := time.NewTicker(w.cfg.PollInterval)
		defer ticker.Stop()
	supervise:
		for {
			select {
			case <-ctx.Done():
				break supervise
			case <-ticker.C:
			}
			resp, err := w.client.Stats(ctx, &queuepb.StatsRequest{})
			if err != nil {
				continue // transient; the pool keeps its size
			}
			advised := int(resp.GetAdvisedWorkerCount())
			if advised <= 0 {
				continue // no advice
			}
			target := min(advised, w.cfg.MaxWorkers)
			if target < 1 {
				target = 1
			}
			switch cur := len(stops); {
			case target > cur:
				w.logf("scaling workers %d -> %d (advised %d)", cur, target, advised)
				spawn(target - cur)
			case target < cur:
				w.logf("scaling workers %d -> %d (advised %d)", cur, target, advised)
				for _, s := range stops[target:] {
					close(s)
				}
				stops = stops[:target]
			}
		}
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context, stop <-chan struct{}) {
	backoff := w.cfg.MinBackoff
	for {
		select {
		case <-stop:
			return // retired by the autoscaler
		default:
		}
		if ctx.Err() != nil {
			return
		}
		resp, err := w.client.Dequeue(ctx, &queuepb.DequeueRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// NotFound = empty queue: expected, back off quietly.
			if status.Code(err) != codes.NotFound {
				w.logf("dequeue: %v", err)
			}
			if !sleepCtx(ctx, stop, jitter(backoff)) {
				return
			}
			backoff = min(backoff*2, w.cfg.MaxBackoff)
			continue
		}
		backoff = w.cfg.MinBackoff

		task := resp.GetTask()
		if herr := w.safeHandle(ctx, task); herr != nil {
			if _, err := w.client.Nack(ctx, &queuepb.NackRequest{TaskId: task.GetId()}); err != nil {
				w.logf("nack %s: %v", task.GetId(), err)
			}
		} else {
			if _, err := w.client.Ack(ctx, &queuepb.AckRequest{TaskId: task.GetId()}); err != nil {
				w.logf("ack %s: %v", task.GetId(), err)
			}
		}
	}
}

// safeHandle converts a handler panic into a nack instead of killing the
// whole worker process.
func (w *Worker) safeHandle(ctx context.Context, task *queuepb.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			w.logf("handler panic on task %s: %v", task.GetId(), r)
			err = errors.New("handler panicked")
		}
	}()
	return w.handler(ctx, task)
}

func (w *Worker) logf(format string, args ...any) {
	if w.cfg.Logger != nil {
		w.cfg.Logger.Printf(format, args...)
	}
}

// jitter spreads sleeps across [d/2, d) so a fleet of idle workers doesn't
// poll the broker in lockstep.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d/2)))
}

// sleepCtx waits d, returning false if the context is cancelled or the
// goroutine is retired by the autoscaler while sleeping (so an idle worker
// scales down promptly instead of after a full backoff).
func sleepCtx(ctx context.Context, stop <-chan struct{}, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}
