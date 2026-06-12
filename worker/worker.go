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

	"github.com/srihari-kompella/distqueue/gen/queuepb"
)

// Handler processes one task. Returning nil acks the task; returning an
// error nacks it (the broker retries it up to its MaxRetries, then
// dead-letters it). Delivery is at-least-once: handlers must be idempotent.
type Handler func(ctx context.Context, task *queuepb.Task) error

type Config struct {
	// Concurrency is the number of goroutines pulling tasks. Default 1.
	Concurrency int
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
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 50 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	return &Worker{client: client, handler: handler, cfg: cfg}
}

// Run pulls and processes tasks until ctx is cancelled. It blocks.
func (w *Worker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.loop(ctx)
		}()
	}
	wg.Wait()
}

func (w *Worker) loop(ctx context.Context) {
	backoff := w.cfg.MinBackoff
	for {
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
			if !sleepCtx(ctx, jitter(backoff)) {
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

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
