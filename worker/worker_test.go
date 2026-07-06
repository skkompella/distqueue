package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/skkompella/distqueue/gen/queuepb"
)

// fakeClient is an in-memory TaskQueueClient: Dequeue serves an endless
// backlog, Stats returns a settable advised worker count. No gRPC.
type fakeClient struct {
	advised atomic.Int32
	seq     atomic.Int64
	empty   atomic.Bool // when true, Dequeue returns NotFound
}

var _ queuepb.TaskQueueClient = (*fakeClient)(nil)

func (f *fakeClient) Dequeue(ctx context.Context, _ *queuepb.DequeueRequest, _ ...grpc.CallOption) (*queuepb.DequeueResponse, error) {
	if f.empty.Load() {
		return nil, status.Error(codes.NotFound, "queue is empty")
	}
	id := f.seq.Add(1)
	return &queuepb.DequeueResponse{Task: &queuepb.Task{
		Id: fmt.Sprintf("t-%d", id), Payload: []byte("x"), Priority: 1,
	}}, nil
}

func (f *fakeClient) Stats(ctx context.Context, _ *queuepb.StatsRequest, _ ...grpc.CallOption) (*queuepb.StatsResponse, error) {
	return &queuepb.StatsResponse{AdvisedWorkerCount: f.advised.Load()}, nil
}

func (f *fakeClient) Enqueue(ctx context.Context, _ *queuepb.EnqueueRequest, _ ...grpc.CallOption) (*queuepb.EnqueueResponse, error) {
	return &queuepb.EnqueueResponse{}, nil
}

func (f *fakeClient) Ack(ctx context.Context, _ *queuepb.AckRequest, _ ...grpc.CallOption) (*queuepb.AckResponse, error) {
	return &queuepb.AckResponse{}, nil
}

func (f *fakeClient) Nack(ctx context.Context, _ *queuepb.NackRequest, _ ...grpc.CallOption) (*queuepb.NackResponse, error) {
	return &queuepb.NackResponse{}, nil
}

func (f *fakeClient) ListDLQ(ctx context.Context, _ *queuepb.ListDLQRequest, _ ...grpc.CallOption) (*queuepb.ListDLQResponse, error) {
	return &queuepb.ListDLQResponse{}, nil
}

// gauge tracks current and high-water concurrent handler executions.
type gauge struct {
	mu   sync.Mutex
	cur  int
	high int
}

func (g *gauge) enter() {
	g.mu.Lock()
	g.cur++
	if g.cur > g.high {
		g.high = g.cur
	}
	g.mu.Unlock()
}

func (g *gauge) exit() {
	g.mu.Lock()
	g.cur--
	g.mu.Unlock()
}

func (g *gauge) snapshot() (cur, high int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur, g.high
}

// startWorker runs an autoscaling worker whose handler blocks ~5ms per
// task (so concurrency is observable) and returns the gauge + a stopper.
func startWorker(t *testing.T, fc *fakeClient, initial, maxWorkers int) (*gauge, context.CancelFunc, *sync.WaitGroup) {
	t.Helper()
	g := &gauge{}
	handler := func(ctx context.Context, _ *queuepb.Task) error {
		g.enter()
		defer g.exit()
		time.Sleep(5 * time.Millisecond)
		return nil
	}
	w := New(fc, handler, Config{
		Concurrency:  initial,
		AutoScale:    true,
		MaxWorkers:   maxWorkers,
		PollInterval: 20 * time.Millisecond,
		MinBackoff:   time.Millisecond,
		MaxBackoff:   10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Run(ctx)
	}()
	return g, cancel, &wg
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestAutoScaleGrows(t *testing.T) {
	fc := &fakeClient{}
	fc.advised.Store(8)
	g, cancel, wg := startWorker(t, fc, 2, 64)
	defer func() { cancel(); wg.Wait() }()

	// With an endless backlog and 5ms handlers, high-water concurrency
	// reaching 8 requires 8 goroutines to exist.
	waitFor(t, "pool to grow to 8", 5*time.Second, func() bool {
		_, high := g.snapshot()
		return high >= 8
	})
}

func TestAutoScaleShrinks(t *testing.T) {
	fc := &fakeClient{}
	fc.advised.Store(8)
	g, cancel, wg := startWorker(t, fc, 8, 64)
	defer func() { cancel(); wg.Wait() }()

	waitFor(t, "warm-up to 8", 5*time.Second, func() bool {
		_, high := g.snapshot()
		return high >= 8
	})

	fc.advised.Store(2)
	// After the retirement round trip, concurrency must settle at <= 2 and
	// stay there across several observation windows.
	waitFor(t, "pool to shrink to 2", 5*time.Second, func() bool {
		for i := 0; i < 10; i++ {
			if cur, _ := g.snapshot(); cur > 2 {
				return false
			}
			time.Sleep(2 * time.Millisecond)
		}
		return true
	})
}

func TestAutoScaleIgnoresZeroAdvice(t *testing.T) {
	fc := &fakeClient{} // advised stays 0 = no advice
	g, cancel, wg := startWorker(t, fc, 3, 64)
	defer func() { cancel(); wg.Wait() }()

	waitFor(t, "initial pool of 3 to be busy", 5*time.Second, func() bool {
		_, high := g.snapshot()
		return high >= 3
	})
	// Give the supervisor several poll cycles; the pool must not collapse.
	time.Sleep(150 * time.Millisecond)
	if cur, high := g.snapshot(); high > 3 || cur == 0 {
		t.Fatalf("zero advice must leave the pool alone: cur=%d high=%d", cur, high)
	}
}

func TestAutoScaleRespectsMaxWorkers(t *testing.T) {
	fc := &fakeClient{}
	fc.advised.Store(50)
	g, cancel, wg := startWorker(t, fc, 1, 4)
	defer func() { cancel(); wg.Wait() }()

	waitFor(t, "pool to reach the cap of 4", 5*time.Second, func() bool {
		_, high := g.snapshot()
		return high >= 4
	})
	time.Sleep(150 * time.Millisecond) // several polls past the cap
	if _, high := g.snapshot(); high > 4 {
		t.Fatalf("pool exceeded MaxWorkers: high=%d", high)
	}
}

func TestScaleDownWhileIdle(t *testing.T) {
	// Retiring goroutines must exit promptly from their idle backoff sleep,
	// not only when a task arrives.
	fc := &fakeClient{}
	fc.empty.Store(true)
	fc.advised.Store(6)
	_, cancel, wg := startWorker(t, fc, 6, 64)

	time.Sleep(100 * time.Millisecond) // let it idle at 6
	fc.advised.Store(1)
	time.Sleep(100 * time.Millisecond) // poll + retire

	// Cancel and require the whole pool to unwind quickly — retired
	// goroutines stuck in a sleep would hang this.
	done := make(chan struct{})
	go func() { cancel(); wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker pool did not unwind after scale-down + cancel")
	}
}
