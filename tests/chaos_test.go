// Package tests holds integration and chaos tests that exercise the whole
// stack: broker + WAL recovery + gRPC transport + worker SDK.
//
// "Crash" here means closing a broker without draining it and reopening the
// same WAL — the same state a kill -9 leaves behind, since every mutation
// is fsync'd before it is applied.
package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/gen/queuepb"
	"github.com/skkompella/distqueue/server"
	"github.com/skkompella/distqueue/worker"
)

func chaosConfig(t *testing.T) broker.Config {
	t.Helper()
	cfg := broker.DefaultConfig(filepath.Join(t.TempDir(), "chaos.wal"))
	cfg.TaskTimeout = 100 * time.Millisecond
	cfg.ScanInterval = 10 * time.Millisecond
	cfg.MaxRetries = 100 // chaos tests want redelivery, not dead-lettering
	return cfg
}

// TestBrokerCrashRecovery: enqueue 1000 tasks, crash the broker after 500,
// restart, verify all 1000 are still queued.
func TestBrokerCrashRecovery(t *testing.T) {
	cfg := chaosConfig(t)
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err := b.Enqueue(&broker.Task{Payload: []byte{byte(i)}, Priority: i % 5}); err != nil {
			t.Fatal(err)
		}
	}
	b.Close() // crash

	b, err = broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for i := 500; i < 1000; i++ {
		if err := b.Enqueue(&broker.Task{Payload: []byte{byte(i)}, Priority: i % 5}); err != nil {
			t.Fatal(err)
		}
	}
	if s := b.Stats(); s.Pending != 1000 {
		t.Fatalf("expected 1000 pending after crash+recovery, got %+v", s)
	}
	for i := 0; i < 1000; i++ {
		if _, err := b.Dequeue(); err != nil {
			t.Fatalf("dequeue %d after recovery: %v", i, err)
		}
	}
}

// TestWorkerCrashMidExecution: 100 tasks, a worker dequeues 50 and dies
// without acking. The timeout scanner redelivers them and a second worker
// finishes the job — every task completes (at least once).
func TestWorkerCrashMidExecution(t *testing.T) {
	cfg := chaosConfig(t)
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for i := 0; i < 100; i++ {
		if err := b.Enqueue(&broker.Task{ID: idOf(i), Payload: []byte{byte(i)}, Priority: 1}); err != nil {
			t.Fatal(err)
		}
	}

	// Crashing worker: takes 50 tasks and never acks.
	for i := 0; i < 50; i++ {
		if _, err := b.Dequeue(); err != nil {
			t.Fatal(err)
		}
	}

	// Surviving worker: drains everything, acking as it goes.
	completed := make(map[string]bool)
	deadline := time.Now().Add(10 * time.Second)
	for len(completed) < 100 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/100 tasks completed before deadline", len(completed))
		}
		task, err := b.Dequeue()
		if err != nil {
			time.Sleep(5 * time.Millisecond) // waiting on timeout redelivery
			continue
		}
		if err := b.Ack(task.ID); err != nil {
			t.Fatalf("ack %s: %v", task.ID, err)
		}
		completed[task.ID] = true
	}
}

// TestBrokerCrashWithInFlight: tasks in flight at crash time must be
// redelivered after restart (at-least-once across process death).
func TestBrokerCrashWithInFlight(t *testing.T) {
	cfg := chaosConfig(t)
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := b.Enqueue(&broker.Task{ID: idOf(i), Payload: []byte{byte(i)}, Priority: 1}); err != nil {
			t.Fatal(err)
		}
	}
	// 10 in flight, 5 of them acked, then crash.
	var inflight []string
	for i := 0; i < 10; i++ {
		task, err := b.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		inflight = append(inflight, task.ID)
	}
	for _, id := range inflight[:5] {
		if err := b.Ack(id); err != nil {
			t.Fatal(err)
		}
	}
	b.Close() // crash

	b, err = broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// 20 - 5 acked = 15 must come back (10 never dequeued + 5 unacked in-flight).
	if s := b.Stats(); s.Pending != 15 {
		t.Fatalf("expected 15 pending after recovery, got %+v", s)
	}
	seen := make(map[string]bool)
	for i := 0; i < 15; i++ {
		task, err := b.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		seen[task.ID] = true
	}
	for _, id := range inflight[:5] {
		if seen[id] {
			t.Fatalf("acked task %s was redelivered after recovery", id)
		}
	}
	for _, id := range inflight[5:] {
		if !seen[id] {
			t.Fatalf("unacked in-flight task %s was lost in the crash", id)
		}
	}
}

// TestEndToEndGRPC runs the full stack — broker, gRPC server, worker SDK —
// over an in-memory bufconn listener: 200 tasks, 4 concurrent handlers,
// 10%% of handler calls fail (and are retried), all tasks complete.
func TestEndToEndGRPC(t *testing.T) {
	cfg := chaosConfig(t)
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	lis := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	server.New(b).Register(grpcServer)
	go grpcServer.Serve(lis)
	defer grpcServer.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := queuepb.NewTaskQueueClient(conn)

	const total = 200
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < total; i++ {
		if _, err := client.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
			Id: idOf(i), Payload: []byte{byte(i)}, Priority: int32(i % 3),
		}}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	done := make(map[string]bool)
	var failures atomic.Int64
	handler := func(_ context.Context, task *queuepb.Task) error {
		// Fail each task once on its first delivery: exercises nack+retry.
		if task.GetRetryCount() == 0 && int(task.GetPayload()[0])%10 == 0 {
			failures.Add(1)
			return errors.New("transient failure")
		}
		mu.Lock()
		done[task.GetId()] = true
		mu.Unlock()
		return nil
	}

	w := worker.New(client, handler, worker.Config{
		Concurrency: 4,
		MinBackoff:  time.Millisecond,
		MaxBackoff:  20 * time.Millisecond,
	})
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	go w.Run(workerCtx)

	deadline := time.Now().Add(25 * time.Second)
	for {
		mu.Lock()
		n := len(done)
		mu.Unlock()
		if n == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d tasks completed; %d injected failures", n, total, failures.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if failures.Load() == 0 {
		t.Fatal("failure injection never fired; retry path untested")
	}
	if s := b.Stats(); s.DLQ != 0 {
		t.Fatalf("no task should be dead-lettered, got %+v", s)
	}
}

func idOf(i int) string {
	return fmt.Sprintf("task-%03d", i)
}
