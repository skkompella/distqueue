package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "broker.wal"))
	cfg.MaxRetries = 3
	cfg.TaskTimeout = 50 * time.Millisecond
	cfg.ScanInterval = 10 * time.Millisecond
	return cfg
}

func newTestBroker(t *testing.T, cfg Config) *Broker {
	t.Helper()
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestApplyTuningHotReload(t *testing.T) {
	cfg := testConfig(t)
	cfg.TaskTimeout = 30 * time.Second // long enough that the scanner won't fire
	b := newTestBroker(t, cfg)

	// Tighten the timeout at runtime, then a fresh dequeue must use it.
	b.ApplyTuning(5*time.Second, 4)
	if err := b.Enqueue(&Task{ID: "t1", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	got := time.Until(task.Deadline)
	if got < 4*time.Second || got > 6*time.Second {
		t.Fatalf("expected deadline ~5s out after ApplyTuning, got %s", got)
	}

	// Zero values must leave the corresponding knob unchanged.
	b.ApplyTuning(0, 0) // no-op
	b.ApplyTuning(10*time.Second, 0)
	if err := b.Enqueue(&Task{ID: "t2", Payload: []byte("y"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task2, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(task2.Deadline); d < 9*time.Second || d > 11*time.Second {
		t.Fatalf("expected deadline ~10s out, got %s", d)
	}
}

func TestNackCounter(t *testing.T) {
	cfg := testConfig(t)
	cfg.TaskTimeout = 30 * time.Second // isolate the explicit-nack path from timeouts
	cfg.MaxRetries = 5
	b := newTestBroker(t, cfg)

	if err := b.Enqueue(&Task{ID: "n1", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if s := b.Stats(); s.Nacked != 0 {
		t.Fatalf("expected 0 nacks before nack, got %d", s.Nacked)
	}
	if err := b.Nack(task.ID); err != nil {
		t.Fatal(err)
	}
	if s := b.Stats(); s.Nacked != 1 {
		t.Fatalf("expected Nacked=1 after explicit nack, got %d", s.Nacked)
	}
}

func TestNackCounterCountsTimeouts(t *testing.T) {
	b := newTestBroker(t, testConfig(t)) // 50ms timeout, 10ms scan
	if err := b.Enqueue(&Task{ID: "slow", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Dequeue(); err != nil {
		t.Fatal(err)
	}
	// Worker never acks; the timeout scanner redelivers it, counting a nack.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if b.Stats().Nacked >= 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout redelivery never incremented the nack counter")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestEnqueueDequeueAck(t *testing.T) {
	b := newTestBroker(t, testConfig(t))

	if err := b.Enqueue(&Task{Payload: []byte("hello"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if string(task.Payload) != "hello" || task.Status != InFlight {
		t.Fatalf("unexpected task: %+v", task)
	}
	if task.ID == "" {
		t.Fatal("expected auto-assigned UUID")
	}
	if err := b.Ack(task.ID); err != nil {
		t.Fatal(err)
	}
	s := b.Stats()
	if s.Pending != 0 || s.InFlight != 0 || s.Acked != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestDequeueEmpty(t *testing.T) {
	b := newTestBroker(t, testConfig(t))
	if _, err := b.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty, got %v", err)
	}
}

func TestAckUnknownTask(t *testing.T) {
	b := newTestBroker(t, testConfig(t))
	if err := b.Ack("nope"); !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("expected ErrUnknownTask, got %v", err)
	}
}

func TestNackRequeues(t *testing.T) {
	b := newTestBroker(t, testConfig(t))
	if err := b.Enqueue(&Task{ID: "t1", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	task, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Nack(task.ID); err != nil {
		t.Fatal(err)
	}
	again, err := b.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if again.ID != "t1" || again.RetryCount != 1 {
		t.Fatalf("expected t1 with retry=1, got %+v", again)
	}
}

func TestRetriesExhaustedGoToDLQ(t *testing.T) {
	cfg := testConfig(t)
	b := newTestBroker(t, cfg)
	if err := b.Enqueue(&Task{ID: "doomed", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cfg.MaxRetries; i++ {
		task, err := b.Dequeue()
		if err != nil {
			t.Fatalf("dequeue %d: %v", i, err)
		}
		if err := b.Nack(task.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected empty queue after DLQ promotion, got %v", err)
	}
	dlq := b.ListDLQ()
	if len(dlq) != 1 || dlq[0].ID != "doomed" || dlq[0].Status != Dead {
		t.Fatalf("unexpected DLQ: %+v", dlq)
	}
}

func TestTimeoutRedelivery(t *testing.T) {
	b := newTestBroker(t, testConfig(t))
	if err := b.Enqueue(&Task{ID: "slow", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Dequeue(); err != nil {
		t.Fatal(err)
	}

	// Worker never acks. The timeout scanner must re-enqueue it.
	deadline := time.Now().Add(2 * time.Second)
	for {
		task, err := b.Dequeue()
		if err == nil {
			if task.ID != "slow" || task.RetryCount != 1 {
				t.Fatalf("expected redelivered slow task with retry=1, got %+v", task)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("task was never redelivered after timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCrashRecoveryPendingTasks(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := b.Enqueue(&Task{Payload: []byte{byte(i)}, Priority: i}); err != nil {
			t.Fatal(err)
		}
	}
	// "Crash": close without draining. WAL keeps everything.
	b.Close()

	b2 := newTestBroker(t, cfg)
	if s := b2.Stats(); s.Pending != 10 {
		t.Fatalf("expected 10 pending after recovery, got %+v", s)
	}
	// Priority order must survive recovery.
	prev := -1
	for i := 0; i < 10; i++ {
		task, err := b2.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if task.Priority < prev {
			t.Fatalf("priority order broken after recovery: %d after %d", task.Priority, prev)
		}
		prev = task.Priority
	}
}

func TestCrashRecoveryInFlightRedelivered(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(&Task{ID: "inflight", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Dequeue(); err != nil {
		t.Fatal(err)
	}
	b.Close() // crash while task is in flight, never acked

	b2 := newTestBroker(t, cfg)
	task, err := b2.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "inflight" {
		t.Fatalf("expected in-flight task redelivered after crash, got %+v", task)
	}
}

func TestCrashRecoveryAckedNotRedelivered(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(&Task{ID: "done", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(&Task{ID: "todo", Payload: []byte("y"), Priority: 2}); err != nil {
		t.Fatal(err)
	}
	task, _ := b.Dequeue()
	if err := b.Ack(task.ID); err != nil {
		t.Fatal(err)
	}
	b.Close()

	b2 := newTestBroker(t, cfg)
	if s := b2.Stats(); s.Pending != 1 {
		t.Fatalf("expected exactly 1 pending after recovery, got %+v", s)
	}
	remaining, err := b2.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if remaining.ID != "todo" {
		t.Fatalf("expected todo, got %s", remaining.ID)
	}
}

func TestDLQSurvivesRecovery(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(&Task{ID: "doomed", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cfg.MaxRetries; i++ {
		task, err := b.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Nack(task.ID); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()

	b2 := newTestBroker(t, cfg)
	dlq := b2.ListDLQ()
	if len(dlq) != 1 || dlq[0].ID != "doomed" {
		t.Fatalf("expected doomed in DLQ after recovery, got %+v", dlq)
	}
	if s := b2.Stats(); s.Pending != 0 {
		t.Fatalf("DLQ task must not be pending: %+v", s)
	}
}

// TestConcurrentEnqueueDurability hammers the group-commit path: many
// goroutines enqueue concurrently (sharing fsyncs), then the broker
// "crashes". Every enqueue that returned nil must survive recovery.
func TestConcurrentEnqueueDurability(t *testing.T) {
	cfg := testConfig(t)
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines, perG = 16, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				task := &Task{
					ID:       fmt.Sprintf("g%d-t%d", g, i),
					Payload:  []byte{byte(i)},
					Priority: i % 7,
				}
				if err := b.Enqueue(task); err != nil {
					t.Errorf("enqueue %s: %v", task.ID, err)
				}
			}
		}(g)
	}
	wg.Wait()
	b.Close() // crash

	b2 := newTestBroker(t, cfg)
	if s := b2.Stats(); s.Pending != goroutines*perG {
		t.Fatalf("expected %d pending after crash, got %+v", goroutines*perG, s)
	}
	seen := make(map[string]bool)
	for i := 0; i < goroutines*perG; i++ {
		task, err := b2.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if seen[task.ID] {
			t.Fatalf("duplicate task %s", task.ID)
		}
		seen[task.ID] = true
	}
}

func TestCompactionPreservesState(t *testing.T) {
	cfg := testConfig(t)
	cfg.CompactEvery = 10 // force frequent compaction
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Churn enough operations to trigger several compactions.
	for i := 0; i < 50; i++ {
		if err := b.Enqueue(&Task{Payload: []byte{byte(i)}, Priority: 1}); err != nil {
			t.Fatal(err)
		}
		task, err := b.Dequeue()
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Ack(task.ID); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Enqueue(&Task{ID: "leftover", Payload: []byte("x"), Priority: 1}); err != nil {
		t.Fatal(err)
	}
	b.Close()

	b2 := newTestBroker(t, cfg)
	task, err := b2.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "leftover" {
		t.Fatalf("expected leftover after compaction+recovery, got %+v", task)
	}
	if _, err := b2.Dequeue(); !errors.Is(err, ErrEmpty) {
		t.Fatal("acked tasks must not reappear after compaction")
	}
}
