package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// ErrEmpty is returned by Dequeue when no task is pending. Workers
	// should back off and retry.
	ErrEmpty = errors.New("broker: queue is empty")
	// ErrUnknownTask is returned by Ack/Nack for an ID that isn't in
	// flight (already acked, timed out and re-enqueued, or never existed).
	ErrUnknownTask = errors.New("broker: unknown or not-in-flight task")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("broker: closed")
)

type Config struct {
	MaxRetries   int           // nacks/timeouts before a task is dead-lettered
	TaskTimeout  time.Duration // how long a worker may hold a task before redelivery
	WALPath      string
	CompactEvery int           // compact the WAL every N appends (0 = never)
	ScanInterval time.Duration // in-flight timeout scan period (default 1s)
}

func DefaultConfig(walPath string) Config {
	return Config{
		MaxRetries:   5,
		TaskTimeout:  30 * time.Second,
		WALPath:      walPath,
		CompactEvery: 10000,
		ScanInterval: time.Second,
	}
}

// Broker is the single-node queue: a priority heap of pending tasks, an
// in-flight tracker with timeout redelivery, a dead-letter queue, and a WAL
// that makes every state mutation durable before it is applied in memory.
//
// Delivery semantics are at-least-once: Dequeue is deliberately NOT written
// to the WAL, so a crash with tasks in flight replays them as pending and
// they are redelivered. Handlers must be idempotent.
type Broker struct {
	mu      sync.Mutex
	pq      *PriorityQueue
	tracker *InFlightTracker
	dlq     []*Task
	wal     *WAL
	cfg     Config
	acked   int64 // lifetime acks, for stats
	nacked  int64 // lifetime nacks + timeout redeliveries, for stats
	// advisedWorkers is the control plane's recommended worker concurrency,
	// served to workers via Stats. 0 = no advice. The broker itself never
	// acts on it; it is a relay from broker.conf to polling workers.
	advisedWorkers int
	closed         bool
}

func New(cfg Config) (*Broker, error) {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = time.Second
	}
	wal, err := OpenWAL(cfg.WALPath)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		pq:  NewPriorityQueue(),
		wal: wal,
		cfg: cfg,
	}
	b.tracker = NewInFlightTracker(cfg.ScanInterval, b.onTimeout)

	if err := b.recover(); err != nil {
		wal.Close()
		return nil, err
	}
	b.tracker.Start()
	return b, nil
}

// recover rebuilds in-memory state from the WAL. Tasks that were in flight
// at crash time have no terminal record, so they come back as pending —
// that is the at-least-once redelivery path.
func (b *Broker) recover() error {
	entries, err := b.wal.Replay()
	if err != nil {
		return err
	}

	live := make(map[string]*Task)
	var dlq []*Task
	for _, e := range entries {
		switch e.Op {
		case OpEnqueue:
			t := e.Task
			t.Status = Pending
			t.Deadline = time.Time{}
			live[t.ID] = t
		case OpAck:
			delete(live, e.TaskID)
		case OpNack:
			if t, ok := live[e.TaskID]; ok {
				t.RetryCount++
			}
		case OpDead:
			if t, ok := live[e.TaskID]; ok {
				t.Status = Dead
				dlq = append(dlq, t)
				delete(live, e.TaskID)
			}
		}
	}

	for _, t := range live {
		b.pq.Push(t)
	}
	b.dlq = dlq

	// Recovery already folded the log down to live state; persist that
	// compact form so replay cost doesn't accumulate across restarts.
	if len(entries) > 0 {
		return b.compactLocked()
	}
	return nil
}

func (b *Broker) Enqueue(t *Task) error {
	if t.ID == "" {
		t.ID = uuid.NewString()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	t.Status = Pending

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	seq, err := b.wal.AppendTaskNoSync(OpEnqueue, t)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	b.pq.Push(t)
	cerr := b.maybeCompactLocked()
	b.mu.Unlock()

	if cerr != nil {
		return cerr
	}
	// Durability wait happens outside the broker lock so concurrent
	// callers share one group-committed fsync (see WAL.WaitSync).
	return b.wal.WaitSync(seq)
}

func (b *Broker) Dequeue() (*Task, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	t := b.pq.Pop()
	if t == nil {
		return nil, ErrEmpty
	}
	t.Status = InFlight
	t.Deadline = time.Now().Add(b.cfg.TaskTimeout)
	b.tracker.Track(t)
	// Intentionally no WAL append here — see at-least-once note on Broker.
	return t.Clone(), nil
}

func (b *Broker) Ack(taskID string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	t := b.tracker.Remove(taskID)
	if t == nil {
		b.mu.Unlock()
		return ErrUnknownTask
	}
	seq, err := b.wal.AppendIDNoSync(OpAck, taskID)
	if err != nil {
		// Record never hit the log: put the task back so it isn't lost.
		b.tracker.Track(t)
		b.mu.Unlock()
		return err
	}
	t.Status = Done
	b.acked++
	cerr := b.maybeCompactLocked()
	b.mu.Unlock()

	if cerr != nil {
		return cerr
	}
	return b.wal.WaitSync(seq)
}

func (b *Broker) Nack(taskID string) error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	t := b.tracker.Remove(taskID)
	if t == nil {
		b.mu.Unlock()
		return ErrUnknownTask
	}
	seq, err := b.requeueLocked(t)
	b.mu.Unlock()

	if err != nil {
		return err
	}
	return b.wal.WaitSync(seq)
}

// onTimeout is the tracker's expiry callback: an expired in-flight task is
// treated exactly like a nack.
func (b *Broker) onTimeout(t *Task) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	seq, err := b.requeueLocked(t) // WAL errors here have no caller to surface to
	b.mu.Unlock()

	if err == nil {
		_ = b.wal.WaitSync(seq)
	}
}

// requeueLocked is the shared nack/timeout path: bump the retry count, then
// either dead-letter or re-enqueue. Caller holds b.mu and must WaitSync on
// the returned sequence number after releasing it.
func (b *Broker) requeueLocked(t *Task) (uint64, error) {
	b.nacked++ // every nack and timeout redelivery counts as a tuning signal
	t.RetryCount++
	if t.RetryCount >= b.cfg.MaxRetries {
		seq, err := b.wal.AppendIDNoSync(OpDead, t.ID)
		if err != nil {
			b.tracker.Track(t)
			return 0, err
		}
		t.Status = Dead
		b.dlq = append(b.dlq, t)
		return seq, b.maybeCompactLocked()
	}
	seq, err := b.wal.AppendIDNoSync(OpNack, t.ID)
	if err != nil {
		b.tracker.Track(t)
		return 0, err
	}
	t.Status = Pending
	t.Deadline = time.Time{}
	b.pq.Push(t)
	return seq, b.maybeCompactLocked()
}

// ApplyTuning hot-updates the two runtime-tunable knobs without a restart.
// It is safe to call concurrently: both fields are only ever read while
// holding b.mu (TaskTimeout in Dequeue, MaxRetries in requeueLocked).
//
// Only these two are reloadable — WALPath and ScanInterval are baked into
// the WAL handle and the tracker ticker at construction, so they are
// deliberately left untouched here. taskTimeout <= 0 or maxRetries <= 0
// leaves the corresponding field unchanged.
func (b *Broker) ApplyTuning(taskTimeout time.Duration, maxRetries int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if taskTimeout > 0 {
		b.cfg.TaskTimeout = taskTimeout
	}
	if maxRetries > 0 {
		b.cfg.MaxRetries = maxRetries
	}
}

// SetAdvisedWorkers records the control plane's recommended worker
// concurrency (from broker.conf on SIGHUP). n <= 0 clears the advice.
func (b *Broker) SetAdvisedWorkers(n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 {
		n = 0
	}
	b.advisedWorkers = n
}

type Stats struct {
	Pending        int
	InFlight       int
	DLQ            int
	Acked          int64
	Nacked         int64
	AdvisedWorkers int // 0 = no advice
}

func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		Pending:        b.pq.Len(),
		InFlight:       b.tracker.Len(),
		DLQ:            len(b.dlq),
		Acked:          b.acked,
		Nacked:         b.nacked,
		AdvisedWorkers: b.advisedWorkers,
	}
}

// ListDLQ returns copies of the dead-lettered tasks.
func (b *Broker) ListDLQ() []*Task {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*Task, len(b.dlq))
	for i, t := range b.dlq {
		out[i] = t.Clone()
	}
	return out
}

func (b *Broker) maybeCompactLocked() error {
	if b.cfg.CompactEvery <= 0 || b.wal.EntriesSinceOpen() < b.cfg.CompactEvery {
		return nil
	}
	return b.compactLocked()
}

// compactLocked rewrites the WAL to contain only live state: one OpEnqueue
// per pending/in-flight task (in-flight tasks are live — they'd be
// redelivered after a crash) and the DLQ. Caller holds b.mu.
func (b *Broker) compactLocked() error {
	var entries []LogEntry
	for _, t := range b.pq.h {
		entries = append(entries, LogEntry{Op: OpEnqueue, Task: t, TaskID: t.ID})
	}
	b.tracker.mu.Lock()
	for _, t := range b.tracker.tasks {
		entries = append(entries, LogEntry{Op: OpEnqueue, Task: t, TaskID: t.ID})
	}
	b.tracker.mu.Unlock()
	for _, t := range b.dlq {
		entries = append(entries,
			LogEntry{Op: OpEnqueue, Task: t, TaskID: t.ID},
			LogEntry{Op: OpDead, TaskID: t.ID})
	}
	if err := b.wal.Rewrite(entries); err != nil {
		return fmt.Errorf("broker: compact: %w", err)
	}
	return nil
}

// Close stops the timeout scanner and closes the WAL. In-flight and pending
// tasks remain durable in the WAL and are recovered on next startup.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	b.tracker.Stop()
	return b.wal.Close()
}
