package broker

import (
	"sync"
	"time"
)

// InFlightTracker holds tasks that have been delivered to a worker but not
// yet acked/nacked. A background goroutine scans for tasks whose deadline
// has passed and hands them to the expiry callback (the broker re-enqueues
// them there, going through the normal nack path so retry accounting and
// the WAL stay consistent).
type InFlightTracker struct {
	mu    sync.Mutex
	tasks map[string]*Task

	scanEvery time.Duration
	onExpire  func(*Task)
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func NewInFlightTracker(scanEvery time.Duration, onExpire func(*Task)) *InFlightTracker {
	return &InFlightTracker{
		tasks:     make(map[string]*Task),
		scanEvery: scanEvery,
		onExpire:  onExpire,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start launches the timeout-scan goroutine.
func (tr *InFlightTracker) Start() {
	go tr.loop()
}

// Stop terminates the scan goroutine and waits for it to exit.
func (tr *InFlightTracker) Stop() {
	close(tr.stopCh)
	<-tr.doneCh
}

func (tr *InFlightTracker) Track(t *Task) {
	tr.mu.Lock()
	tr.tasks[t.ID] = t
	tr.mu.Unlock()
}

// Remove untracks a task and returns it, or nil if it wasn't in flight
// (already expired and re-enqueued, or a bogus ID).
func (tr *InFlightTracker) Remove(id string) *Task {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	t, ok := tr.tasks[id]
	if !ok {
		return nil
	}
	delete(tr.tasks, id)
	return t
}

func (tr *InFlightTracker) Len() int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return len(tr.tasks)
}

func (tr *InFlightTracker) loop() {
	defer close(tr.doneCh)
	ticker := time.NewTicker(tr.scanEvery)
	defer ticker.Stop()
	for {
		select {
		case <-tr.stopCh:
			return
		case now := <-ticker.C:
			for _, t := range tr.expired(now) {
				tr.onExpire(t)
			}
		}
	}
}

// expired removes and returns all tasks past their deadline. The callback
// runs outside the lock to avoid deadlocking with the broker.
func (tr *InFlightTracker) expired(now time.Time) []*Task {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	var out []*Task
	for id, t := range tr.tasks {
		if now.After(t.Deadline) {
			delete(tr.tasks, id)
			out = append(out, t)
		}
	}
	return out
}
