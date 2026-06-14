package broker

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"
)

// QueueOp is one replicated queue command, gob-encoded into a Raft log
// entry. In cluster mode the Raft log replaces the Phase 1 WAL.
//
// There is no replicated "dead" op: DLQ promotion is decided
// deterministically inside Apply (RetryCount vs MaxRetries are part of
// replicated state), so every replica reaches the same verdict.
type QueueOp struct {
	Type   byte // OpEnqueue, OpAck, OpNack (WAL constants reused)
	Task   *Task
	TaskID string
}

func EncodeOp(op QueueOp) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		return nil, fmt.Errorf("statemachine: encode op: %w", err)
	}
	return buf.Bytes(), nil
}

func DecodeOp(b []byte) (QueueOp, error) {
	var op QueueOp
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&op); err != nil {
		return QueueOp{}, fmt.Errorf("statemachine: decode op: %w", err)
	}
	return op, nil
}

// StateMachine is the deterministic queue state every replica maintains by
// applying committed QueueOps in log order. It is the Phase 1 broker minus
// the WAL (Raft provides durability + replication) and minus the in-flight
// tracker (delivery is a leader-local concern; see Dequeue).
type StateMachine struct {
	mu         sync.Mutex
	maxRetries int

	tasks  map[string]*Task // live tasks (Pending, or InFlight on the leader)
	pq     *PriorityQueue   // may hold stale pointers; validated on pop
	inHeap map[string]bool  // tracks heap membership to avoid duplicates
	dlq    []*Task
	acked  int64
	nacked int64
}

func NewStateMachine(maxRetries int) *StateMachine {
	return &StateMachine{
		maxRetries: maxRetries,
		tasks:      make(map[string]*Task),
		pq:         NewPriorityQueue(),
		inHeap:     make(map[string]bool),
	}
}

// Apply executes one committed op. It must be called in log order and is
// the only mutation path shared by all replicas.
func (sm *StateMachine) Apply(op QueueOp) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	switch op.Type {
	case OpEnqueue:
		t := op.Task
		if t == nil || t.ID == "" {
			return
		}
		if _, exists := sm.tasks[t.ID]; exists {
			return // duplicate of a live task (client retry): idempotent
		}
		t.Status = Pending
		sm.tasks[t.ID] = t
		sm.pushLocked(t)
	case OpAck:
		t, ok := sm.tasks[op.TaskID]
		if !ok {
			return // already acked/dead: idempotent
		}
		t.Status = Done
		delete(sm.tasks, op.TaskID)
		sm.acked++
	case OpNack:
		t, ok := sm.tasks[op.TaskID]
		if !ok {
			return
		}
		sm.nacked++
		t.RetryCount++
		if t.RetryCount >= sm.maxRetries {
			t.Status = Dead
			delete(sm.tasks, op.TaskID)
			sm.dlq = append(sm.dlq, t)
			return
		}
		t.Status = Pending
		t.Deadline = time.Time{}
		sm.pushLocked(t)
	}
}

func (sm *StateMachine) pushLocked(t *Task) {
	if !sm.inHeap[t.ID] {
		sm.pq.Push(t)
		sm.inHeap[t.ID] = true
	}
}

// Dequeue hands out the highest-priority pending task. This is leader-only
// and deliberately NOT replicated: followers keep the task pending, so if
// the leader dies before the ack commits, the next leader simply
// redelivers — the same at-least-once contract as Phase 1, now spanning
// leader failover.
func (sm *StateMachine) Dequeue(timeout time.Duration) (*Task, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for {
		t := sm.pq.Pop()
		if t == nil {
			return nil, false
		}
		delete(sm.inHeap, t.ID)
		// Skip stale heap entries: acked/dead tasks, or ones already
		// handed out (leader overlay).
		if live, ok := sm.tasks[t.ID]; !ok || live != t || t.Status != Pending {
			continue
		}
		t.Status = InFlight
		t.Deadline = time.Now().Add(timeout)
		return t.Clone(), true
	}
}

// Has reports whether the task is live (pending or in flight) on this
// replica — used by the server to reject acks for unknown tasks without
// burning a log entry.
func (sm *StateMachine) Has(taskID string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_, ok := sm.tasks[taskID]
	return ok
}

// RequeueInFlight returns every in-flight task to pending. Called when
// this node loses leadership: the dequeue overlay is leader-local state,
// and a future term's dequeues must see these tasks again.
func (sm *StateMachine) RequeueInFlight() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, t := range sm.tasks {
		if t.Status == InFlight {
			t.Status = Pending
			t.Deadline = time.Time{}
			sm.pushLocked(t)
		}
	}
}

func (sm *StateMachine) Stats() Stats {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	pending, inflight := 0, 0
	for _, t := range sm.tasks {
		if t.Status == InFlight {
			inflight++
		} else {
			pending++
		}
	}
	return Stats{Pending: pending, InFlight: inflight, DLQ: len(sm.dlq), Acked: sm.acked, Nacked: sm.nacked}
}

func (sm *StateMachine) ListDLQ() []*Task {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	out := make([]*Task, len(sm.dlq))
	for i, t := range sm.dlq {
		out[i] = t.Clone()
	}
	return out
}

// snapshotState is the serialized form handed to Raft for log compaction.
type snapshotState struct {
	Tasks []*Task
	DLQ   []*Task
	Acked int64
}

// Snapshot serializes the full queue state. In-flight status is a
// leader-local overlay, so tasks are captured as pending — a replica
// restoring this snapshot redelivers them, consistent with failover.
func (sm *StateMachine) Snapshot() ([]byte, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	st := snapshotState{DLQ: sm.dlq, Acked: sm.acked}
	for _, t := range sm.tasks {
		c := t.Clone()
		c.Status = Pending
		c.Deadline = time.Time{}
		st.Tasks = append(st.Tasks, c)
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		return nil, fmt.Errorf("statemachine: snapshot: %w", err)
	}
	return buf.Bytes(), nil
}

// Restore replaces all state from a snapshot (startup, or an
// InstallSnapshot from the leader after this replica fell too far behind).
func (sm *StateMachine) Restore(data []byte) error {
	var st snapshotState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return fmt.Errorf("statemachine: restore: %w", err)
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.tasks = make(map[string]*Task, len(st.Tasks))
	sm.pq = NewPriorityQueue()
	sm.inHeap = make(map[string]bool, len(st.Tasks))
	for _, t := range st.Tasks {
		sm.tasks[t.ID] = t
		sm.pushLocked(t)
	}
	sm.dlq = st.DLQ
	sm.acked = st.Acked
	return nil
}
