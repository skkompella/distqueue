// ClusterServer serves the TaskQueue API backed by a Raft-replicated
// state machine instead of a local WAL'd broker. Writes (enqueue, ack,
// nack) are proposed to the Raft log and acknowledged only after they
// commit and apply; dequeues are leader-local (see StateMachine.Dequeue).
package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/gen/queuepb"
	"github.com/skkompella/distqueue/raft"
)

func newTaskID() string { return uuid.NewString() }

type ClusterConfig struct {
	TaskTimeout       time.Duration     // redeliver unacked tasks after this
	MaxRetries        int               // nacks/timeouts before dead-lettering
	SnapshotThreshold int               // snapshot every N applied entries (0 = never)
	ClientAddrs       map[string]string // node ID → client-facing address, for redirects
	ScanInterval      time.Duration     // in-flight timeout scan period
}

type applyOutcome struct {
	term int // term of the entry that actually landed at the index
}

type ClusterServer struct {
	queuepb.UnimplementedTaskQueueServer
	node *raft.Node
	sm   *broker.StateMachine
	cfg  ClusterConfig

	mu        sync.Mutex
	waiters   map[int]chan applyOutcome // log index → waiting RPC
	inflight  map[string]time.Time      // taskID → redelivery deadline (leader-local)
	wasLeader bool

	appliedSinceSnap int
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

func NewClusterServer(node *raft.Node, sm *broker.StateMachine, cfg ClusterConfig) *ClusterServer {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = time.Second
	}
	return &ClusterServer{
		node:     node,
		sm:       sm,
		cfg:      cfg,
		waiters:  make(map[int]chan applyOutcome),
		inflight: make(map[string]time.Time),
		stopCh:   make(chan struct{}),
	}
}

// Register attaches the TaskQueue service to a gRPC server.
func (s *ClusterServer) Register(g *grpc.Server) {
	queuepb.RegisterTaskQueueServer(g, s)
}

// Run drives the apply loop and background scanners. It returns when the
// apply channel closes or Stop is called.
func (s *ClusterServer) Run(applyCh chan raft.ApplyMsg) {
	s.wg.Add(2)
	go s.timeoutLoop()
	go s.leadershipWatch()
	s.applyLoop(applyCh)
}

func (s *ClusterServer) Stop() {
	s.mu.Lock()
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// applyLoop is the single consumer of committed entries: it mutates the
// state machine, wakes any RPC waiting on that index, and triggers
// snapshots.
func (s *ClusterServer) applyLoop(applyCh chan raft.ApplyMsg) {
	for {
		var msg raft.ApplyMsg
		select {
		case <-s.stopCh:
			return
		case m, ok := <-applyCh:
			if !ok {
				return
			}
			msg = m
		}

		if msg.SnapshotValid {
			if err := s.sm.Restore(msg.Snapshot); err != nil {
				panic(fmt.Sprintf("cluster: restore snapshot at %d: %v", msg.SnapshotIndex, err))
			}
			s.mu.Lock()
			s.appliedSinceSnap = 0
			s.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}

		if len(msg.Command) > 0 { // leader no-ops carry no command
			op, err := broker.DecodeOp(msg.Command)
			if err != nil {
				panic(fmt.Sprintf("cluster: corrupt command at index %d: %v", msg.CommandIndex, err))
			}
			s.sm.Apply(op)
		}

		s.mu.Lock()
		if ch, ok := s.waiters[msg.CommandIndex]; ok {
			delete(s.waiters, msg.CommandIndex)
			ch <- applyOutcome{term: msg.CommandTerm}
		}
		s.appliedSinceSnap++
		needSnap := s.cfg.SnapshotThreshold > 0 && s.appliedSinceSnap >= s.cfg.SnapshotThreshold
		if needSnap {
			s.appliedSinceSnap = 0
		}
		s.mu.Unlock()

		if needSnap {
			data, err := s.sm.Snapshot()
			if err != nil {
				panic(fmt.Sprintf("cluster: snapshot: %v", err))
			}
			s.node.Snapshot(msg.CommandIndex, data)
		}
	}
}

var errNotLeader = errors.New("not leader")

// propose submits an op to the Raft log and blocks until it commits and
// applies locally (or leadership is lost).
func (s *ClusterServer) propose(op broker.QueueOp) error {
	cmd, err := broker.EncodeOp(op)
	if err != nil {
		return err
	}
	index, term, isLeader := s.node.Propose(cmd)
	if !isLeader {
		return errNotLeader
	}

	ch := make(chan applyOutcome, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()

	select {
	case out := <-ch:
		if out.term != term {
			// A different leader's entry landed at our index: ours was
			// discarded in a leadership change.
			return errNotLeader
		}
		return nil
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
		return status.Error(codes.DeadlineExceeded, "commit timed out (lost quorum?)")
	case <-s.stopCh:
		return status.Error(codes.Unavailable, "shutting down")
	}
}

// redirect converts errNotLeader into a FailedPrecondition carrying the
// leader's client address, which the client SDK follows.
func (s *ClusterServer) redirect() error {
	hint := s.node.LeaderHint()
	if addr, ok := s.cfg.ClientAddrs[hint]; ok && hint != "" {
		return status.Errorf(codes.FailedPrecondition, "not leader; leader=%s", addr)
	}
	return status.Error(codes.FailedPrecondition, "not leader; leader unknown")
}

// --- TaskQueue RPCs ---

func (s *ClusterServer) Enqueue(_ context.Context, req *queuepb.EnqueueRequest) (*queuepb.EnqueueResponse, error) {
	if req.GetTask() == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	t := fromProto(req.GetTask())
	if t.ID == "" {
		t.ID = newTaskID()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	if err := s.propose(broker.QueueOp{Type: broker.OpEnqueue, Task: t}); err != nil {
		if errors.Is(err, errNotLeader) {
			return nil, s.redirect()
		}
		return nil, err
	}
	return &queuepb.EnqueueResponse{TaskId: t.ID}, nil
}

func (s *ClusterServer) Dequeue(_ context.Context, _ *queuepb.DequeueRequest) (*queuepb.DequeueResponse, error) {
	if _, isLeader := s.node.State(); !isLeader {
		return nil, s.redirect()
	}
	t, ok := s.sm.Dequeue(s.cfg.TaskTimeout)
	if !ok {
		return nil, status.Error(codes.NotFound, "queue is empty")
	}
	s.mu.Lock()
	s.inflight[t.ID] = t.Deadline
	s.mu.Unlock()
	return &queuepb.DequeueResponse{Task: toProto(t)}, nil
}

func (s *ClusterServer) Ack(_ context.Context, req *queuepb.AckRequest) (*queuepb.AckResponse, error) {
	if err := s.finish(broker.OpAck, req.GetTaskId()); err != nil {
		return nil, err
	}
	return &queuepb.AckResponse{}, nil
}

func (s *ClusterServer) Nack(_ context.Context, req *queuepb.NackRequest) (*queuepb.NackResponse, error) {
	if err := s.finish(broker.OpNack, req.GetTaskId()); err != nil {
		return nil, err
	}
	return &queuepb.NackResponse{}, nil
}

// finish is the shared ack/nack path: validate, replicate, untrack.
func (s *ClusterServer) finish(opType byte, taskID string) error {
	if _, isLeader := s.node.State(); !isLeader {
		return s.redirect()
	}
	if !s.sm.Has(taskID) {
		return status.Error(codes.NotFound, "unknown task (already acked, dead, or never existed)")
	}
	if err := s.propose(broker.QueueOp{Type: opType, TaskID: taskID}); err != nil {
		if errors.Is(err, errNotLeader) {
			return s.redirect()
		}
		return err
	}
	s.mu.Lock()
	delete(s.inflight, taskID)
	s.mu.Unlock()
	return nil
}

func (s *ClusterServer) Stats(_ context.Context, _ *queuepb.StatsRequest) (*queuepb.StatsResponse, error) {
	st := s.sm.Stats()
	// AdvisedWorkerCount stays 0: the control plane is single-node v1, so
	// cluster nodes never emit scaling advice (0 = "no advice" to workers).
	return &queuepb.StatsResponse{
		Pending:  int64(st.Pending),
		InFlight: int64(st.InFlight),
		Dlq:      int64(st.DLQ),
		Acked:    st.Acked,
	}, nil
}

func (s *ClusterServer) ListDLQ(_ context.Context, _ *queuepb.ListDLQRequest) (*queuepb.ListDLQResponse, error) {
	tasks := s.sm.ListDLQ()
	out := make([]*queuepb.Task, len(tasks))
	for i, t := range tasks {
		out[i] = toProto(t)
	}
	return &queuepb.ListDLQResponse{Tasks: out}, nil
}

// --- background loops ---

// timeoutLoop redelivers tasks whose worker went silent: an expired
// in-flight task is nacked through the log, so every replica counts the
// retry and the task becomes deliverable again.
func (s *ClusterServer) timeoutLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			var expired []string
			s.mu.Lock()
			for id, deadline := range s.inflight {
				if now.After(deadline) {
					delete(s.inflight, id)
					expired = append(expired, id)
				}
			}
			s.mu.Unlock()
			for _, id := range expired {
				if _, isLeader := s.node.State(); !isLeader {
					break // not ours to redeliver; leadershipWatch resets the overlay
				}
				if s.sm.Has(id) {
					// Best-effort: on failure the task is still safe — a
					// future scan or leader change redelivers it.
					_ = s.propose(broker.QueueOp{Type: broker.OpNack, TaskID: id})
				}
			}
		}
	}
}

// leadershipWatch resets the leader-local dequeue overlay when this node
// loses leadership: in-flight tasks return to pending so a future term on
// this node redelivers them (the new leader does so regardless).
func (s *ClusterServer) leadershipWatch() {
	defer s.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}
		_, isLeader := s.node.State()
		s.mu.Lock()
		was := s.wasLeader
		s.wasLeader = isLeader
		if was && !isLeader {
			s.inflight = make(map[string]time.Time)
			s.mu.Unlock()
			s.sm.RequeueInFlight()
			continue
		}
		s.mu.Unlock()
	}
}
