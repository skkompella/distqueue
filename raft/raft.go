// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, "In Search of an Understandable
// Consensus Algorithm"): leader election (§5.2), log replication (§5.3),
// safety (§5.4), and log compaction via snapshots (§7).
//
// The package is transport-agnostic: tests drive it with an in-memory
// transport (with partitions and delays); production uses gRPC.
package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one replicated command. Index and Term identify it uniquely
// (Log Matching Property).
type LogEntry struct {
	Index   int
	Term    int
	Command []byte
}

// ApplyMsg is delivered on the apply channel: either one committed command
// or a snapshot the state machine must restore (when this node was too far
// behind and got caught up via InstallSnapshot).
//
// Command is nil for the no-op entry each new leader commits at the start
// of its term; state machines must treat those as skips (but still account
// for CommandIndex when snapshotting).
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// Transport sends RPCs to peers. Implementations must be safe for
// concurrent use.
type Transport interface {
	RequestVote(ctx context.Context, peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peerID string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peerID string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

type Config struct {
	ID    string
	Peers []string // peer IDs, not including this node

	// DataDir holds raft-state.gob and snapshot.bin for this node.
	DataDir string

	ElectionTimeoutMin time.Duration // default 150ms
	ElectionTimeoutMax time.Duration // default 300ms
	HeartbeatInterval  time.Duration // default 50ms

	// Logger receives state-transition and RPC diagnostics; nil disables.
	Logger Logger
}

type Logger interface {
	Printf(format string, args ...any)
}

func (c *Config) applyDefaults() {
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 150 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
}

// Node is one Raft peer. All fields are guarded by mu unless noted.
type Node struct {
	mu        sync.Mutex
	cfg       Config
	transport Transport

	state       State
	currentTerm int    // persistent
	votedFor    string // persistent
	// log[0] is a sentinel holding the snapshot boundary: its Index/Term
	// are LastIncludedIndex/LastIncludedTerm (0/0 before any snapshot).
	// Real entries start at log[1]. This keeps index arithmetic uniform
	// across compaction.
	log []LogEntry // persistent

	commitIndex int
	lastApplied int

	// Leader-only, reinitialized on election.
	nextIndex  map[string]int
	matchIndex map[string]int

	leaderID string // last known leader (for client redirects)

	// Election timing: lastReset is the last time we heard from a valid
	// leader or granted a vote; timeout is the current randomized window.
	lastReset time.Time
	timeout   time.Duration

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // signals the applier that commitIndex advanced

	// snapshot is the latest snapshot bytes (also persisted to disk);
	// pendingSnapshot, when set, must be delivered to applyCh before any
	// further commands (set by InstallSnapshot).
	snapshot        []byte
	pendingSnapshot *ApplyMsg

	replicateCond map[string]*sync.Cond // per-peer: wake the replicator

	stopCh  chan struct{}
	stopped sync.WaitGroup

	// Metrics counters (atomic access not needed; guarded by mu).
	electionsStarted int64
}

// NewNode restores persistent state from cfg.DataDir (if any) and returns
// a node ready to Start. Committed entries are delivered on applyCh; the
// caller must drain it promptly.
func NewNode(cfg Config, transport Transport, applyCh chan ApplyMsg) (*Node, error) {
	cfg.applyDefaults()
	n := &Node{
		cfg:           cfg,
		transport:     transport,
		state:         Follower,
		log:           []LogEntry{{Index: 0, Term: 0}}, // sentinel
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		applyCh:       applyCh,
		replicateCond: make(map[string]*sync.Cond),
		stopCh:        make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		n.replicateCond[p] = sync.NewCond(&n.mu)
	}
	if err := n.readPersist(); err != nil {
		return nil, fmt.Errorf("raft: restore persistent state: %w", err)
	}
	// Nothing below the snapshot boundary needs re-applying.
	n.commitIndex = n.firstIndex()
	n.lastApplied = n.firstIndex()
	n.resetElectionTimerLocked()
	return n, nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.stopped.Add(3 + len(n.cfg.Peers))
	go n.electionLoop()
	go n.heartbeatLoop()
	go n.applier()
	for _, p := range n.cfg.Peers {
		go n.replicator(p)
	}
}

// Stop terminates all goroutines. The applyCh is not closed (the caller
// owns it) but no further messages are sent after Stop returns.
func (n *Node) Stop() {
	n.mu.Lock()
	select {
	case <-n.stopCh:
		n.mu.Unlock()
		return // already stopped
	default:
	}
	close(n.stopCh)
	n.applyCond.Broadcast()
	for _, c := range n.replicateCond {
		c.Broadcast()
	}
	n.mu.Unlock()
	n.stopped.Wait()
}

// Propose appends a command to the log if this node is the leader.
// It returns the entry's index and term; commitment is reported later via
// applyCh. Not leader → isLeader=false and the caller should redirect.
func (n *Node) Propose(command []byte) (index, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, n.currentTerm, false
	}
	index = n.lastIndex() + 1
	term = n.currentTerm
	n.log = append(n.log, LogEntry{Index: index, Term: term, Command: command})
	n.persistLocked()
	n.matchIndex[n.cfg.ID] = index
	for _, c := range n.replicateCond {
		c.Signal()
	}
	return index, term, true
}

// State reports the current term and whether this node believes it is the
// leader.
func (n *Node) State() (term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state == Leader
}

// LeaderHint returns the ID of the most recently observed leader ("" if
// unknown). Used for client redirects.
func (n *Node) LeaderHint() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.cfg.ID
	}
	return n.leaderID
}

// Metrics is a point-in-time snapshot of node state for observability.
type Metrics struct {
	Term             int
	State            State
	LogLength        int // entries currently held (excl. sentinel)
	CommitIndex      int
	LastApplied      int
	SnapshotIndex    int
	ElectionsStarted int64
}

func (n *Node) Metrics() Metrics {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Metrics{
		Term:             n.currentTerm,
		State:            n.state,
		LogLength:        len(n.log) - 1,
		CommitIndex:      n.commitIndex,
		LastApplied:      n.lastApplied,
		SnapshotIndex:    n.firstIndex(),
		ElectionsStarted: n.electionsStarted,
	}
}

// --- log index helpers (callers hold mu) ---

// firstIndex is the snapshot boundary: the index of the last entry that
// has been compacted away (0 if none).
func (n *Node) firstIndex() int { return n.log[0].Index }

func (n *Node) lastIndex() int { return n.log[len(n.log)-1].Index }

func (n *Node) lastTerm() int { return n.log[len(n.log)-1].Term }

// entryAt returns the log entry with the given absolute index.
// Valid for firstIndex() <= i <= lastIndex(); i == firstIndex() returns
// the sentinel (term of the snapshot boundary).
func (n *Node) entryAt(i int) LogEntry { return n.log[i-n.firstIndex()] }

// termAt returns the term of entry i, valid in the same range.
func (n *Node) termAt(i int) int { return n.entryAt(i).Term }

// sliceFrom returns a copy of entries with index >= i.
func (n *Node) sliceFrom(i int) []LogEntry {
	src := n.log[i-n.firstIndex():]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// truncateFrom drops all entries with index >= i (keeps the sentinel).
func (n *Node) truncateFrom(i int) {
	n.log = n.log[:i-n.firstIndex()]
}

// --- state transitions (callers hold mu) ---

// becomeFollowerLocked steps down into term `term`. Caller must persist
// afterward if this changed persistent state (it does when term advances).
func (n *Node) becomeFollowerLocked(term int) {
	prev := n.state
	n.state = Follower
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	if prev != Follower {
		n.logf("stepping down to follower in term %d", n.currentTerm)
	}
}

func (n *Node) resetElectionTimerLocked() {
	n.lastReset = time.Now()
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	n.timeout = n.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

func (n *Node) logf(format string, args ...any) {
	if n.cfg.Logger != nil {
		n.cfg.Logger.Printf("[%s t%d %s] "+format,
			append([]any{n.cfg.ID, n.currentTerm, n.state}, args...)...)
	}
}

// majority returns the quorum size for the cluster (self + peers).
func (n *Node) majority() int { return (len(n.cfg.Peers)+1)/2 + 1 }
