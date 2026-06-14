package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// --- in-memory network with partitions and unreliable delivery ---

var errUnreachable = errors.New("memnet: peer unreachable")

type memNetwork struct {
	mu         sync.Mutex
	nodes      map[string]*Node
	cut        map[string]bool // node fully disconnected
	dropRate   float64         // fraction of RPCs silently dropped
	maxDelayMs int             // random per-RPC delay
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		nodes:      make(map[string]*Node),
		cut:        make(map[string]bool),
		maxDelayMs: 2,
	}
}

func (m *memNetwork) reachable(from, to string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.cut[from] && !m.cut[to] && m.nodes[to] != nil
}

func (m *memNetwork) drop() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dropRate > 0 && rand.Float64() < m.dropRate
}

func (m *memNetwork) delay() {
	m.mu.Lock()
	d := m.maxDelayMs
	m.mu.Unlock()
	if d > 0 {
		time.Sleep(time.Duration(rand.Intn(d*1000)) * time.Microsecond)
	}
}

func (m *memNetwork) target(id string) *Node {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodes[id]
}

// memTransport implements Transport for one node on a memNetwork.
type memTransport struct {
	net  *memNetwork
	from string
}

func call[A any, R any](t *memTransport, peer string, args *A, handle func(*Node, *A) *R) (*R, error) {
	if !t.net.reachable(t.from, peer) || t.net.drop() {
		return nil, errUnreachable
	}
	t.net.delay()
	// Re-check after the delay: the partition may have happened mid-flight.
	if !t.net.reachable(t.from, peer) {
		return nil, errUnreachable
	}
	target := t.net.target(peer)
	if target == nil {
		return nil, errUnreachable
	}
	reply := handle(target, args)
	// The reply can be lost too.
	if !t.net.reachable(t.from, peer) || t.net.drop() {
		return nil, errUnreachable
	}
	return reply, nil
}

func (t *memTransport) RequestVote(_ context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return call(t, peer, args, (*Node).HandleRequestVote)
}

func (t *memTransport) AppendEntries(_ context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return call(t, peer, args, (*Node).HandleAppendEntries)
}

func (t *memTransport) InstallSnapshot(_ context.Context, peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return call(t, peer, args, (*Node).HandleInstallSnapshot)
}

// --- cluster harness ---

type cluster struct {
	t       *testing.T
	net     *memNetwork
	ids     []string
	dirs    map[string]string // "" = ephemeral
	nodes   map[string]*Node
	applyCh map[string]chan ApplyMsg

	mu       sync.Mutex
	applied  map[string]map[int]string // node → log index → command
	lastIdx  map[string]int            // node → highest applied index (order check)
	snapIdx  map[string]int            // node → last restored snapshot index
	drainers sync.WaitGroup
}

func newCluster(t *testing.T, size int, persistent bool) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		net:     newMemNetwork(),
		dirs:    make(map[string]string),
		nodes:   make(map[string]*Node),
		applyCh: make(map[string]chan ApplyMsg),
		applied: make(map[string]map[int]string),
		lastIdx: make(map[string]int),
		snapIdx: make(map[string]int),
	}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		c.ids = append(c.ids, id)
		if persistent {
			c.dirs[id] = t.TempDir()
		}
	}
	for _, id := range c.ids {
		c.startNode(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func (c *cluster) config(id string) Config {
	var peers []string
	for _, p := range c.ids {
		if p != id {
			peers = append(peers, p)
		}
	}
	return Config{
		ID:                 id,
		Peers:              peers,
		DataDir:            c.dirs[id],
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		HeartbeatInterval:  20 * time.Millisecond,
	}
}

func (c *cluster) startNode(id string) {
	ch := make(chan ApplyMsg, 256)
	n, err := NewNode(c.config(id), &memTransport{net: c.net, from: id}, ch)
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.net.mu.Lock()
	c.net.nodes[id] = n
	c.net.mu.Unlock()
	c.nodes[id] = n
	c.applyCh[id] = ch
	c.drainers.Add(1)
	go c.drain(id, ch)
	n.Start()
}

// drain records every applied command by log index, and asserts that
// delivery is strictly ordered (a node never applies backwards or skips,
// except for a jump caused by a snapshot restore).
func (c *cluster) drain(id string, ch chan ApplyMsg) {
	defer c.drainers.Done()
	for msg := range ch {
		c.mu.Lock()
		if msg.SnapshotValid {
			c.snapIdx[id] = msg.SnapshotIndex
			c.lastIdx[id] = msg.SnapshotIndex
		} else if msg.CommandValid {
			if last := c.lastIdx[id]; last > 0 && msg.CommandIndex != last+1 {
				c.t.Errorf("%s applied index %d after %d (out of order)", id, msg.CommandIndex, last)
			}
			c.lastIdx[id] = msg.CommandIndex
			if len(msg.Command) > 0 { // skip leader no-ops
				if c.applied[id] == nil {
					c.applied[id] = make(map[int]string)
				}
				c.applied[id][msg.CommandIndex] = string(msg.Command)
			}
		}
		c.mu.Unlock()
	}
}

func (c *cluster) stopNode(id string) {
	if n := c.nodes[id]; n != nil {
		c.net.mu.Lock()
		delete(c.net.nodes, id)
		c.net.mu.Unlock()
		n.Stop()
		close(c.applyCh[id])
		delete(c.nodes, id)
	}
}

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		c.stopNode(id)
	}
	c.drainers.Wait()
}

// crash kills a node; restart brings it back with the same data dir (and a
// fresh applied record, since the state machine restarts too).
func (c *cluster) crash(id string) { c.stopNode(id) }

func (c *cluster) restart(id string) {
	c.mu.Lock()
	c.applied[id] = nil
	c.lastIdx[id] = 0
	c.snapIdx[id] = 0
	c.mu.Unlock()
	c.startNode(id)
}

func (c *cluster) disconnect(id string) {
	c.net.mu.Lock()
	c.net.cut[id] = true
	c.net.mu.Unlock()
}

func (c *cluster) reconnect(id string) {
	c.net.mu.Lock()
	delete(c.net.cut, id)
	c.net.mu.Unlock()
}

func (c *cluster) connectedIDs() []string {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	var out []string
	for id, n := range c.nodes {
		if n != nil && !c.net.cut[id] {
			out = append(out, id)
		}
	}
	return out
}

// waitForLeader blocks until exactly one connected node claims leadership
// and a quorum of connected nodes agree on its term.
func (c *cluster) waitForLeader() string {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []string
		for _, id := range c.connectedIDs() {
			if _, isLeader := c.nodes[id].State(); isLeader {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no single leader elected within 5s")
	return ""
}

// propose submits a command via whichever node is leader, retrying across
// elections. Returns the log index it was accepted at.
func (c *cluster) propose(cmd string) int {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.connectedIDs() {
			if idx, _, ok := c.nodes[id].Propose([]byte(cmd)); ok {
				return idx
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("propose %q: no leader accepted within 5s", cmd)
	return 0
}

// appliedCount returns how many distinct commands a node has applied.
func (c *cluster) appliedCount(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.applied[id])
}

// hasApplied reports whether the node applied cmd at any index.
func (c *cluster) hasApplied(id, cmd string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, got := range c.applied[id] {
		if got == cmd {
			return true
		}
	}
	return false
}

// waitApplied blocks until at least `count` nodes have applied `cmd`.
func (c *cluster) waitApplied(cmd string, count int) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n := 0
		c.mu.Lock()
		for _, byIdx := range c.applied {
			for _, got := range byIdx {
				if got == cmd {
					n++
					break
				}
			}
		}
		c.mu.Unlock()
		if n >= count {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("command %q not applied on %d nodes within 5s", cmd, count)
}

// checkConsistency asserts that no two nodes applied different commands at
// the same log index (State Machine Safety, §5.4.3).
func (c *cluster) checkConsistency() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for a, mapA := range c.applied {
		for b, mapB := range c.applied {
			if a >= b {
				continue
			}
			for idx, cmdA := range mapA {
				if cmdB, ok := mapB[idx]; ok && cmdA != cmdB {
					c.t.Fatalf("state machine divergence at index %d: %s applied %q, %s applied %q",
						idx, a, cmdA, b, cmdB)
				}
			}
		}
	}
}
