package raft

import (
	"fmt"
	"testing"
	"time"
)

// --- leader election ---

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, false)
	leader := c.waitForLeader()

	// Leadership should be stable while the network is healthy: same
	// leader, same term, after several election timeouts.
	term1, _ := c.nodes[leader].State()
	time.Sleep(500 * time.Millisecond)
	leader2 := c.waitForLeader()
	term2, _ := c.nodes[leader2].State()
	if leader != leader2 || term1 != term2 {
		t.Fatalf("leadership unstable on a healthy network: %s/t%d -> %s/t%d",
			leader, term1, leader2, term2)
	}
}

func TestLeaderDisconnectTriggersReelection(t *testing.T) {
	c := newCluster(t, 3, false)
	first := c.waitForLeader()

	c.disconnect(first)
	second := c.waitForLeader()
	if second == first {
		t.Fatalf("disconnected node %s still considered leader", first)
	}

	// The old leader rejoins and must step down (higher term wins).
	c.reconnect(first)
	time.Sleep(300 * time.Millisecond)
	final := c.waitForLeader()
	if _, isLeader := c.nodes[first].State(); isLeader && final != first {
		t.Fatalf("two leaders after heal: %s and %s", first, final)
	}
}

func TestNoQuorumNoLeader(t *testing.T) {
	c := newCluster(t, 3, false)
	leader := c.waitForLeader()

	// Cut two nodes: the survivor can never win an election.
	var cut int
	for _, id := range c.ids {
		if id != leader && cut < 2 {
			c.disconnect(id)
			cut++
		}
	}
	// The leader keeps leading only until it notices; either way it must
	// not commit anything new. Give it time to attempt elections.
	time.Sleep(600 * time.Millisecond)

	idx, _, isLeader := c.nodes[leader].Propose([]byte("doomed"))
	if isLeader {
		// It may still think it leads, but the entry must never commit.
		time.Sleep(500 * time.Millisecond)
		m := c.nodes[leader].Metrics()
		if m.CommitIndex >= idx {
			t.Fatalf("entry committed without quorum: commit=%d propose=%d", m.CommitIndex, idx)
		}
	}
}

func TestSplitVoteEventuallyResolves(t *testing.T) {
	// 5 nodes with lossy links: elections will collide and split votes;
	// randomized timeouts must still converge on a leader.
	c := newCluster(t, 5, false)
	c.net.mu.Lock()
	c.net.dropRate = 0.15
	c.net.mu.Unlock()

	c.waitForLeader()

	c.net.mu.Lock()
	c.net.dropRate = 0
	c.net.mu.Unlock()
	c.waitForLeader()
}

// --- log replication ---

func TestBasicReplication(t *testing.T) {
	c := newCluster(t, 3, false)
	c.waitForLeader()

	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
	}
	for i := 0; i < 10; i++ {
		c.waitApplied(fmt.Sprintf("cmd-%d", i), 3)
	}
	c.checkConsistency()
}

func TestFollowerCrashAndRejoin(t *testing.T) {
	c := newCluster(t, 3, false)
	leader := c.waitForLeader()

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.disconnect(follower)

	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("away-%d", i))
	}
	c.waitApplied("away-19", 2) // majority commits without the follower

	c.reconnect(follower)
	c.waitApplied("away-19", 3) // and it catches up after rejoining
	c.checkConsistency()
}

func TestLeaderCrashNoCommittedLoss(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.waitForLeader()

	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("pre-crash-%d", i))
	}
	c.waitApplied("pre-crash-9", 3)

	c.crash(leader)
	c.waitForLeader()

	// Everything committed before the crash must still be applied by the
	// new majority, and new proposals must work.
	c.propose("post-crash")
	c.waitApplied("post-crash", 2)
	for i := 0; i < 10; i++ {
		c.waitApplied(fmt.Sprintf("pre-crash-%d", i), 2)
	}
	c.checkConsistency()
}

func TestPartitionedMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 3, false)
	oldLeader := c.waitForLeader()

	// Isolate the leader: it keeps accepting proposals but can't commit.
	c.disconnect(oldLeader)
	staleIdx, _, stillLeader := c.nodes[oldLeader].Propose([]byte("stale"))

	// Majority side elects a new leader and commits.
	newLeader := c.waitForLeader()
	if newLeader == oldLeader {
		t.Fatal("partitioned leader should not win the new election")
	}
	c.propose("fresh")
	c.waitApplied("fresh", 2)

	if stillLeader {
		m := c.nodes[oldLeader].Metrics()
		if m.CommitIndex >= staleIdx {
			t.Fatalf("minority leader committed %d", staleIdx)
		}
	}

	// Heal: the stale entry must be overwritten, never applied anywhere.
	c.reconnect(oldLeader)
	c.waitApplied("fresh", 3)
	time.Sleep(200 * time.Millisecond)
	for _, id := range c.ids {
		if c.hasApplied(id, "stale") {
			t.Fatalf("%s applied an uncommitted entry from a deposed leader", id)
		}
	}
	c.checkConsistency()
}

func TestFullClusterRestart(t *testing.T) {
	c := newCluster(t, 3, true)
	c.waitForLeader()
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("durable-%d", i))
	}
	c.waitApplied("durable-9", 3)

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	c.waitForLeader()

	// Re-applied from the persisted logs after a full power loss.
	for i := 0; i < 10; i++ {
		c.waitApplied(fmt.Sprintf("durable-%d", i), 3)
	}
	c.checkConsistency()
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3, false)
	c.waitForLeader()

	const n = 50
	done := make(chan string, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			cmd := fmt.Sprintf("conc-%d", i)
			c.propose(cmd)
			done <- cmd
		}(i)
	}
	for i := 0; i < n; i++ {
		c.waitApplied(<-done, 3)
	}
	c.checkConsistency()

	// No command may be applied twice on one node (each was proposed once
	// and accepted once).
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, byIdx := range c.applied {
		seen := make(map[string]int)
		for idx, cmd := range byIdx {
			if prev, dup := seen[cmd]; dup {
				t.Fatalf("%s applied %q at both index %d and %d", id, cmd, prev, idx)
			}
			seen[cmd] = idx
		}
	}
}

// --- snapshots ---

func TestSnapshotTrimsLog(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.waitForLeader()

	for i := 0; i < 30; i++ {
		c.propose(fmt.Sprintf("s-%d", i))
	}
	c.waitApplied("s-29", 3)

	before := c.nodes[leader].Metrics()
	cutoff := c.nodes[leader].Metrics().LastApplied
	c.nodes[leader].Snapshot(cutoff, []byte("snapshot-state"))
	after := c.nodes[leader].Metrics()
	if after.LogLength >= before.LogLength {
		t.Fatalf("snapshot did not trim the log: %d -> %d entries", before.LogLength, after.LogLength)
	}
	if after.SnapshotIndex != cutoff {
		t.Fatalf("snapshot boundary = %d, want %d", after.SnapshotIndex, cutoff)
	}

	// The cluster still replicates normally after compaction.
	c.propose("post-snapshot")
	c.waitApplied("post-snapshot", 3)
	c.checkConsistency()
}

func TestInstallSnapshotCatchesUpLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.waitForLeader()

	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	c.disconnect(follower)

	// Build up history and compact it away while the follower is gone.
	for i := 0; i < 40; i++ {
		c.propose(fmt.Sprintf("gone-%d", i))
	}
	c.waitApplied("gone-39", 2)
	lead := c.nodes[c.waitForLeader()]
	lead.Snapshot(lead.Metrics().LastApplied, []byte("compacted-state"))

	c.reconnect(follower)

	// The follower can't get the entries (they're compacted) — it must
	// receive the snapshot, then any tail commands.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		snap := c.snapIdx[follower]
		c.mu.Unlock()
		if snap > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lagging follower never received a snapshot")
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.propose("after-install")
	c.waitApplied("after-install", 3)
	c.checkConsistency()
}

func TestSnapshotSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.waitForLeader()

	for i := 0; i < 20; i++ {
		c.propose(fmt.Sprintf("r-%d", i))
	}
	c.waitApplied("r-19", 3)
	cutoff := c.nodes[leader].Metrics().LastApplied
	c.nodes[leader].Snapshot(cutoff, []byte("leader-snap"))

	c.crash(leader)
	c.restart(leader)
	c.waitForLeader()

	// The restarted node must restore from its snapshot file (delivered
	// via applyCh before any commands).
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		snap := c.snapIdx[leader]
		c.mu.Unlock()
		if snap == cutoff {
			break
		}
		if time.Now().After(deadline) {
			c.mu.Lock()
			got := c.snapIdx[leader]
			c.mu.Unlock()
			t.Fatalf("restarted node restored snapshot at %d, want %d", got, cutoff)
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.propose("post-restart")
	c.waitApplied("post-restart", 3)
	c.checkConsistency()
}
