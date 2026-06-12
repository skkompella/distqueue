package raft

import (
	"context"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int        // index of entry immediately preceding Entries
	PrevLogTerm  int        // term of that entry
	Entries      []LogEntry // empty = heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Fast backtracking (§5.3 optimization): on mismatch the follower
	// reports the conflicting term and the first index it holds for that
	// term, so the leader skips a whole term per round trip instead of
	// decrementing nextIndex one entry at a time.
	ConflictTerm  int // 0 if the follower's log is just too short
	ConflictIndex int
}

// replicator is the per-peer catch-up loop: it sends whenever this node
// is leader and the peer is missing entries, sleeping otherwise. Entries
// proposed while an RPC is in flight go out together on the next send —
// batching falls out for free. Heartbeats are handled separately by
// heartbeatLoop.
func (n *Node) replicator(peer string) {
	defer n.stopped.Done()
	for {
		n.mu.Lock()
		for !(n.state == Leader && n.lastIndex() >= n.nextIndex[peer]) {
			if n.isStopped() {
				n.mu.Unlock()
				return
			}
			n.replicateCond[peer].Wait()
		}
		n.mu.Unlock()
		if n.isStopped() {
			return
		}
		if !n.sendAppendEntries(peer) {
			// Peer unreachable or no progress: back off for one heartbeat
			// instead of hammering it.
			select {
			case <-time.After(n.cfg.HeartbeatInterval):
			case <-n.stopCh:
				return
			}
		}
	}
}

// heartbeatLoop sends empty (or catch-up) AppendEntries to every peer each
// interval so followers hear from the leader well inside the election
// timeout.
func (n *Node) heartbeatLoop() {
	defer n.stopped.Done()
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		isLeader := n.state == Leader
		n.mu.Unlock()
		if isLeader {
			for _, p := range n.cfg.Peers {
				go n.sendAppendEntries(p)
			}
		}
	}
}

func (n *Node) isStopped() bool {
	select {
	case <-n.stopCh:
		return true
	default:
		return false
	}
}

// sendAppendEntries sends one replication RPC (possibly empty = heartbeat)
// to peer and processes the reply. Returns false when no progress was
// made (transport error or rejection) so callers can back off.
func (n *Node) sendAppendEntries(peer string) bool {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return true
	}
	next := n.nextIndex[peer]
	if next <= n.firstIndex() {
		// The entries this peer needs are gone (compacted): ship the
		// whole snapshot instead.
		n.mu.Unlock()
		return n.sendInstallSnapshot(peer)
	}
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: next - 1,
		PrevLogTerm:  n.termAt(next - 1),
		Entries:      n.sliceFrom(next),
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeoutMin)
	defer cancel()
	reply, err := n.transport.AppendEntries(ctx, peer, args)
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		n.persistLocked()
		n.resetElectionTimerLocked()
		return true
	}
	if n.state != Leader || n.currentTerm != term {
		return true // stale reply from a previous term
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
		return true
	}

	// Log inconsistency: back up using the follower's conflict hint.
	if reply.ConflictTerm > 0 {
		// If we hold entries of ConflictTerm, resend from just after our
		// last entry of that term; otherwise skip the whole term.
		lastOfTerm := 0
		for i := n.lastIndex(); i > n.firstIndex(); i-- {
			if n.termAt(i) == reply.ConflictTerm {
				lastOfTerm = i
				break
			}
			if n.termAt(i) < reply.ConflictTerm {
				break
			}
		}
		if lastOfTerm > 0 {
			n.nextIndex[peer] = lastOfTerm + 1
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
	} else {
		n.nextIndex[peer] = reply.ConflictIndex // follower log too short
	}
	if n.nextIndex[peer] <= n.firstIndex() {
		// Will trigger InstallSnapshot on the retry.
		n.nextIndex[peer] = n.firstIndex()
	}
	if n.nextIndex[peer] < 1 {
		n.nextIndex[peer] = 1
	}
	n.replicateCond[peer].Signal() // retry immediately with the better hint
	return true
}

// advanceCommitLocked moves commitIndex to the highest index replicated on
// a majority — but only for entries of the current term (§5.4.2: a leader
// may never commit a previous term's entry by counting replicas).
// Caller holds mu.
func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndex(); idx > n.commitIndex && idx > n.firstIndex(); idx-- {
		if n.termAt(idx) != n.currentTerm {
			break // older-term entries commit implicitly once a newer one does
		}
		count := 0
		for _, m := range n.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Signal()
			break
		}
	}
}

// HandleAppendEntries is the RPC handler on the receiving side.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply // stale leader
	}
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(args.Term)
		n.persistLocked()
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	// If PrevLogIndex is inside our snapshot, everything up to the
	// boundary is committed state we already hold; shift the window so it
	// starts at the boundary.
	if args.PrevLogIndex < n.firstIndex() {
		cut := n.firstIndex() - args.PrevLogIndex
		if cut > len(args.Entries) {
			// Entire batch is below the snapshot: it's all applied state.
			reply.Success = true
			return reply
		}
		args.Entries = args.Entries[cut:]
		args.PrevLogIndex = n.firstIndex()
		args.PrevLogTerm = n.log[0].Term
	}

	// Consistency check (§5.3): we must hold PrevLogIndex with PrevLogTerm.
	if args.PrevLogIndex > n.lastIndex() {
		reply.ConflictTerm = 0
		reply.ConflictIndex = n.lastIndex() + 1
		return reply
	}
	if t := n.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > n.firstIndex()+1 && n.termAt(i-1) == t {
			i--
		}
		reply.ConflictIndex = i
		return reply
	}

	// Append: skip entries we already hold; on the first term conflict,
	// truncate our tail and take the leader's entries from there. Never
	// truncate on a mere duplicate — an old RPC must not undo newer state.
	changed := false
	for i, e := range args.Entries {
		if e.Index <= n.lastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue // already have it
			}
			n.truncateFrom(e.Index)
		}
		n.log = append(n.log, args.Entries[i:]...)
		changed = true
		break
	}
	if changed {
		n.persistLocked()
	}

	// Advance commitIndex only over entries this RPC confirmed we share
	// with the leader.
	lastNew := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, lastNew)
		if n.commitIndex > n.lastApplied {
			n.applyCond.Signal()
		}
	}
	reply.Success = true
	return reply
}

// applier delivers committed entries (and installed snapshots) to the
// state machine, in order, without holding the node lock during channel
// sends (the state machine may call back into the node, e.g. Snapshot).
func (n *Node) applier() {
	defer n.stopped.Done()
	for {
		n.mu.Lock()
		for n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			if n.isStopped() {
				n.mu.Unlock()
				return
			}
			n.applyCond.Wait()
		}
		if n.isStopped() {
			n.mu.Unlock()
			return
		}

		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			if snap.SnapshotIndex > n.lastApplied {
				n.lastApplied = snap.SnapshotIndex
			}
			n.mu.Unlock()
			select {
			case n.applyCh <- *snap:
			case <-n.stopCh:
				return
			}
			continue
		}

		from, to := n.lastApplied+1, n.commitIndex
		if from <= n.firstIndex() {
			from = n.firstIndex() + 1 // those entries live in the snapshot now
		}
		batch := make([]LogEntry, 0, to-from+1)
		for i := from; i <= to; i++ {
			batch = append(batch, n.entryAt(i))
		}
		n.mu.Unlock()

		for _, e := range batch {
			select {
			case n.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			}:
			case <-n.stopCh:
				return
			}
		}

		n.mu.Lock()
		if to > n.lastApplied {
			n.lastApplied = to
		}
		n.mu.Unlock()
	}
}
