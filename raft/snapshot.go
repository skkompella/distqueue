package raft

import "context"

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Snapshot is called by the state machine when it has serialized its state
// through `index`: the log up to and including index is replaced by the
// snapshot. The sentinel keeps the boundary's index/term so consistency
// checks still work across the cut.
func (n *Node) Snapshot(index int, snapshot []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index <= n.firstIndex() || index > n.lastApplied {
		return // already compacted past here, or state machine is ahead of itself
	}
	boundaryTerm := n.termAt(index)
	tail := n.sliceFrom(index + 1)
	n.log = append([]LogEntry{{Index: index, Term: boundaryTerm}}, tail...)
	n.snapshot = snapshot
	n.persistWithSnapshotLocked(snapshot)
	n.logf("snapshot taken at index %d, log now %d entries", index, len(n.log)-1)
}

// sendInstallSnapshot ships the full snapshot to a follower whose
// nextIndex falls below our log's start (the entries it needs are gone).
// Returns false when the peer was unreachable.
func (n *Node) sendInstallSnapshot(peer string) bool {
	n.mu.Lock()
	if n.state != Leader || n.snapshot == nil {
		n.mu.Unlock()
		return true
	}
	args := &InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: n.firstIndex(),
		LastIncludedTerm:  n.log[0].Term,
		Data:              n.snapshot,
	}
	term := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*n.cfg.ElectionTimeoutMin)
	defer cancel()
	reply, err := n.transport.InstallSnapshot(ctx, peer, args)
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
		return true
	}
	if args.LastIncludedIndex > n.matchIndex[peer] {
		n.matchIndex[peer] = args.LastIncludedIndex
	}
	n.nextIndex[peer] = n.matchIndex[peer] + 1
	return true
}

// HandleInstallSnapshot is the RPC handler on the receiving follower.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := &InstallSnapshotReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}
	if args.Term > n.currentTerm || n.state != Follower {
		n.becomeFollowerLocked(args.Term)
		n.persistLocked()
	}
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Term = n.currentTerm

	if args.LastIncludedIndex <= n.commitIndex {
		return reply // we already have this prefix committed locally
	}

	// If our log extends past the snapshot with a matching boundary entry,
	// keep the tail; otherwise the snapshot replaces the whole log.
	var tail []LogEntry
	if args.LastIncludedIndex <= n.lastIndex() &&
		n.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		tail = n.sliceFrom(args.LastIncludedIndex + 1)
	}
	n.log = append([]LogEntry{{
		Index: args.LastIncludedIndex,
		Term:  args.LastIncludedTerm,
	}}, tail...)
	n.snapshot = args.Data
	n.persistWithSnapshotLocked(args.Data)

	n.commitIndex = args.LastIncludedIndex
	n.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	n.applyCond.Signal()
	n.logf("installed snapshot at index %d from %s", args.LastIncludedIndex, args.LeaderID)
	return reply
}
