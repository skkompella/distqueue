package raft

import (
	"context"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// electionLoop fires an election when no valid leader has been heard from
// within the randomized timeout. Checking on a short tick (rather than
// juggling a resettable timer) keeps the locking simple and is the
// standard 6.5840 pattern.
func (n *Node) electionLoop() {
	defer n.stopped.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
		}
		n.mu.Lock()
		if n.state != Leader && time.Since(n.lastReset) >= n.timeout {
			n.startElectionLocked()
		}
		n.mu.Unlock()
	}
}

// startElectionLocked transitions to candidate and solicits votes.
// Caller holds mu.
func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.persistLocked()
	n.resetElectionTimerLocked()
	n.electionsStarted++
	n.logf("election timeout, starting election")

	args := &RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	term := n.currentTerm
	votes := 1 // self

	for _, peer := range n.cfg.Peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeoutMin)
			defer cancel()
			reply, err := n.transport.RequestVote(ctx, peer, args)
			if err != nil {
				return // unreachable peer; the election may still win quorum
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				n.persistLocked()
				return
			}
			// Stale reply: we moved on (new term or already won/lost).
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= n.majority() {
					n.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

// becomeLeaderLocked initializes leader volatile state (§5.3), commits a
// no-op entry for the new term (§8: lets the leader learn the commit
// index without waiting for client traffic — a leader may only count
// replicas for entries of its own term), and announces leadership with
// immediate heartbeats. Caller holds mu.
func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.cfg.ID
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}

	noop := LogEntry{Index: n.lastIndex() + 1, Term: n.currentTerm}
	n.log = append(n.log, noop)
	n.persistLocked()
	n.matchIndex[n.cfg.ID] = n.lastIndex()

	n.logf("won election with last log index %d", n.lastIndex())
	for _, p := range n.cfg.Peers {
		go n.sendAppendEntries(p) // announce immediately, don't wait a tick
	}
	for _, c := range n.replicateCond {
		c.Broadcast()
	}
}

// HandleRequestVote is the RPC handler invoked by the transport when a
// candidate asks for our vote.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
		n.persistLocked()
	}

	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply // stale candidate
	}

	// Election restriction (§5.4.1): only vote for candidates whose log is
	// at least as up-to-date as ours. This is what guarantees a new leader
	// holds every committed entry.
	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persistLocked()
		n.resetElectionTimerLocked() // granting a vote defers our own candidacy
		reply.VoteGranted = true
		n.logf("granted vote to %s", args.CandidateID)
	}
	return reply
}
