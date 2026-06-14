package raft

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/skkompella/distqueue/gen/raftpb"
)

// Reconnect aggressively: the peer set is small and static, and gRPC's
// default backoff (cap 120s) is poison for a consensus cluster — a node
// returning after a long outage would stay unreachable from the leader
// until the old channel's backoff expired, so it never hears a heartbeat
// and keeps disrupting elections with ever-higher terms.
var fastReconnect = grpc.WithConnectParams(grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   time.Second,
	},
	MinConnectTimeout: time.Second,
})

// GRPCTransport implements Transport over gRPC. Connections to peers are
// created lazily and cached; gRPC reconnects under the hood, so a peer
// restart needs no handling here.
type GRPCTransport struct {
	mu    sync.Mutex
	addrs map[string]string // peer ID → host:port
	conns map[string]raftpb.RaftClient
}

func NewGRPCTransport(peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		addrs: peerAddrs,
		conns: make(map[string]raftpb.RaftClient),
	}
}

func (t *GRPCTransport) client(peer string) (raftpb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peer]; ok {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("raft: unknown peer %q", peer)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		fastReconnect)
	if err != nil {
		return nil, err
	}
	c := raftpb.NewRaftClient(conn)
	t.conns[peer] = c
	return c, nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         int64(args.Term),
		CandidateId:  args.CandidateID,
		LastLogIndex: int64(args.LastLogIndex),
		LastLogTerm:  int64(args.LastLogTerm),
	})
	if err != nil {
		return nil, err
	}
	return &RequestVoteReply{
		Term:        int(resp.GetTerm()),
		VoteGranted: resp.GetVoteGranted(),
	}, nil
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	entries := make([]*raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = &raftpb.LogEntry{
			Index:   int64(e.Index),
			Term:    int64(e.Term),
			Command: e.Command,
		}
	}
	resp, err := c.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         int64(args.Term),
		LeaderId:     args.LeaderID,
		PrevLogIndex: int64(args.PrevLogIndex),
		PrevLogTerm:  int64(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int64(args.LeaderCommit),
	})
	if err != nil {
		return nil, err
	}
	return &AppendEntriesReply{
		Term:          int(resp.GetTerm()),
		Success:       resp.GetSuccess(),
		ConflictTerm:  int(resp.GetConflictTerm()),
		ConflictIndex: int(resp.GetConflictIndex()),
	}, nil
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	c, err := t.client(peer)
	if err != nil {
		return nil, err
	}
	resp, err := c.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              int64(args.Term),
		LeaderId:          args.LeaderID,
		LastIncludedIndex: int64(args.LastIncludedIndex),
		LastIncludedTerm:  int64(args.LastIncludedTerm),
		Data:              args.Data,
	})
	if err != nil {
		return nil, err
	}
	return &InstallSnapshotReply{Term: int(resp.GetTerm())}, nil
}

// GRPCServer adapts a Node to the raftpb.RaftServer interface (the
// receiving side of the RPCs).
type GRPCServer struct {
	raftpb.UnimplementedRaftServer
	node *Node
}

func NewGRPCServer(n *Node) *GRPCServer { return &GRPCServer{node: n} }

// Register attaches the Raft service to a gRPC server.
func (s *GRPCServer) Register(g *grpc.Server) { raftpb.RegisterRaftServer(g, s) }

func (s *GRPCServer) RequestVote(_ context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	reply := s.node.HandleRequestVote(&RequestVoteArgs{
		Term:         int(req.GetTerm()),
		CandidateID:  req.GetCandidateId(),
		LastLogIndex: int(req.GetLastLogIndex()),
		LastLogTerm:  int(req.GetLastLogTerm()),
	})
	return &raftpb.RequestVoteResponse{
		Term:        int64(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *GRPCServer) AppendEntries(_ context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	entries := make([]LogEntry, len(req.GetEntries()))
	for i, e := range req.GetEntries() {
		entries[i] = LogEntry{
			Index:   int(e.GetIndex()),
			Term:    int(e.GetTerm()),
			Command: e.GetCommand(),
		}
	}
	reply := s.node.HandleAppendEntries(&AppendEntriesArgs{
		Term:         int(req.GetTerm()),
		LeaderID:     req.GetLeaderId(),
		PrevLogIndex: int(req.GetPrevLogIndex()),
		PrevLogTerm:  int(req.GetPrevLogTerm()),
		Entries:      entries,
		LeaderCommit: int(req.GetLeaderCommit()),
	})
	return &raftpb.AppendEntriesResponse{
		Term:          int64(reply.Term),
		Success:       reply.Success,
		ConflictTerm:  int64(reply.ConflictTerm),
		ConflictIndex: int64(reply.ConflictIndex),
	}, nil
}

func (s *GRPCServer) InstallSnapshot(_ context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	reply := s.node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term:              int(req.GetTerm()),
		LeaderID:          req.GetLeaderId(),
		LastIncludedIndex: int(req.GetLastIncludedIndex()),
		LastIncludedTerm:  int(req.GetLastIncludedTerm()),
		Data:              req.GetData(),
	})
	return &raftpb.InstallSnapshotResponse{Term: int64(reply.Term)}, nil
}
