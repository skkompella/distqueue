package tests

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/client"
	"github.com/skkompella/distqueue/gen/queuepb"
	"github.com/skkompella/distqueue/raft"
	"github.com/skkompella/distqueue/server"
)

// testClusterNode is one full in-process cluster member: raft node + state
// machine + both gRPC services on real localhost listeners.
type testClusterNode struct {
	id         string
	raftAddr   string
	clientAddr string
	dataDir    string

	node      *raft.Node
	cs        *server.ClusterServer
	raftSrv   *grpc.Server
	clientSrv *grpc.Server
}

type testCluster struct {
	t           *testing.T
	mu          sync.Mutex
	nodes       map[string]*testClusterNode
	raftAddrs   map[string]string
	clientAddrs map[string]string
	dataDirs    map[string]string
	ids         []string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	tc := &testCluster{
		t:           t,
		nodes:       make(map[string]*testClusterNode),
		raftAddrs:   make(map[string]string),
		clientAddrs: make(map[string]string),
		dataDirs:    make(map[string]string),
	}
	for i := 1; i <= size; i++ {
		id := fmt.Sprintf("node%d", i)
		tc.ids = append(tc.ids, id)
		tc.raftAddrs[id] = freeAddr(t)
		tc.clientAddrs[id] = freeAddr(t)
		tc.dataDirs[id] = t.TempDir()
	}
	// Register before starting anything: if a startNode fatals midway,
	// already-running nodes must still be torn down (a leaked node would
	// panic when its temp dir vanishes).
	t.Cleanup(func() {
		for _, id := range tc.ids {
			tc.killNode(id)
		}
	})
	for _, id := range tc.ids {
		tc.startNode(id)
	}
	return tc
}

// listenRetry rides out the window where a just-released port is still
// settling (kill → restart races, allocator reuse between tests).
func listenRetry(t *testing.T, addr string) net.Listener {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		lis, err := net.Listen("tcp", addr)
		if err == nil {
			return lis
		}
		if time.Now().After(deadline) {
			t.Fatalf("listen %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (tc *testCluster) startNode(id string) {
	tc.t.Helper()
	var peerIDs []string
	peerRaft := make(map[string]string)
	for _, p := range tc.ids {
		if p != id {
			peerIDs = append(peerIDs, p)
			peerRaft[p] = tc.raftAddrs[p]
		}
	}

	applyCh := make(chan raft.ApplyMsg, 1024)
	node, err := raft.NewNode(raft.Config{
		ID:                 id,
		Peers:              peerIDs,
		DataDir:            tc.dataDirs[id],
		ElectionTimeoutMin: 100 * time.Millisecond,
		ElectionTimeoutMax: 200 * time.Millisecond,
		HeartbeatInterval:  30 * time.Millisecond,
		Logger:             log.New(os.Stderr, "raft-test ", 0),
	}, raft.NewGRPCTransport(peerRaft), applyCh)
	if err != nil {
		tc.t.Fatalf("start raft %s: %v", id, err)
	}

	sm := broker.NewStateMachine(5)
	cs := server.NewClusterServer(node, sm, server.ClusterConfig{
		TaskTimeout:       500 * time.Millisecond,
		MaxRetries:        5,
		SnapshotThreshold: 100,
		ClientAddrs:       tc.clientAddrs,
		ScanInterval:      50 * time.Millisecond,
	})

	raftLis := listenRetry(tc.t, tc.raftAddrs[id])
	raftSrv := grpc.NewServer()
	raft.NewGRPCServer(node).Register(raftSrv)
	go raftSrv.Serve(raftLis)

	clientLis := listenRetry(tc.t, tc.clientAddrs[id])
	clientSrv := grpc.NewServer()
	cs.Register(clientSrv)
	go clientSrv.Serve(clientLis)

	node.Start()
	go cs.Run(applyCh)

	tc.mu.Lock()
	tc.nodes[id] = &testClusterNode{
		id: id, raftAddr: tc.raftAddrs[id], clientAddr: tc.clientAddrs[id],
		dataDir: tc.dataDirs[id], node: node, cs: cs, raftSrv: raftSrv, clientSrv: clientSrv,
	}
	tc.mu.Unlock()
}

// killNode is the kill -9 equivalent: hard-stop both gRPC servers and the
// raft node. Persistent state stays on disk for restart.
func (tc *testCluster) killNode(id string) {
	tc.mu.Lock()
	n := tc.nodes[id]
	delete(tc.nodes, id)
	tc.mu.Unlock()
	if n == nil {
		return
	}
	n.raftSrv.Stop()
	n.clientSrv.Stop()
	n.node.Stop()
	n.cs.Stop()
}

func (tc *testCluster) leaderID() string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for id, n := range tc.nodes {
		if _, isLeader := n.node.State(); isLeader {
			return id
		}
	}
	return ""
}

func (tc *testCluster) waitForLeader() string {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if id := tc.leaderID(); id != "" {
			return id
		}
		time.Sleep(20 * time.Millisecond)
	}
	tc.t.Fatal("no leader within 10s")
	return ""
}

func (tc *testCluster) clientAddrList() []string {
	var out []string
	for _, id := range tc.ids {
		out = append(out, tc.clientAddrs[id])
	}
	return out
}

// TestClusterEndToEnd: a 3-node cluster serves the full produce → dequeue
// → ack cycle through the failover client.
func TestClusterEndToEnd(t *testing.T) {
	tc := startTestCluster(t, 3)
	tc.waitForLeader()

	qc := client.New(tc.clientAddrList())
	defer qc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const total = 50
	ids := make(map[string]bool)
	for i := 0; i < total; i++ {
		resp, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
			Payload: []byte(fmt.Sprintf("e2e-%d", i)), Priority: int32(i % 3),
		}})
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		ids[resp.GetTaskId()] = true
	}

	done := 0
	for done < total {
		resp, err := qc.Dequeue(ctx, &queuepb.DequeueRequest{})
		if status.Code(err) == codes.NotFound {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		id := resp.GetTask().GetId()
		if !ids[id] {
			t.Fatalf("dequeued unknown or duplicate task %s", id)
		}
		if _, err := qc.Ack(ctx, &queuepb.AckRequest{TaskId: id}); err != nil {
			t.Fatalf("ack %s: %v", id, err)
		}
		delete(ids, id)
		done++
	}

	stats, err := qc.Stats(ctx, &queuepb.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.GetPending() != 0 || stats.GetDlq() != 0 {
		t.Fatalf("queue should be drained: %+v", stats)
	}
}

// TestClusterLeaderKillNoTaskLoss: kill the leader mid-stream; every
// acknowledged enqueue must survive into the new term and complete.
func TestClusterLeaderKillNoTaskLoss(t *testing.T) {
	tc := startTestCluster(t, 3)
	tc.waitForLeader()

	qc := client.New(tc.clientAddrList())
	defer qc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const total = 60
	accepted := make(map[string]bool)
	for i := 0; i < total; i++ {
		if i == total/2 {
			// Hard-kill the leader mid-stream. The failover client must
			// ride out the election.
			leader := tc.leaderID()
			if leader != "" {
				tc.killNode(leader)
			}
		}
		resp, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
			Payload: []byte(fmt.Sprintf("kill-%d", i)), Priority: 1,
		}})
		if err != nil {
			t.Fatalf("enqueue %d failed even with failover: %v", i, err)
		}
		accepted[resp.GetTaskId()] = true
	}

	// Drain on the surviving 2-node quorum: every acknowledged enqueue
	// must come out exactly... at least once.
	completed := make(map[string]bool)
	deadline := time.Now().Add(60 * time.Second)
	for len(completed) < len(accepted) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d tasks completed after leader kill", len(completed), len(accepted))
		}
		resp, err := qc.Dequeue(ctx, &queuepb.DequeueRequest{})
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		id := resp.GetTask().GetId()
		if !accepted[id] {
			t.Fatalf("dequeued task %s that was never accepted", id)
		}
		if _, err := qc.Ack(ctx, &queuepb.AckRequest{TaskId: id}); err == nil {
			completed[id] = true
		}
	}
}

// TestClusterAckedNeverRedelivered: acked tasks must not come back after a
// leader failover (acks are replicated; delivery state is not).
func TestClusterAckedNeverRedelivered(t *testing.T) {
	tc := startTestCluster(t, 3)
	tc.waitForLeader()

	qc := client.New(tc.clientAddrList())
	defer qc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const total = 20
	for i := 0; i < total; i++ {
		if _, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
			Payload: []byte(fmt.Sprintf("ack-%d", i)), Priority: 1,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	// Complete half.
	acked := make(map[string]bool)
	for len(acked) < total/2 {
		resp, err := qc.Dequeue(ctx, &queuepb.DequeueRequest{})
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		id := resp.GetTask().GetId()
		if _, err := qc.Ack(ctx, &queuepb.AckRequest{TaskId: id}); err != nil {
			t.Fatalf("ack: %v", err)
		}
		acked[id] = true
	}

	// Failover.
	tc.killNode(tc.leaderID())

	// Drain the rest; acked tasks must never reappear.
	seen := make(map[string]bool)
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < total-len(acked) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d remaining tasks delivered after failover", len(seen), total-len(acked))
		}
		resp, err := qc.Dequeue(ctx, &queuepb.DequeueRequest{})
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		id := resp.GetTask().GetId()
		if acked[id] {
			t.Fatalf("acked task %s was redelivered after failover", id)
		}
		if _, err := qc.Ack(ctx, &queuepb.AckRequest{TaskId: id}); err == nil {
			seen[id] = true
		}
	}
}

// TestClusterNodeRestartRejoins: a killed node restarts from its raft
// state on disk and catches back up.
func TestClusterNodeRestartRejoins(t *testing.T) {
	tc := startTestCluster(t, 3)
	leader := tc.waitForLeader()

	qc := client.New(tc.clientAddrList())
	defer qc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Kill a follower, write traffic, restart it.
	var follower string
	for _, id := range tc.ids {
		if id != leader {
			follower = id
			break
		}
	}
	tc.killNode(follower)

	for i := 0; i < 30; i++ {
		if _, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
			Payload: []byte(fmt.Sprintf("rejoin-%d", i)), Priority: 1,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	tc.startNode(follower)

	// The restarted node must converge to the same applied state: its
	// commit index reaches the leader's within the catch-up window.
	deadline := time.Now().Add(15 * time.Second)
	for {
		leaderNow := tc.leaderID() // before taking tc.mu: leaderID locks it too
		tc.mu.Lock()
		f := tc.nodes[follower]
		var l *testClusterNode
		if leaderNow != "" {
			l = tc.nodes[leaderNow]
		}
		tc.mu.Unlock()
		if f != nil && l != nil {
			fm, lm := f.node.Metrics(), l.node.Metrics()
			if fm.CommitIndex >= lm.CommitIndex && lm.CommitIndex > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted follower never caught up to the leader's commit index")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// And the cluster still works end to end.
	if _, err := qc.Enqueue(ctx, &queuepb.EnqueueRequest{Task: &queuepb.Task{
		Payload: []byte("final"), Priority: 1,
	}}); err != nil {
		t.Fatal(err)
	}
}
