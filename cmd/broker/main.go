// The broker binary. Two modes:
//
//	single-node (default): Phase 1 broker — local WAL, one process.
//	cluster (--cluster):   Raft-replicated broker — this process is one
//	                       node of a quorum; writes go through the log.
//
// Cluster example (3 nodes on one host):
//
//	broker --cluster --node-id node1 --port 9001 --raft-port 8001 \
//	       --data-dir data/node1 \
//	       --peers node2=localhost:8002=localhost:9002,node3=localhost:8003=localhost:9003
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/raft"
	"github.com/skkompella/distqueue/server"
)

func main() {
	var (
		port         = flag.Int("port", 9000, "client gRPC listen port")
		walPath      = flag.String("wal", "broker.wal", "write-ahead log path (single-node mode)")
		taskTimeout  = flag.Duration("task-timeout", 30*time.Second, "redeliver tasks not acked within this duration")
		maxRetries   = flag.Int("max-retries", 5, "nacks/timeouts before a task is dead-lettered")
		compactEvery = flag.Int("compact-every", 10000, "compact the WAL every N entries (single-node mode, 0 = never)")

		metricsPort = flag.Int("metrics-port", 0, "Prometheus /metrics port (0 = disabled)")

		cluster   = flag.Bool("cluster", false, "run as a Raft cluster node")
		nodeID    = flag.String("node-id", "", "this node's ID (cluster mode)")
		raftPort  = flag.Int("raft-port", 8000, "raft RPC listen port (cluster mode)")
		dataDir   = flag.String("data-dir", "data", "raft state + snapshot directory (cluster mode)")
		peersFlag = flag.String("peers", "", "peers as id=raftAddr=clientAddr,... (cluster mode)")
		// Persistence rewrites the whole log per write, so log length is
		// the dominant cost of a replicated op: snapshot often.
		snapEvery = flag.Int("snapshot-threshold", 1000, "snapshot every N applied entries (cluster mode, 0 = never)")
	)
	flag.Parse()

	if *cluster {
		runCluster(*nodeID, *port, *raftPort, *metricsPort, *dataDir, *peersFlag, *taskTimeout, *maxRetries, *snapEvery)
		return
	}
	runSingle(*port, *metricsPort, *walPath, *taskTimeout, *maxRetries, *compactEvery)
}

func runSingle(port, metricsPort int, walPath string, taskTimeout time.Duration, maxRetries, compactEvery int) {
	cfg := broker.DefaultConfig(walPath)
	cfg.TaskTimeout = taskTimeout
	cfg.MaxRetries = maxRetries
	cfg.CompactEvery = compactEvery

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("starting broker: %v", err)
	}
	if s := b.Stats(); s.Pending > 0 || s.DLQ > 0 {
		log.Printf("recovered from WAL: %d pending, %d dead-lettered", s.Pending, s.DLQ)
	}
	if metricsPort > 0 {
		server.ServeMetrics(metricsPort, "single", nil, b)
	}

	grpcServer := grpc.NewServer()
	server.New(b).Register(grpcServer)
	serveUntilSignal(grpcServer, port,
		fmt.Sprintf("single-node broker (wal=%s timeout=%s retries=%d)", walPath, taskTimeout, maxRetries))
	if err := b.Close(); err != nil {
		log.Printf("close broker: %v", err)
	}
}

func runCluster(nodeID string, port, raftPort, metricsPort int, dataDir, peersFlag string, taskTimeout time.Duration, maxRetries, snapEvery int) {
	if nodeID == "" {
		log.Fatal("--node-id is required in cluster mode")
	}
	peerRaft, peerClient, err := parsePeers(peersFlag)
	if err != nil {
		log.Fatalf("--peers: %v", err)
	}
	peerClient[nodeID] = fmt.Sprintf("localhost:%d", port)

	var peerIDs []string
	for id := range peerRaft {
		peerIDs = append(peerIDs, id)
	}

	applyCh := make(chan raft.ApplyMsg, 1024)
	node, err := raft.NewNode(raft.Config{
		ID:      nodeID,
		Peers:   peerIDs,
		DataDir: dataDir,
		Logger:  log.New(os.Stderr, "raft ", log.LstdFlags|log.Lmicroseconds),
	}, raft.NewGRPCTransport(peerRaft), applyCh)
	if err != nil {
		log.Fatalf("starting raft node: %v", err)
	}

	sm := broker.NewStateMachine(maxRetries)
	cs := server.NewClusterServer(node, sm, server.ClusterConfig{
		TaskTimeout:       taskTimeout,
		MaxRetries:        maxRetries,
		SnapshotThreshold: snapEvery,
		ClientAddrs:       peerClient,
	})

	// Raft RPC service on its own port.
	raftLis, err := net.Listen("tcp", fmt.Sprintf(":%d", raftPort))
	if err != nil {
		log.Fatalf("raft listen: %v", err)
	}
	raftSrv := grpc.NewServer()
	raft.NewGRPCServer(node).Register(raftSrv)
	go func() {
		if err := raftSrv.Serve(raftLis); err != nil {
			log.Fatalf("raft serve: %v", err)
		}
	}()

	node.Start()
	go cs.Run(applyCh)
	if metricsPort > 0 {
		server.ServeMetrics(metricsPort, nodeID, node, sm)
	}

	grpcServer := grpc.NewServer()
	cs.Register(grpcServer)
	serveUntilSignal(grpcServer, port,
		fmt.Sprintf("cluster node %s (raft=:%d data=%s peers=%d)", nodeID, raftPort, dataDir, len(peerIDs)))

	raftSrv.GracefulStop()
	node.Stop()
	cs.Stop()
}

// parsePeers parses "id=raftAddr=clientAddr,..." into two maps.
func parsePeers(s string) (raftAddrs, clientAddrs map[string]string, err error) {
	raftAddrs = make(map[string]string)
	clientAddrs = make(map[string]string)
	if s == "" {
		return raftAddrs, clientAddrs, nil
	}
	for _, part := range strings.Split(s, ",") {
		fields := strings.Split(part, "=")
		if len(fields) != 3 {
			return nil, nil, fmt.Errorf("bad peer %q (want id=raftAddr=clientAddr)", part)
		}
		raftAddrs[fields[0]] = fields[1]
		clientAddrs[fields[0]] = fields[2]
	}
	return raftAddrs, clientAddrs, nil
}

func serveUntilSignal(g *grpc.Server, port int, banner string) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down")
		g.GracefulStop()
	}()
	log.Printf("%s listening on :%d", banner, port)
	if err := g.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
