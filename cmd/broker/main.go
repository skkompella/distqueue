// The broker binary: runs the task queue and serves the gRPC API.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/skkompella/distqueue/broker"
	"github.com/skkompella/distqueue/server"
)

func main() {
	var (
		port         = flag.Int("port", 9000, "gRPC listen port")
		walPath      = flag.String("wal", "broker.wal", "write-ahead log path")
		taskTimeout  = flag.Duration("task-timeout", 30*time.Second, "redeliver tasks not acked within this duration")
		maxRetries   = flag.Int("max-retries", 5, "nacks/timeouts before a task is dead-lettered")
		compactEvery = flag.Int("compact-every", 10000, "compact the WAL every N entries (0 = never)")
	)
	flag.Parse()

	cfg := broker.DefaultConfig(*walPath)
	cfg.TaskTimeout = *taskTimeout
	cfg.MaxRetries = *maxRetries
	cfg.CompactEvery = *compactEvery

	b, err := broker.New(cfg)
	if err != nil {
		log.Fatalf("starting broker: %v", err)
	}
	if s := b.Stats(); s.Pending > 0 || s.DLQ > 0 {
		log.Printf("recovered from WAL: %d pending, %d dead-lettered", s.Pending, s.DLQ)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	server.New(b).Register(grpcServer)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down")
		grpcServer.GracefulStop()
	}()

	log.Printf("broker listening on :%d (wal=%s timeout=%s retries=%d)",
		*port, *walPath, *taskTimeout, *maxRetries)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
	if err := b.Close(); err != nil {
		log.Printf("close broker: %v", err)
	}
}
