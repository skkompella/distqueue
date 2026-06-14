// Package client provides a TaskQueueClient that fails over across the
// nodes of a distqueue cluster: it follows "leader=" redirect hints, and
// rotates to the next node when the current one is down or won't answer.
// It implements queuepb.TaskQueueClient, so it drops into the worker SDK
// (and any other call site) unchanged.
package client

import (
	"context"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/skkompella/distqueue/gen/queuepb"
)

// Same rationale as the raft transport: a node that was down for a while
// must become usable promptly once it returns, not after gRPC's default
// (up to 120s) reconnect backoff drains.
var fastReconnect = grpc.WithConnectParams(grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  100 * time.Millisecond,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   time.Second,
	},
	MinConnectTimeout: time.Second,
})

const maxAttempts = 6

type Failover struct {
	mu    sync.Mutex
	addrs []string
	cur   int
	conns map[string]*grpc.ClientConn
}

var _ queuepb.TaskQueueClient = (*Failover)(nil)

// New returns a failover client over the given node addresses.
func New(addrs []string) *Failover {
	return &Failover{addrs: addrs, conns: make(map[string]*grpc.ClientConn)}
}

func (f *Failover) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.conns {
		c.Close()
	}
	f.conns = make(map[string]*grpc.ClientConn)
}

// current returns a client for the node we currently believe is leader.
func (f *Failover) current() (queuepb.TaskQueueClient, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	addr := f.addrs[f.cur]
	conn, ok := f.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			fastReconnect)
		if err != nil {
			return nil, err
		}
		f.conns[addr] = conn
	}
	return queuepb.NewTaskQueueClient(conn), nil
}

// advance rotates to the next node; if hint is non-empty (a "leader=addr"
// redirect), jump straight to it instead.
func (f *Failover) advance(hint string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if hint != "" {
		for i, a := range f.addrs {
			if a == hint {
				f.cur = i
				return
			}
		}
		// Unknown address (e.g. cluster reconfigured): track it.
		f.addrs = append(f.addrs, hint)
		f.cur = len(f.addrs) - 1
		return
	}
	f.cur = (f.cur + 1) % len(f.addrs)
}

// leaderHint extracts the redirect target from a "not leader; leader=addr"
// error, or "" if the leader is unknown.
func leaderHint(err error) string {
	msg := status.Convert(err).Message()
	if i := strings.Index(msg, "leader="); i >= 0 {
		return strings.TrimSpace(msg[i+len("leader="):])
	}
	return ""
}

// do runs one RPC with redirect-following and node rotation. Genuine
// application errors (NotFound for an empty queue, InvalidArgument…) pass
// through untouched — only routing errors trigger failover.
func do[R any](f *Failover, ctx context.Context, rpc func(queuepb.TaskQueueClient) (R, error)) (R, error) {
	var zero R
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		c, err := f.current()
		if err != nil {
			lastErr = err
			f.advance("")
			continue
		}
		resp, err := rpc(c)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		switch status.Code(err) {
		case codes.FailedPrecondition: // not leader
			f.advance(leaderHint(err))
		case codes.Unavailable, codes.DeadlineExceeded: // node down / lost quorum
			f.advance("")
			// Brief pause: an election is likely in progress.
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return zero, ctx.Err()
			}
		default:
			return zero, err // real answer from the cluster
		}
	}
	return zero, lastErr
}

func (f *Failover) Enqueue(ctx context.Context, req *queuepb.EnqueueRequest, opts ...grpc.CallOption) (*queuepb.EnqueueResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.EnqueueResponse, error) {
		return c.Enqueue(ctx, req, opts...)
	})
}

func (f *Failover) Dequeue(ctx context.Context, req *queuepb.DequeueRequest, opts ...grpc.CallOption) (*queuepb.DequeueResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.DequeueResponse, error) {
		return c.Dequeue(ctx, req, opts...)
	})
}

func (f *Failover) Ack(ctx context.Context, req *queuepb.AckRequest, opts ...grpc.CallOption) (*queuepb.AckResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.AckResponse, error) {
		return c.Ack(ctx, req, opts...)
	})
}

func (f *Failover) Nack(ctx context.Context, req *queuepb.NackRequest, opts ...grpc.CallOption) (*queuepb.NackResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.NackResponse, error) {
		return c.Nack(ctx, req, opts...)
	})
}

func (f *Failover) Stats(ctx context.Context, req *queuepb.StatsRequest, opts ...grpc.CallOption) (*queuepb.StatsResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.StatsResponse, error) {
		return c.Stats(ctx, req, opts...)
	})
}

func (f *Failover) ListDLQ(ctx context.Context, req *queuepb.ListDLQRequest, opts ...grpc.CallOption) (*queuepb.ListDLQResponse, error) {
	return do(f, ctx, func(c queuepb.TaskQueueClient) (*queuepb.ListDLQResponse, error) {
		return c.ListDLQ(ctx, req, opts...)
	})
}
