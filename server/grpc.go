// Package server exposes a Broker over gRPC. It is a thin adapter: proto
// messages map to broker types, broker errors map to gRPC status codes.
package server

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/srihari-kompella/distqueue/broker"
	"github.com/srihari-kompella/distqueue/gen/queuepb"
)

type TaskQueueServer struct {
	queuepb.UnimplementedTaskQueueServer
	broker *broker.Broker
}

func New(b *broker.Broker) *TaskQueueServer {
	return &TaskQueueServer{broker: b}
}

// Register attaches the service to a gRPC server.
func (s *TaskQueueServer) Register(g *grpc.Server) {
	queuepb.RegisterTaskQueueServer(g, s)
}

func (s *TaskQueueServer) Enqueue(_ context.Context, req *queuepb.EnqueueRequest) (*queuepb.EnqueueResponse, error) {
	if req.GetTask() == nil {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	t := fromProto(req.GetTask())
	if err := s.broker.Enqueue(t); err != nil {
		return nil, toStatus(err)
	}
	return &queuepb.EnqueueResponse{TaskId: t.ID}, nil
}

func (s *TaskQueueServer) Dequeue(_ context.Context, _ *queuepb.DequeueRequest) (*queuepb.DequeueResponse, error) {
	t, err := s.broker.Dequeue()
	if err != nil {
		return nil, toStatus(err)
	}
	return &queuepb.DequeueResponse{Task: toProto(t)}, nil
}

func (s *TaskQueueServer) Ack(_ context.Context, req *queuepb.AckRequest) (*queuepb.AckResponse, error) {
	if err := s.broker.Ack(req.GetTaskId()); err != nil {
		return nil, toStatus(err)
	}
	return &queuepb.AckResponse{}, nil
}

func (s *TaskQueueServer) Nack(_ context.Context, req *queuepb.NackRequest) (*queuepb.NackResponse, error) {
	if err := s.broker.Nack(req.GetTaskId()); err != nil {
		return nil, toStatus(err)
	}
	return &queuepb.NackResponse{}, nil
}

func (s *TaskQueueServer) Stats(_ context.Context, _ *queuepb.StatsRequest) (*queuepb.StatsResponse, error) {
	st := s.broker.Stats()
	return &queuepb.StatsResponse{
		Pending:  int64(st.Pending),
		InFlight: int64(st.InFlight),
		Dlq:      int64(st.DLQ),
		Acked:    st.Acked,
	}, nil
}

func (s *TaskQueueServer) ListDLQ(_ context.Context, _ *queuepb.ListDLQRequest) (*queuepb.ListDLQResponse, error) {
	tasks := s.broker.ListDLQ()
	out := make([]*queuepb.Task, len(tasks))
	for i, t := range tasks {
		out[i] = toProto(t)
	}
	return &queuepb.ListDLQResponse{Tasks: out}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, broker.ErrEmpty):
		return status.Error(codes.NotFound, "queue is empty")
	case errors.Is(err, broker.ErrUnknownTask):
		return status.Error(codes.NotFound, "task not in flight")
	case errors.Is(err, broker.ErrClosed):
		return status.Error(codes.Unavailable, "broker is shutting down")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func fromProto(p *queuepb.Task) *broker.Task {
	t := &broker.Task{
		ID:         p.GetId(),
		Payload:    p.GetPayload(),
		Priority:   int(p.GetPriority()),
		RetryCount: int(p.GetRetryCount()),
	}
	if p.GetCreatedAt() != nil {
		t.CreatedAt = p.GetCreatedAt().AsTime()
	}
	return t
}

func toProto(t *broker.Task) *queuepb.Task {
	return &queuepb.Task{
		Id:         t.ID,
		Payload:    t.Payload,
		Priority:   int32(t.Priority),
		RetryCount: int32(t.RetryCount),
		CreatedAt:  timestamppb.New(t.CreatedAt),
	}
}
