package server

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrNotLeader      = status.Error(codes.FailedPrecondition, "not leader")
	ErrTimeout        = status.Error(codes.DeadlineExceeded, "request timeout")
	ErrKeyNotFound    = status.Error(codes.NotFound, "key not found")
	ErrInvalidRequest = status.Error(codes.InvalidArgument, "invalid request")
)

func WrapError(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return status.Error(codes.Internal, err.Error())
}

func NotLeaderError(leaderID uint64, leaderAddr string) error {
	msg := fmt.Sprintf("not leader, current leader: %d (%s)", leaderID, leaderAddr)
	return status.Errorf(codes.FailedPrecondition, msg)
}
