package server

import (
	"context"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	mu sync.RWMutex

	logger *zap.Logger

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener

	clusterID uint64
	memberID  uint64

	mykvstoreserverpb.UnimplementedKVServer
	mykvstoreserverpb.UnimplementedWatchServer
}

type Config struct {
	Logger *zap.Logger

	ListenAddr string
	ClusterID  uint64
	MemberID   uint64
}

func NewConfig(cfg *Config) *Server {
	return &Server{
		logger: cfg.Logger,

		clusterID: cfg.ClusterID,
		memberID:  cfg.MemberID,
	}
}

func (s *Server) Put(ctx context.Context, req *mykvstoreserverpb.PutRequest) (*mykvstoreserverpb.PutResponse, error) {

	s.logger.Debug("Put request",
		zap.ByteString("key", req.Key),
		zap.Int("value_size", len(req.Value)),
		zap.Int64("lease", req.Lease),
	)

	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	// Get previous value if requested

	return nil, nil
}

func (s *Server) Start(listenAddr string) error {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = lis

	s.grpcServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024), // 10 MB
		grpc.MaxSendMsgSize(10*1024*1024), // 10 MB
	)

	mykvstoreserverpb.RegisterKVServer(s.grpcServer, s)
	mykvstoreserverpb.RegisterWatchServer(s.grpcServer, s)

	s.logger.Info("Starting gRPC server", zap.String("addr", listenAddr))

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) checkLeader() error {
	if !s.raftNode.IsLeader() {
		return fmt.Errorf("not leader")
	}
	return nil
}
