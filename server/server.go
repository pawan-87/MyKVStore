package server

import (
	"fmt"
	"mykvstore/mvcc"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/raftnode"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	mu sync.RWMutex

	// Components
	raftNode  *raftnode.Node
	mvccStore *mvcc.Store
	logger    *zap.Logger

	// gRPC server
	grpcServer *grpc.Server
	listener   net.Listener

	clusterID uint64
	memberID  uint64

	mykvstoreserverpb.UnimplementedKVServer
	mykvstoreserverpb.UnimplementedWatchServer
}

type Config struct {
	RaftNode  *raftnode.Node
	MVCCStore *mvcc.Store
	Logger    *zap.Logger

	ListenAddr string
	ClusterID  uint64
	MemberID   uint64
}

func NewConfig(cfg *Config) *Server {
	return &Server{
		raftNode:  cfg.RaftNode,
		mvccStore: cfg.MVCCStore,
		logger:    cfg.Logger,
		clusterID: cfg.ClusterID,
		memberID:  cfg.MemberID,
	}
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

func (s *Server) makeHeader() *mykvstoreserverpb.ResponseHeader {
	return &mykvstoreserverpb.ResponseHeader{
		ClusterId: s.clusterID,
		MemberId:  s.memberID,
		Revision:  s.mvccStore.CurrentRevision(),
		RaftTerm:  0,
	}
}
