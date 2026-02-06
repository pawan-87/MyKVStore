package server

import (
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/lease"
	"mykvstore/pkg/mvcc"
	"mykvstore/pkg/raftnode"
	"mykvstore/pkg/watch"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	mu sync.RWMutex

	raftNode     *raftnode.Node
	mvccStore    *mvcc.Store
	logger       *zap.Logger
	watchManager *watch.WatchManager

	grpcServer *grpc.Server
	listener   net.Listener

	clusterID uint64
	memberID  uint64

	applyWait *ApplyWait
	reqIDGen  *ReqIDGen

	lessor *lease.Lessor

	mykvstoreserverpb.UnimplementedKVServer
	mykvstoreserverpb.UnimplementedWatchServer
	mykvstoreserverpb.UnimplementedLeaseServer
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
	s := &Server{
		raftNode:     cfg.RaftNode,
		mvccStore:    cfg.MVCCStore,
		logger:       cfg.Logger,
		clusterID:    cfg.ClusterID,
		memberID:     cfg.MemberID,
		applyWait:    NewApplyWait(),
		reqIDGen:     NewReqIDGen(cfg.MemberID),
		watchManager: watch.NewWatchManager(cfg.MVCCStore, cfg.Logger),
		lessor:       lease.NewLessor(cfg.Logger),
	}

	cfg.RaftNode.SetApplyCallback(s.onApply)

	cfg.RaftNode.SetWatchCallback(s.onWatch)

	s.lessor.SetExpiredCallback(s.onLeaseExpire)
	s.lessor.Start()

	return s
}

// onApply is called when
func (s *Server) onApply(reqID uint64, result interface{}, err error) {
	s.logger.Debug("Command applied",
		zap.Uint64("req_id", reqID),
		zap.Error(err),
	)

	// Trigger the waiting request
	s.applyWait.Trigger(ReqID(reqID), ApplyResult{Data: result, Err: err})
}

// onWatch is called when a key changes
func (s *Server) onWatch(key []byte, eventType int, kv, prevKV interface{}) {
	var pbEventType mykvstoreserverpb.Event_EventType

	if eventType == 0 {
		pbEventType = mykvstoreserverpb.Event_PUT
	} else {
		pbEventType = mykvstoreserverpb.Event_DELETE
	}

	var mvccKV *mvcc.KeyValue
	if kv != nil {
		mvccKV = kv.(*mvcc.KeyValue)
	}

	var mvccPrevKV *mvcc.KeyValue
	if prevKV != nil {
		mvccPrevKV = prevKV.(*mvcc.KeyValue)
	}

	// Notify watch manager
	s.watchManager.Notify(key, pbEventType, mvccKV, mvccPrevKV)
}

func (s *Server) onLeaseExpire(leaseID lease.LeaseID, keys []string) {
	s.logger.Debug("Lease expired, deleting keys",
		zap.Int64("lease_id", int64(leaseID)),
		zap.Int("keys", len(keys)),
	)

	for _, key := range keys {
		s.mvccStore.Delete([]byte(key))
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
	if s.lessor != nil {
		s.lessor.Stop()
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
