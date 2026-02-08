package server

import (
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/cluster"
	"mykvstore/pkg/lease"
	"mykvstore/pkg/mvcc"
	"mykvstore/pkg/raftnode"
	"mykvstore/pkg/storage"
	"mykvstore/pkg/watch"
	"net"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
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

	memberStore   *cluster.MemberStore
	healthChecker *cluster.HealthChecker

	clusterID uint64
	memberID  uint64

	applyWait *ApplyWait
	reqIDGen  *ReqIDGen

	lessor *lease.Lessor

	mykvstoreserverpb.UnimplementedKVServer
	mykvstoreserverpb.UnimplementedWatchServer
	mykvstoreserverpb.UnimplementedLeaseServer
	mykvstoreserverpb.UnimplementedClusterServer
	mykvstoreserverpb.UnimplementedMaintenanceServer
}

type Config struct {
	RaftNode  *raftnode.Node
	MVCCStore *mvcc.Store
	Backend   *storage.BoltBackend
	Logger    *zap.Logger

	ListenAddr string
	ClusterID  uint64
	MemberID   uint64
	PeerURL    string
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
		lessor:       lease.NewLessor(cfg.Backend, cfg.Logger),
	}

	cfg.RaftNode.SetApplyCallback(s.onApply)

	cfg.RaftNode.SetWatchCallback(s.onWatch)

	s.lessor.SetExpiredCallback(s.onLeaseExpire)
	s.lessor.Start()

	s.memberStore = cluster.NewMemberStore(cfg.Backend, cfg.Logger)

	cfg.RaftNode.SetMemberStore(s.memberStore)
	cfg.RaftNode.SetConfChangeCallback(s.onConfChange)

	if len(s.memberStore.List()) == 0 {
		self := &cluster.Member{
			ID:         cfg.MemberID,
			Name:       fmt.Sprintln("member-%d", cfg.MemberID),
			PeerURLs:   []string{cfg.PeerURL},
			ClientURLs: []string{cfg.ListenAddr},
			IsLearner:  false,
		}
		if err := s.memberStore.Add(self); err != nil {
			cfg.Logger.Warn("Failed to register self in member store", zap.Error(err))
		}
	}

	s.healthChecker = cluster.NewHealthChecker(5*time.Second, 2*time.Second, cfg.Logger)
	s.healthChecker.Start(s.memberStore, s.checkMemberHealth)

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

// onConfChange is called after a ConfgChange is commited and applied
func (s *Server) onConfChange(cc raftpb.ConfChange, state *raftpb.ConfState) {
	switch cc.Type {

	// A new member was added
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		member := s.memberStore.Get(cc.NodeID)
		if member != nil {
			s.logger.Info("New member added, update transport",
				zap.Uint64("member_id", member.ID),
				zap.Strings("peer_urls", member.PeerURLs),
			)
		}

	case raftpb.ConfChangeRemoveNode:
		s.logger.Info("Member removed, update transport",
			zap.Uint64("member_id", cc.NodeID),
		)
	}
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
	mykvstoreserverpb.RegisterLeaseServer(s.grpcServer, s)
	mykvstoreserverpb.RegisterClusterServer(s.grpcServer, s)
	mykvstoreserverpb.RegisterMaintenanceServer(s.grpcServer, s)

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
	if s.healthChecker != nil {
		s.healthChecker.Stop()
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

func (s *Server) checkMemberHealth(u uint64) error {
	// TODO: Implement
	return nil
}
