package server

import (
	"context"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/lease"
	"mykvstore/pkg/raftnode"
	"time"

	"go.uber.org/zap"
)

func (s *Server) Range(ctx context.Context, req *mykvstoreserverpb.RangeRequest) (*mykvstoreserverpb.RangeResponse, error) {

	s.logger.Debug("Range request",
		zap.ByteString("key", req.Key),
		zap.ByteString("range_end", req.RangeEnd),
		zap.Int64("revision", req.Revision),
	)

	// Reads from followers may return stale data. For linearizable reads, must read from leader
	if !req.Serializable {
		if !s.raftNode.IsLeader() {
			leaderID := s.raftNode.LeaderID()
			return nil, NotLeaderError(leaderID, "unknown")
		}
	}

	result, err := s.mvccStore.Range(
		req.Key,
		req.RangeEnd,
		req.Revision,
		int(req.Limit),
	)
	if err != nil {
		s.logger.Error("Range failed", zap.Error(err))
		return nil, err
	}

	kvs := make([]*mykvstoreserverpb.KeyValue, len(result.KVs))
	for i, kv := range result.KVs {
		kvs[i] = &mykvstoreserverpb.KeyValue{
			Key:            kv.Key,
			Value:          kv.Value,
			CreateRevision: kv.CreateRevision,
			ModRevision:    kv.ModRevision,
			Version:        kv.Version,
			Lease:          kv.Lease,
		}
	}

	s.logger.Debug("Range response", zap.Int("count", len(kvs)))

	return &mykvstoreserverpb.RangeResponse{
		Header: s.makeHeader(),
		Kvs:    kvs,
		More:   false,
		Count:  result.Count,
	}, nil
}

func (s *Server) Put(ctx context.Context, req *mykvstoreserverpb.PutRequest) (*mykvstoreserverpb.PutResponse, error) {

	s.logger.Debug("Put request",
		zap.ByteString("key", req.Key),
		zap.Int("value_size", len(req.Value)),
		zap.Int64("lease", req.Lease),
	)

	if err := s.checkLeader(); err != nil {
		leaderID := s.raftNode.LeaderID()
		return nil, NotLeaderError(leaderID, "unknown")
	}

	// Get previous value if requested
	var prevKv *mykvstoreserverpb.KeyValue
	if req.PrevKv {
		kv, err := s.mvccStore.Get(req.Key, 0)
		if err == nil {
			prevKv = &mykvstoreserverpb.KeyValue{
				Key:            kv.Key,
				Value:          kv.Value,
				CreateRevision: kv.CreateRevision,
				ModRevision:    kv.ModRevision,
				Version:        kv.Version,
				Lease:          kv.Lease,
			}
		}
	}

	// Create Raft command
	reqID := ReqID(s.reqIDGen.Next())
	ch := s.applyWait.Register(reqID)
	defer s.applyWait.Cancel(reqID)

	s.logger.Debug("Registered request", zap.Uint64("req_id", uint64(reqID)))

	cmd := raftnode.NewPutCommand(req.Key, req.Value, req.Lease, uint64(reqID))

	// Propose through Raft
	if err := s.raftNode.Propose(cmd); err != nil {
		s.logger.Error("Failed to propose", zap.Error(err))
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	// Wait for apply
	if _, err := Wait(ctx, ch, 5*time.Second); err != nil {
		s.logger.Error("Request failed", zap.Error(err))
		return nil, err
	}

	s.logger.Debug("Put completed", zap.Uint64("req_id", uint64(reqID)))

	if req.Lease != 0 {
		if err := s.lessor.AttachKey(lease.LeaseID(req.Lease), string(req.Key)); err != nil {
			s.logger.Warn("Failed to attach key to lease",
				zap.Int64("lease", req.Lease),
				zap.ByteString("key", req.Key),
				zap.Error(err),
			)
		}
	}

	return &mykvstoreserverpb.PutResponse{
		Header: s.makeHeader(),
		PrevKv: prevKv,
	}, nil
}

func (s *Server) DeleteRange(ctx context.Context, req *mykvstoreserverpb.DeleteRangeRequest) (*mykvstoreserverpb.DeleteRangeResponse, error) {

	s.logger.Debug("DeleteRange request",
		zap.ByteString("key", req.Key),
		zap.ByteString("range_end", req.RangeEnd),
	)

	if err := s.checkLeader(); err != nil {
		leaderID := s.raftNode.LeaderID()
		return nil, NotLeaderError(leaderID, "unknown")
	}

	// Get previous value if requested
	var prevKvs []*mykvstoreserverpb.KeyValue
	if req.PrevKv {
		result, err := s.mvccStore.Range(req.Key, req.RangeEnd, 0, 0)
		if err == nil {
			for _, kv := range result.KVs {
				prevKvs = append(prevKvs, &mykvstoreserverpb.KeyValue{
					Key:            kv.Key,
					Value:          kv.Value,
					CreateRevision: kv.CreateRevision,
					ModRevision:    kv.ModRevision,
					Version:        kv.Version,
					Lease:          kv.Lease,
				},
				)
			}
		}
	}

	if len(req.RangeEnd) > 0 {
		return nil, fmt.Errorf("range delete not yet implemented")
	}

	reqID := ReqID(s.reqIDGen.Next())
	ch := s.applyWait.Register(reqID)
	defer s.applyWait.Cancel(reqID)
	cmd := raftnode.NewDeleteCommand(req.Key, uint64(reqID))

	if err := s.raftNode.Propose(cmd); err != nil {
		s.logger.Error("Failed to propose", zap.Error(err))
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	if _, err := Wait(ctx, ch, 5*time.Second); err != nil {
		s.logger.Error("Request failed", zap.Error(err))
		return nil, err
	}

	s.logger.Debug("Delete completed", zap.Uint64("req_id", uint64(reqID)))

	return &mykvstoreserverpb.DeleteRangeResponse{
		Header:  s.makeHeader(),
		Deleted: int64(len(prevKvs)),
		PrevKvs: prevKvs,
	}, nil
}

func (s *Server) Compact(ctx context.Context, req *mykvstoreserverpb.CompactionRequest) (*mykvstoreserverpb.CompactionResponse, error) {

	s.logger.Debug("Compact request", zap.Int64("revision", req.Revision))

	if err := s.checkLeader(); err != nil {
		leaderID := s.raftNode.LeaderID()
		return nil, NotLeaderError(leaderID, "unknown")
	}

	reqID := ReqID(s.reqIDGen.Next())
	ch := s.applyWait.Register(reqID)
	defer s.applyWait.Cancel(reqID)
	cmd := raftnode.NewCompactCommand(req.Revision, uint64(reqID))

	if err := s.raftNode.Propose(cmd); err != nil {
		s.logger.Error("Failed to propose", zap.Error(err))
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	if _, err := Wait(ctx, ch, 5*time.Second); err != nil {
		s.logger.Error("Request failed", zap.Error(err))
		return nil, err
	}

	s.logger.Debug("Compact completed", zap.Uint64("req_id", uint64(reqID)))

	return &mykvstoreserverpb.CompactionResponse{
		Header: s.makeHeader(),
	}, nil
}
