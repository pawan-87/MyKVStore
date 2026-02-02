package server

import (
	"context"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/raftnode"
	"time"

	"go.uber.org/zap"
)

func (s *Server) Range(ctx context.Context, req *mykvstoreserverpb.RangeRequest) (*mykvstoreserverpb.RangeResponse, error) {

	s.logger.Debug("Range request",
		zap.ByteString("key", req.Key),
		zap.ByteString("range_end", req.RangeEnd),
		zap.Int64("revision", req.Revision),
	)

	result, err := s.mvccStore.Range(
		req.Key,
		req.RangeEnd,
		req.Revision,
		int(req.Limit),
	)
	if err != nil {
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
		return nil, err
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
	cmd := raftnode.NewPutCommand(req.Key, req.Value, req.Lease)

	// Propose through Raft
	if err := s.raftNode.Propose(cmd); err != nil {
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

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
		return nil, err
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

	cmd := raftnode.NewDeleteCommand(req.Key)

	if err := s.raftNode.Propose(cmd); err != nil {
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

	return &mykvstoreserverpb.DeleteRangeResponse{
		Header:  s.makeHeader(),
		Deleted: int64(len(prevKvs)),
		PrevKvs: prevKvs,
	}, nil
}

func (s *Server) Compact(ctx context.Context, req *mykvstoreserverpb.CompactionRequest) (*mykvstoreserverpb.CompactionResponse, error) {

	s.logger.Debug("Compact request", zap.Int64("revision", req.Revision))

	if err := s.checkLeader(); err != nil {
		return nil, err
	}

	cmd := raftnode.NewCompactCommand(req.Revision)

	if err := s.raftNode.Propose(cmd); err != nil {
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

	return &mykvstoreserverpb.CompactionResponse{
		Header: s.makeHeader(),
	}, nil
}
