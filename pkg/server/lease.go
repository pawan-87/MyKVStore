package server

import (
	"context"
	"fmt"
	"github.com/pawan-87/MyKVStore/mykvstoreserverpb"
	"github.com/pawan-87/MyKVStore/pkg/lease"
	"github.com/pawan-87/MyKVStore/pkg/raftnode"
	"io"
	"time"

	"go.uber.org/zap"
)

func (s *Server) LeaseGrant(ctx context.Context, req *mykvstoreserverpb.LeaseGrantRequest) (*mykvstoreserverpb.LeaseGrantResponse, error) {

	s.logger.Debug("LeaseGrant request",
		zap.Int64("lease_id", req.ID),
		zap.Int64("ttl", req.TTL),
	)

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	if req.TTL <= 0 {
		return nil, fmt.Errorf("TTL must be positive")
	}

	leaseID := req.ID
	if leaseID == 0 {
		leaseID = time.Now().UnixNano()
	}

	reqID := ReqID(s.reqIDGen.Next())
	ch := s.applyWait.Register(reqID)
	defer s.applyWait.Cancel(reqID)

	cmd := raftnode.NewLeaseGrantCommand(leaseID, req.TTL, uint64(reqID))

	if err := s.raftNode.Propose(cmd); err != nil {
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	_, err := Wait(ctx, ch, 5*time.Second)
	if err != nil {
		return nil, err
	}

	_, err = s.lessor.Grant(lease.LeaseID(leaseID), req.TTL)
	if err != nil {
		return nil, err
	}

	return &mykvstoreserverpb.LeaseGrantResponse{
		Header: s.makeHeader(),
		ID:     leaseID,
		TTL:    req.TTL,
	}, nil
}

func (s *Server) LeaseRevoke(ctx context.Context, req *mykvstoreserverpb.LeaseRevokeRequest) (*mykvstoreserverpb.LeaseRevokeResponse, error) {

	s.logger.Debug("LeaseRevoke request",
		zap.Int64("lease_id", req.ID),
	)

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	reqID := ReqID(s.reqIDGen.Next())
	ch := s.applyWait.Register(reqID)
	defer s.applyWait.Cancel(reqID)

	cmd := raftnode.NewLeaseRevokeCommand(req.ID, uint64(reqID))

	if err := s.raftNode.Propose(cmd); err != nil {
		return nil, fmt.Errorf("failed to propose: %w", err)
	}

	_, err := Wait(ctx, ch, 5*time.Second)
	if err != nil {
		return nil, err
	}

	keys, err := s.lessor.Revoke(lease.LeaseID(req.ID))
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		s.mvccStore.Delete([]byte(key))
	}

	return &mykvstoreserverpb.LeaseRevokeResponse{
		Header: s.makeHeader(),
	}, nil
}

func (s *Server) LeaseKeepAlive(stream mykvstoreserverpb.Lease_LeaseKeepAliveServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		reqID := ReqID(s.reqIDGen.Next())
		ch := s.applyWait.Register(reqID)

		cmd := raftnode.NewLeaseKeepAliveCommand(req.ID, uint64(reqID))

		if err := s.raftNode.Propose(cmd); err != nil {
			s.applyWait.Cancel(reqID)
			s.logger.Warn("Failed to propose lease keep-alive",
				zap.Int64("lease_id", req.ID),
				zap.Error(err),
			)
			// Send TTL=0 so the client knows the keep-alive failed
			resp := &mykvstoreserverpb.LeaseKeepAliveResponse{
				Header: s.makeHeader(),
				ID:     req.ID,
				TTL:    0,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			continue
		}

		result, err := Wait(stream.Context(), ch, 5*time.Second)
		s.applyWait.Cancel(reqID)

		var ttl int64
		resultTTL, ok := result.Data.(int64)
		if ok {
			ttl = resultTTL
		}

		resp := &mykvstoreserverpb.LeaseKeepAliveResponse{
			Header: s.makeHeader(),
			ID:     req.ID,
			TTL:    ttl,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *Server) LeaseTimeToLive(ctx context.Context, req *mykvstoreserverpb.LeaseTimeToLiveRequest) (*mykvstoreserverpb.LeaseTimeToLiveResponse, error) {

	l := s.lessor.Lookup(lease.LeaseID(req.ID))
	if l == nil {
		return nil, fmt.Errorf("lease not found: %d", req.ID)
	}

	resp := &mykvstoreserverpb.LeaseTimeToLiveResponse{
		Header:     s.makeHeader(),
		ID:         req.ID,
		TTL:        l.Remaining(),
		GrantedTTL: l.TTL,
	}

	if req.Keys {
		keys := l.Keys()
		resp.Keys = make([][]byte, len(keys))
		for i, k := range keys {
			resp.Keys[i] = []byte(k)
		}
	}

	return resp, nil
}
