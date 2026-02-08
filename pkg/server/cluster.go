package server

import (
	"context"
	"encoding/json"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/cluster"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func (s *Server) MemberAdd(ctx context.Context, req *mykvstoreserverpb.MemberAddRequest) (*mykvstoreserverpb.MemberAddResponse, error) {

	s.logger.Info("MemberAdd request",
		zap.Strings("peer_urls", req.PeerURLs),
		zap.Bool("is_learner", req.IsLearner),
	)

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	if len(req.PeerURLs) == 0 {
		return nil, fmt.Errorf("member peer URLs required")
	}

	memberID := uint64(time.Now().UnixNano())

	member := &cluster.Member{
		ID:        memberID,
		PeerURLs:  req.PeerURLs,
		IsLearner: req.IsLearner,
	}

	if err := s.raftNode.AddMember(member); err != nil {
		return nil, fmt.Errorf("failed to propose member add: %w", err)
	}

	protoMember := &mykvstoreserverpb.Member{
		ID:        memberID,
		PeerURLs:  req.PeerURLs,
		IsLearner: req.IsLearner,
	}

	return &mykvstoreserverpb.MemberAddResponse{
		Header:  s.makeHeader(),
		Member:  protoMember,
		Members: membersToProto(s.memberStore.List()),
	}, nil
}

func (s *Server) MemberRemove(ctx context.Context, req *mykvstoreserverpb.MemberRemoveRequest) (*mykvstoreserverpb.MemberRemoveResponse, error) {
	s.logger.Info("MemberRemove request", zap.Uint64("member_id", req.ID))

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	if s.memberStore.Get(req.ID) == nil {
		return nil, fmt.Errorf("member %d not found", req.ID)
	}

	if err := s.raftNode.RemoveMember(req.ID); err != nil {
		return nil, fmt.Errorf("failed to propse member remove: %w", err)
	}

	return &mykvstoreserverpb.MemberRemoveResponse{
		Header:  s.makeHeader(),
		Members: membersToProto(s.memberStore.List()),
	}, nil
}

func (s *Server) MemberUpdate(ctx context.Context, req *mykvstoreserverpb.MemberUpdateRequest) (*mykvstoreserverpb.MemberUpdateResponse, error) {

	s.logger.Info("MemberUpdate request",
		zap.Uint64("member_id", req.ID),
		zap.Strings("peer_urls", req.PeerURLs),
	)

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	existing := s.memberStore.Get(req.ID)
	if existing == nil {
		return nil, fmt.Errorf("member %d not found", req.ID)
	}

	updated := &cluster.Member{
		ID:         req.ID,
		Name:       existing.Name,
		PeerURLs:   req.PeerURLs,
		ClientURLs: existing.ClientURLs,
		IsLearner:  existing.IsLearner,
	}

	memberData, err := json.Marshal(updated)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal member: %w", err)
	}

	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeUpdateNode,
		NodeID:  req.ID,
		Context: memberData,
	}

	if err := s.raftNode.ProposeConfChange(cc); err != nil {
		return nil, fmt.Errorf("failed to propose member update: %w", err)
	}

	return &mykvstoreserverpb.MemberUpdateResponse{
		Header:  s.makeHeader(),
		Members: membersToProto(s.memberStore.List()),
	}, nil
}

func (s *Server) MemberList(ctx context.Context, req *mykvstoreserverpb.MemberListRequest) (*mykvstoreserverpb.MemberListResponse, error) {
	return &mykvstoreserverpb.MemberListResponse{
		Header:  s.makeHeader(),
		Members: membersToProto(s.memberStore.List()),
	}, nil
}

func (s *Server) MemberPromote(ctx context.Context, req *mykvstoreserverpb.MemberPromoteRequest) (*mykvstoreserverpb.MemberPromoteResponse, error) {

	s.logger.Info("MemberPromote request", zap.Uint64("member_id", req.ID))

	if !s.raftNode.IsLeader() {
		return nil, fmt.Errorf("not leader")
	}

	member := s.memberStore.Get(req.ID)
	if member == nil {
		return nil, fmt.Errorf("member %d is not found", req.ID)
	}
	if !member.IsLearner {
		return nil, fmt.Errorf("member %d is not a leadener", req.ID)
	}

	if err := s.raftNode.PromoteMember(req.ID); err != nil {
		return nil, fmt.Errorf("failed to propose member promote: %w", err)
	}

	return &mykvstoreserverpb.MemberPromoteResponse{
		Header:  s.makeHeader(),
		Members: membersToProto(s.memberStore.List()),
	}, nil
}

func membersToProto(members []*cluster.Member) []*mykvstoreserverpb.Member {
	protoMembers := make([]*mykvstoreserverpb.Member, len(members))
	for i, m := range members {
		protoMembers[i] = &mykvstoreserverpb.Member{
			ID:         m.ID,
			Name:       m.Name,
			PeerURLs:   m.PeerURLs,
			ClientURLs: m.ClientURLs,
			IsLearner:  m.IsLearner,
		}
	}
	return protoMembers
}
