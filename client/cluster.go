package client

import (
	"context"

	"github.com/pawan-87/MyKVStore/mykvstoreserverpb"
)

type ClusterClient struct {
	c mykvstoreserverpb.ClusterClient
}

func (cl *ClusterClient) MemberAdd(ctx context.Context, peerURLS []string, isLeader bool) (*mykvstoreserverpb.MemberAddResponse, error) {
	return cl.c.MemberAdd(ctx, &mykvstoreserverpb.MemberAddRequest{PeerURLs: peerURLS, IsLearner: isLeader})
}

func (cl *ClusterClient) MemberRemove(ctx context.Context, id uint64) (*mykvstoreserverpb.MemberRemoveResponse, error) {
	return cl.c.MemberRemove(ctx, &mykvstoreserverpb.MemberRemoveRequest{ID: id})
}

func (cl *ClusterClient) MemberUpdate(ctx context.Context, id uint64, peerURLs []string) (*mykvstoreserverpb.MemberUpdateResponse, error) {
	return cl.c.MemberUpdate(ctx, &mykvstoreserverpb.MemberUpdateRequest{ID: id, PeerURLs: peerURLs})
}

func (cl *ClusterClient) MemberList(ctx context.Context) (*mykvstoreserverpb.MemberListResponse, error) {
	return cl.c.MemberList(ctx, &mykvstoreserverpb.MemberListRequest{})
}

func (cl *ClusterClient) MemberPromote(ctx context.Context, id uint64) (*mykvstoreserverpb.MemberPromoteResponse, error) {
	return cl.c.MemberPromote(ctx, &mykvstoreserverpb.MemberPromoteRequest{ID: id})
}
