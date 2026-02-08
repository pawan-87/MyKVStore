package client

import (
	"context"
	"github.com/pawan-87/MyKVStore/mykvstoreserverpb"
	"time"
)

type LeaseClient struct {
	l mykvstoreserverpb.LeaseClient
}

func (c *LeaseClient) Grant(ctx context.Context, ttl int64) (*mykvstoreserverpb.LeaseGrantResponse, error) {
	return c.l.LeaseGrant(ctx, &mykvstoreserverpb.LeaseGrantRequest{TTL: ttl})
}

func (c *LeaseClient) Revoke(ctx context.Context, id int64) (*mykvstoreserverpb.LeaseRevokeResponse, error) {
	return c.l.LeaseRevoke(ctx, &mykvstoreserverpb.LeaseRevokeRequest{ID: id})
}

func (c *LeaseClient) TimeToLive(ctx context.Context, id int64, withKeys bool) (*mykvstoreserverpb.LeaseTimeToLiveResponse, error) {
	return c.l.LeaseTimeToLive(ctx, &mykvstoreserverpb.LeaseTimeToLiveRequest{ID: id, Keys: withKeys})
}

func (c *LeaseClient) KeepAliveOnce(ctx context.Context, id int64) (*mykvstoreserverpb.LeaseKeepAliveResponse, error) {
	stream, err := c.l.LeaseKeepAlive(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()

	if err := stream.Send(&mykvstoreserverpb.LeaseKeepAliveRequest{ID: id}); err != nil {
		return nil, err
	}

	return stream.Recv()
}

func (c *LeaseClient) KeepAlive(ctx context.Context, id int64) (<-chan *mykvstoreserverpb.LeaseKeepAliveResponse, error) {
	ch := make(chan *mykvstoreserverpb.LeaseKeepAliveResponse, 4)

	go func() {
		defer close(ch)

		stream, err := c.l.LeaseKeepAlive(ctx)
		if err != nil {
			return
		}
		defer stream.CloseSend()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := stream.Send(&mykvstoreserverpb.LeaseKeepAliveRequest{ID: id}); err != nil {
					return
				}
				resp, err := stream.Recv()
				if err != nil {
					return
				}
				select {
				case ch <- resp:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return

			}
		}
	}()

	return ch, nil
}
