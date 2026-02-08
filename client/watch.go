package client

import (
	"context"
	"mykvstore/mykvstoreserverpb"
)

type WatchClient struct {
	w mykvstoreserverpb.WatchClient
}

type WatchResponse struct {
	Events   []*mykvstoreserverpb.Event
	WatchID  int64
	Created  bool
	Canceled bool
	Err      error
}

func (c *WatchClient) Watch(ctx context.Context, key string) <-chan WatchResponse {
	return c.watch(ctx, []byte(key), nil, 0)
}

func (c *WatchClient) WatchFromRev(ctx context.Context, key string, rev int64) <-chan WatchResponse {
	return c.watch(ctx, []byte(key), nil, rev)
}

func (c *WatchClient) watch(ctx context.Context, key []byte, rangeEnd []byte, startRev int64) <-chan WatchResponse {
	ch := make(chan WatchResponse, 16)

	go func() {
		defer close(ch)

		stream, err := c.w.Watch(ctx)
		if err != nil {
			ch <- WatchResponse{Err: err}
			return
		}
		defer stream.CloseSend()

		err = stream.Send(&mykvstoreserverpb.WatchRequest{
			RequestUnion: &mykvstoreserverpb.WatchRequest_CreateRequest{
				CreateRequest: &mykvstoreserverpb.WatchCreateRequest{
					Key:           key,
					RangeEnd:      rangeEnd,
					StartRevision: startRev,
				},
			},
		})

		if err != nil {
			ch <- WatchResponse{Err: err}
			return
		}

		for {
			resp, err := stream.Recv()

			if err != nil {
				ch <- WatchResponse{Err: err}
				return
			}

			select {
			case ch <- WatchResponse{
				Events:   resp.Events,
				WatchID:  resp.WatchId,
				Created:  resp.Created,
				Canceled: resp.Canceled,
			}:

			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}
