package client

import (
	"context"
	"mykvstore/mykvstoreserverpb"
)

type KVClient struct {
	kv mykvstoreserverpb.KVClient
}

func (c *KVClient) Put(ctx context.Context, key, val string) (*mykvstoreserverpb.PutResponse, error) {
	return c.kv.Put(ctx, &mykvstoreserverpb.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

func (c *KVClient) PutWithLease(ctx context.Context, key, val string, leaseID int64) (*mykvstoreserverpb.PutResponse, error) {
	return c.kv.Put(ctx, &mykvstoreserverpb.PutRequest{
		Key:   []byte(key),
		Value: []byte(val),
		Lease: leaseID,
	})
}

func (c *KVClient) PutWithPrevKv(ctx context.Context, key, val string, leaseID int64) (*mykvstoreserverpb.PutResponse, error) {
	return c.kv.Put(ctx, &mykvstoreserverpb.PutRequest{
		Key:    []byte(key),
		Value:  []byte(val),
		PrevKv: true,
	})
}

func (c *KVClient) Get(ctx context.Context, key string) (*mykvstoreserverpb.RangeResponse, error) {
	return c.kv.Range(ctx, &mykvstoreserverpb.RangeRequest{
		Key: []byte(key),
	})
}

func (c *KVClient) GetRange(ctx context.Context, key, end string) (*mykvstoreserverpb.RangeResponse, error) {
	return c.kv.Range(ctx, &mykvstoreserverpb.RangeRequest{
		Key:      []byte(key),
		RangeEnd: []byte(end),
	})
}

func (c *KVClient) GetWithRev(ctx context.Context, key string, rev int64) (*mykvstoreserverpb.RangeResponse, error) {
	return c.kv.Range(ctx, &mykvstoreserverpb.RangeRequest{
		Key:      []byte(key),
		Revision: rev,
	})
}

func (c *KVClient) Delete(ctx context.Context, key string) (*mykvstoreserverpb.DeleteRangeResponse, error) {
	return c.kv.DeleteRange(ctx, &mykvstoreserverpb.DeleteRangeRequest{
		Key: []byte(key),
	})
}

func (c *KVClient) Compact(ctx context.Context, rev int64) (*mykvstoreserverpb.CompactionResponse, error) {
	return c.kv.Compact(ctx, &mykvstoreserverpb.CompactionRequest{
		Revision: rev,
	})
}

func (c *KVClient) Txn(ctx context.Context, cmps []*mykvstoreserverpb.Compare, thenOps []*mykvstoreserverpb.RequestOp, elseOps []*mykvstoreserverpb.RequestOp) (*mykvstoreserverpb.TxnResponse, error) {
	return c.kv.Txn(ctx, &mykvstoreserverpb.TxnRequest{
		Compare: cmps,
		Success: thenOps,
		Failure: elseOps,
	})
}
