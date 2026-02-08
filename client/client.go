package client

import (
	"context"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn

	KV      *KVClient
	Watch   *WatchClient
	Lease   *LeaseClient
	Cluster *ClusterClient
}

func New(endpoint string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s: %w", endpoint, err)
	}

	c := &Client{conn: conn}
	c.KV = &KVClient{kv: mykvstoreserverpb.NewKVClient(conn)}
	c.Watch = &WatchClient{w: mykvstoreserverpb.NewWatchClient(conn)}
	c.Lease = &LeaseClient{l: mykvstoreserverpb.NewLeaseClient(conn)}
	c.Cluster = &ClusterClient{c: mykvstoreserverpb.NewClusterClient(conn)}

	return c, nil
}
