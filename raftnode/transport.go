package raftnode

import "go.etcd.io/raft/v3/raftpb"

// Transport  handles sending and receiving Raft messages
type Transport struct {
	// Peers maps node ID the address
	peers map[uint64]string
}

func NewTransport(peers map[uint64]string) *Transport {
	return &Transport{
		peers: peers,
	}
}

func (t *Transport) Send(messages []raftpb.Message) {
	for _, msg := range messages {
		// sending...
		_ = msg
	}
}

func (t *Transport) Receive(msg raftpb.Message) {
	// received message !
}
