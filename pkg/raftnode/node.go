package raftnode

import (
	"context"
	"fmt"
	"log"
	mvcc2 "mykvstore/pkg/mvcc"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// ApplyCallback is called when a command is applied
type ApplyCallback func(reqID uint64, result interface{}, err error)

type WatchCallback func(key []byte, eventType int, kv, prevKV interface{})

type Node struct {
	id    uint64
	node  raft.Node
	store *mvcc2.Store

	// Storage
	storage *raft.MemoryStorage

	// Proposal channels
	proposeC    chan []byte
	confChangeC chan raftpb.ConfChange

	// Commit channel
	commitC chan *commit

	// Error channel
	errorC chan error

	// Stop signal
	stopC chan struct{}

	applyCallback ApplyCallback

	watchCallback WatchCallback
}

type commit struct {
	data       [][]byte
	applyDoneC chan struct{}
}

type Config struct {
	ID      uint64
	Peers   []raft.Peer
	Storage *raft.MemoryStorage
	Store   *mvcc2.Store

	// Raft configuration
	ElectionTick  int
	HeartbeatTick int
}

func NewNode(cfg *Config) *Node {
	node := &Node{
		id:          cfg.ID,
		store:       cfg.Store,
		storage:     cfg.Storage,
		proposeC:    make(chan []byte),
		confChangeC: make(chan raftpb.ConfChange),
		commitC:     make(chan *commit),
		errorC:      make(chan error),
		stopC:       make(chan struct{}),
	}

	config := &raft.Config{
		ID:            cfg.ID,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		Storage:       cfg.Storage,
		MaxSizePerMsg: 1024 * 1024,
		CheckQuorum:   true,
		PreVote:       true,
	}

	if len(cfg.Peers) > 0 {
		node.node = raft.StartNode(config, cfg.Peers)
	} else {
		node.node = raft.RestartNode(config)
	}

	return node
}

func (n *Node) Start() {
	go n.serverChannels()
	go n.run()
}

func (n *Node) Stop() {
	close(n.stopC)
}

func (n *Node) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.node.Tick()

		case rd := <-n.node.Ready():
			// Process Ready
			n.processReady(rd)

		case <-n.stopC:
			n.node.Stop()
			return
		}
	}
}

func (n *Node) SetApplyCallback(callback ApplyCallback) {
	n.applyCallback = callback
}

func (n *Node) SetWatchCallback(callback WatchCallback) {
	n.watchCallback = callback
}

// serverChannels serves proposal and commit channels
func (n *Node) serverChannels() {
	for {
		select {
		case data := <-n.proposeC:
			// Propose to Raft
			n.node.Propose(context.TODO(), data)

		case cc := <-n.confChangeC:
			// Propose configuration change
			n.node.ProposeConfChange(context.TODO(), cc)

		case commit := <-n.commitC:
			// Apply commited entries
			for _, data := range commit.data {
				n.applyEntry(data)
			}
			close(commit.applyDoneC)

		case <-n.stopC:
			return
		}
	}
}

// processReady processes a Ready from Raft
func (n *Node) processReady(rd raft.Ready) {
	// 1. Save hard state and entries to stable storage
	if !raft.IsEmptyHardState(rd.HardState) {
		n.storage.SetHardState(rd.HardState)
	}

	if len(rd.Entries) > 0 {
		n.storage.Append(rd.Entries)
	}

	// 2. Send messages to peers

	// 3. Apply committed entries to state machine
	if len(rd.CommittedEntries) > 0 {
		n.publishEntries(rd.CommittedEntries)
	}

	// 4. Adance the Raft node
	n.node.Advance()
}

// publishEntries publishes committed entries to be applied
func (n *Node) publishEntries(entries []raftpb.Entry) {
	data := make([][]byte, 0, len(entries))

	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryNormal:
			if len(entry.Data) == 0 {
				// Empty entry (initial entry)
				break
			}
			data = append(data, entry.Data)

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(entry.Data)
			n.node.ApplyConfChange(cc)
		}
	}

	if len(data) > 0 {
		applyDoneC := make(chan struct{})
		select {
		case n.commitC <- &commit{data, applyDoneC}:
			<-applyDoneC

		case <-n.stopC:
			return
		}
	}
}

func (n *Node) applyEntry(data []byte) {
	// Decode command
	cmd, err := DecodeCommand(data)
	if err != nil {
		log.Printf("Failed to decode command: %v", err)
		if n.applyCallback != nil {
			n.applyCallback(0, nil, err)
		}
		return
	}

	var result interface{}
	var applyErr error
	var eventType int
	var prevKV *mvcc2.KeyValue

	switch cmd.Type {
	case CommandPut:
		// Get previous value for watch
		if n.watchCallback != nil {
			prevKV, _ = n.store.Get(cmd.Key, 0)
		}

		rev, err := n.store.Put(cmd.Key, cmd.Value, cmd.Lease)
		result = rev
		applyErr = err
		eventType = 0

		if err != nil {
			log.Printf("Failed to apply Put: %v", err)
		}

		// Notify watchers
		if n.watchCallback != nil && err == nil {
			kv, _ := n.store.Get(cmd.Key, 0)
			n.watchCallback(cmd.Key, eventType, kv, prevKV)
		}
	case CommandDelete:
		// Get previous value for watch
		if n.watchCallback != nil {
			prevKV, _ = n.store.Get(cmd.Key, 0)
		}

		rev, err := n.store.Delete(cmd.Key)
		result = rev
		applyErr = err
		eventType = 1

		if err != nil {
			log.Printf("Failed to apply Delete: %v", err)
		}

		// Notify watchers
		if n.watchCallback != nil && err == nil {
			kv, _ := n.store.Get(cmd.Key, 0)
			n.watchCallback(cmd.Key, eventType, kv, prevKV)
		}
	case CommandCompact:
		err := n.store.Compact(cmd.Revision)

		applyErr = err

		if err != nil {
			log.Printf("Failed to apply Compact: %v", err)
		}

	case CommandLeaseGrant:
		result = cmd.LeaseID

	case CommandLeaseRevoke:
		result = nil
	}

	if n.applyCallback != nil && cmd.ReqID > 0 {
		n.applyCallback(cmd.ReqID, result, applyErr)
	}
}

func (n *Node) IsLeader() bool {
	return n.node.Status().Lead == n.id
}

func (n *Node) LeaderID() uint64 {
	return n.node.Status().Lead
}

// Propose proposes a command through Raft
func (n *Node) Propose(cmd *Command) error {
	data, err := cmd.Encode()
	if err != nil {
		return err
	}

	select {
	case n.proposeC <- data:
		return nil

	case <-n.stopC:
		return fmt.Errorf("node stopped")
	}
}

// ProposeConfChange proposes a configuration change
func (n *Node) ProposeConfChange(cc raftpb.ConfChange) error {
	select {
	case n.confChangeC <- cc:
		return nil

	case <-n.stopC:
		return fmt.Errorf("node stopped")
	}
}
