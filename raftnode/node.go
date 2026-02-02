package raftnode

import (
	"context"
	"fmt"
	"log"
	"mykvstore/mvcc"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

type Node struct {
	id    uint64
	node  raft.Node
	store *mvcc.Store

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
}

type commit struct {
	data       [][]byte
	applyDoneC chan struct{}
}

type Config struct {
	ID      uint64
	Peers   []raft.Peer
	Storage *raft.MemoryStorage
	Store   *mvcc.Store

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
		return
	}

	switch cmd.Type {
	case CommandPut:
		_, err := n.store.Put(cmd.Key, cmd.Value, cmd.Lease)
		if err != nil {
			log.Printf("Failed to apply Put: %v", err)
		}

	case CommandDelete:
		_, err := n.store.Delete(cmd.Key)
		if err != nil {
			log.Printf("Failed to apply Delete: %v", err)

		}

	case CommandCompact:
		err := n.store.Compact(cmd.Revision)
		if err != nil {
			log.Printf("Failed to apply Compact: %v", err)
		}
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
