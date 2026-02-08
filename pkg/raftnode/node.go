package raftnode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/cluster"
	"mykvstore/pkg/lease"
	"mykvstore/pkg/mvcc"
	"mykvstore/pkg/snapshot"
	"mykvstore/pkg/wal"
	"path/filepath"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Trigger snapshot every 1000 entries
const snapshotInterval uint64 = 10000

// ApplyCallback is called when a command is applied
type ApplyCallback func(reqID uint64, result interface{}, err error)

type WatchCallback func(key []byte, eventType int, kv, prevKV interface{})

type ConfChangeCallback func(cc raftpb.ConfChange, confState *raftpb.ConfState)

type Node struct {
	id   uint64
	node raft.Node

	// Storage
	store         *mvcc.Store
	storage       *raft.MemoryStorage
	wal           *wal.WAL
	snapshotter   *snapshot.Snapshotter
	appliedIndex  uint64
	snapshotIndex uint64

	// Lessor
	lessor *lease.Lessor

	// Proposal channels
	proposeC    chan []byte
	confChangeC chan raftpb.ConfChange

	// Cluster membership
	memberStore        *cluster.MemberStore
	confChangeCallback ConfChangeCallback

	// Transport
	transport *Transport
	peerURLs  map[uint64]string
	listenURL string

	// Commit channel
	commitC chan *commit

	// Error channel
	errorC chan error

	// Stop signal
	stopC chan struct{}

	applyCallback ApplyCallback
	watchCallback WatchCallback

	logger *zap.Logger

	server ServerInterface
}

type ServerInterface interface {
	EvaluateComparisons(req *mykvstoreserverpb.TxnRequest) (bool, error)
	ExecuteOp(ctx context.Context, op *mykvstoreserverpb.RequestOp) (*mykvstoreserverpb.ResponseOp, error)
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

	PeerURLs  map[uint64]string
	ListenURL string
	DataDir   string
	Logger    *zap.Logger

	// Raft configuration
	ElectionTick  int
	HeartbeatTick int
}

func NewNode(cfg *Config) *Node {
	logger := cfg.Logger
	if logger == nil {
		logger, _ = zap.NewProduction()
	}

	node := &Node{
		id:          cfg.ID,
		store:       cfg.Store,
		storage:     cfg.Storage,
		peerURLs:    cfg.PeerURLs,
		listenURL:   cfg.ListenURL,
		proposeC:    make(chan []byte),
		confChangeC: make(chan raftpb.ConfChange),
		commitC:     make(chan *commit),
		errorC:      make(chan error),
		stopC:       make(chan struct{}),
		logger:      logger,
	}

	initializeSnapAndWAL(cfg, logger, node)

	config := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         cfg.Storage,
		MaxSizePerMsg:   1024 * 1024,
		CheckQuorum:     true,
		PreVote:         true,
		MaxInflightMsgs: 256, // How many raft replication messages can be "in flight" send but not yet acknowledge
	}

	hasExistingState := false
	if cfg.DataDir != "" {
		hs, _, _ := cfg.Storage.InitialState()
		hasExistingState = hs.Term > 0 || hs.Commit > 0
	}

	if hasExistingState {
		node.node = raft.RestartNode(config)
		logger.Info("Restarted raft node from WAL", zap.Uint64("id", cfg.ID))
	} else if len(cfg.Peers) > 0 {
		node.node = raft.StartNode(config, cfg.Peers)
		logger.Info("Started new raft node",
			zap.Uint64("id", cfg.ID),
			zap.Int("peers", len(cfg.Peers)),
		)
	} else {
		node.node = raft.RestartNode(config)
		logger.Info("Restarted raft node (no peers)", zap.Uint64("id", cfg.ID))
	}

	return node
}

func initializeSnapAndWAL(cfg *Config, logger *zap.Logger, node *Node) {
	if cfg.DataDir != "" {
		walDir := filepath.Join(cfg.DataDir, "wal")
		snapDir := filepath.Join(cfg.DataDir, "snap")

		// Load existing snapshot if any
		ss, err := snapshot.NewSnapshotter(snapDir)
		if err != nil {
			logger.Fatal("Failed to create snapshotter", zap.Error(err))
		}
		node.snapshotter = ss

		existingSnap, err := ss.Load()
		if err != nil {
			logger.Fatal("Failed to load snapshot", zap.Error(err))
		}
		if existingSnap != nil {
			cfg.Storage.ApplySnapshot(*existingSnap)
			node.snapshotIndex = existingSnap.Metadata.Index
			node.appliedIndex = existingSnap.Metadata.Index
			logger.Info("Restored snapshot",
				zap.Uint64("index", existingSnap.Metadata.Index),
				zap.Uint64("term", existingSnap.Metadata.Term),
			)
		}

		// Open or create WAL
		if wal.Exist(walDir) {
			w, err := wal.Open(walDir)
			if err != nil {
				logger.Fatal("Failed to open WAL", zap.Error(err))
			}

			hs, entries, err := w.ReadAll()
			if err != nil {
				logger.Fatal("Failed to replay WAL", zap.Error(err))
			}

			if hs.Term > 0 || hs.Vote > 0 || hs.Commit > 0 {
				cfg.Storage.SetHardState(hs) // put hard state into raft MemoryStorage
			}

			if len(entries) > 0 {
				cfg.Storage.Append(entries) // add all entries into raft MemoryStorage
				node.appliedIndex = entries[len(entries)-1].Index
			}

			node.wal = w
			logger.Info("WAL replayed",
				zap.Uint64("term", hs.Term),
				zap.Int("entries", len(entries)),
			)
		} else {
			w, err := wal.Create(walDir)
			if err != nil {
				logger.Fatal("Failed to create WAL", zap.Error(err))
			}
			node.wal = w
			logger.Info("Created new WAL")
		}
	}
}

func (n *Node) Start() {
	if len(n.peerURLs) > 0 && n.listenURL != "" {
		n.startTransport()
	}

	go n.serverChannels()
	go n.run()
}

func (n *Node) startTransport() {
	n.transport = NewTransport(n.id, n.logger)

	n.transport.stepFunc = func(ctx context.Context, msg raftpb.Message) error {
		return n.node.Step(ctx, msg)
	}
	n.transport.reportUnreachable = func(id uint64) {
		n.node.ReportUnreachable(id)
	}

	// Register all peers
	for id, peerURL := range n.peerURLs {
		if id != n.id {
			n.transport.AddPeer(id, peerURL)
		}
	}

	// Start HTTP listener
	if err := n.transport.Start(n.listenURL); err != nil {
		n.logger.Fatal("Failed to start transport", zap.Error(err))
	}
}

func (n *Node) Stop() {
	close(n.stopC)

	if n.transport != nil {
		n.transport.Stop()
	}

	if n.wal != nil {
		n.wal.Close()
	}
}

func (n *Node) SetServer(server ServerInterface) {
	n.server = server
}

func (n *Node) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.node.Tick()

		case rd := <-n.node.Ready(): // current point-in-time state (has nay work to do?)
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

func (n *Node) SetLessor(lessor *lease.Lessor) {
	n.lessor = lessor
}

func (n *Node) SetMemberStore(ms *cluster.MemberStore) {
	n.memberStore = ms
}

func (n *Node) SetConfChangeCallback(callback ConfChangeCallback) {
	n.confChangeCallback = callback
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

	// Save snapshot to disk (if raft produced one)
	if !raft.IsEmptySnap(rd.Snapshot) {
		if n.snapshotter != nil {
			if err := n.snapshotter.SaveSnap(rd.Snapshot); err != nil {
				n.logger.Fatal("Failed to save snapshot: %v", zap.Error(err))
			}
		}
		if n.wal != nil {
			n.wal.SaveSnapshot(
				rd.Snapshot.Metadata.Index,
				rd.Snapshot.Metadata.Term,
			)
		}
	}

	// Persist to WAL first
	if n.wal != nil {
		if err := n.wal.Save(rd.HardState, rd.Entries); err != nil {
			n.logger.Fatal("Failed to save to WAL: %v", zap.Error(err))
		}
	}

	// Save to raft MemoryStorage
	if !raft.IsEmptyHardState(rd.HardState) {
		n.storage.SetHardState(rd.HardState)
	}
	if len(rd.Entries) > 0 {
		n.storage.Append(rd.Entries)
	}

	// Send messages to peers
	if n.transport != nil && len(rd.Messages) > 0 {
		n.transport.Send(rd.Messages)
	}

	// Apply snapshot to state machine (if received from leader)
	if !raft.IsEmptySnap(rd.Snapshot) {
		n.storage.ApplySnapshot(rd.Snapshot)
		// TODO: restore application state from rd.Snapshot.Data
		n.appliedIndex = rd.Snapshot.Metadata.Index
		n.snapshotIndex = rd.Snapshot.Metadata.Index
	}

	// (rd.CommittedEntries commited entries) Apply committed entries to state machine
	if len(rd.CommittedEntries) > 0 {
		n.publishEntries(rd.CommittedEntries)
	}

	// Adance the Raft node
	n.node.Advance()
}

// publishEntries publishes committed entries to be applied (Got the Majority Quorum now apply to sate machine)
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

			// Apply to Raft's internal config
			confState := n.node.ApplyConfChange(cc)

			n.applyConfChangeToMemberStore(cc)

			n.updateTransportPeers(cc)

			if n.confChangeCallback != nil {
				n.confChangeCallback(cc, confState)
			}
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

	if len(entries) > 0 {
		n.appliedIndex = entries[len(entries)-1].Index
		n.maybeTriggerSnapshot()
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
	var prevKV *mvcc.KeyValue

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
		result, applyErr = n.applyLeaseGrant(cmd)

	case CommandLeaseRevoke:
		result, applyErr = n.applyLeaseRevoke(cmd)

	case CommandLeaseKeepAlive:
		result, applyErr = n.applyLeaseKeepAlive(cmd)

	case CommandTxn:
		result, applyErr = n.applyTxn(cmd)
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

func (n *Node) applyLeaseGrant(cmd *Command) (interface{}, error) {
	return n.lessor.ApplyGrant(lease.LeaseID(cmd.LeaseID), cmd.LeaseTTL)
}

func (n *Node) applyLeaseRevoke(cmd *Command) (interface{}, error) {
	return n.lessor.ApplyRevoke(lease.LeaseID(cmd.LeaseID))
}

func (n *Node) applyLeaseKeepAlive(cmd *Command) (int64, error) {
	return n.lessor.ApplyKeepAlive(lease.LeaseID(cmd.LeaseID))
}

func (n *Node) applyTxn(cmd *Command) (interface{}, error) {
	if n.server == nil {
		return nil, fmt.Errorf("server not set")
	}

	var txnReq mykvstoreserverpb.TxnRequest
	if err := proto.Unmarshal(cmd.TxnRequest, &txnReq); err != nil {
		return nil, fmt.Errorf("failed to unmarshal txn: %w", err)
	}

	succeeded, err := n.server.EvaluateComparisons(&txnReq)
	if err != nil {
		return nil, err
	}

	var ops []*mykvstoreserverpb.RequestOp
	if succeeded {
		ops = txnReq.Success
	} else {
		ops = txnReq.Failure
	}

	response := make([]*mykvstoreserverpb.ResponseOp, 0, len(ops))
	for _, op := range ops {
		resp, err := n.server.ExecuteOp(context.Background(), op)
		if err != nil {
			return nil, err
		}
		response = append(response, resp)
	}

	return &mykvstoreserverpb.TxnResponse{
		Succeeded: succeeded,
		Responses: response,
	}, nil
}

// applyConfChangeToMemberStore persists the membership change to bbolt
func (n *Node) applyConfChangeToMemberStore(cc raftpb.ConfChange) {
	if n.memberStore == nil {
		return
	}

	switch cc.Type {

	case raftpb.ConfChangeAddNode:
		if existing := n.memberStore.Get(cc.NodeID); existing != nil && existing.IsLearner {
			if err := n.memberStore.Promote(cc.NodeID); err != nil {
				log.Printf("Failed to promote member %d: %v", cc.NodeID, err)
			}
			return
		}

		n.addMemberFromContext(cc)

	case raftpb.ConfChangeAddLearnerNode:
		n.addMemberFromContext(cc)

	case raftpb.ConfChangeRemoveNode:
		if err := n.memberStore.Remove(cc.NodeID); err != nil {
			log.Printf("Failed to remove member %d: %v", cc.NodeID, err)
		}

	case raftpb.ConfChangeUpdateNode:
		if len(cc.Context) > 0 {
			var m cluster.Member
			if err := json.Unmarshal(cc.Context, &m); err == nil {
				n.memberStore.Update(m.ID, m.PeerURLs, m.ClientURLs)
			}
		}
	}
}

func (n *Node) addMemberFromContext(cc raftpb.ConfChange) {
	if len(cc.Context) == 0 {
		log.Printf("ConfChange for node %d hash empty context", cc.NodeID)
		return
	}

	var m cluster.Member
	if err := json.Unmarshal(cc.Context, &m); err != nil {
		log.Printf("Failed to unmarshal member from ConfChange context: %v", err)
		return
	}

	m.ID = cc.NodeID

	if cc.Type == raftpb.ConfChangeAddLearnerNode {
		m.IsLearner = true
	}

	if err := n.memberStore.Add(&m); err != nil {
		log.Printf("Failed to add member %d to store: %v", cc.NodeID, err)
	}
}

// AddMember proposes adding a new member
func (n *Node) AddMember(member *cluster.Member) error {
	memberData, err := json.Marshal(member)
	if err != nil {
		return fmt.Errorf("failed to marshal member: %w", err)
	}

	ccType := raftpb.ConfChangeAddLearnerNode
	if !member.IsLearner {
		ccType = raftpb.ConfChangeAddNode
	}

	cc := raftpb.ConfChange{
		Type:    ccType,
		NodeID:  member.ID,
		Context: memberData,
	}

	return n.ProposeConfChange(cc)
}

// PromoteMember proposes promoting a learner to a voting member
func (n *Node) PromoteMember(memberID uint64) error {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: memberID,
	}
	return n.ProposeConfChange(cc)
}

func (n *Node) RemoveMember(memberID uint64) error {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: memberID,
	}
	return n.ProposeConfChange(cc)
}

// TransferLeadership transfer leadership to another node
func (n *Node) TransferLeadership(ctx context.Context, targetID uint64) error {
	if n.node.Status().Lead != n.id {
		return fmt.Errorf("not the leader")
	}

	log.Printf("Transferring leadership from %d to %d", n.id, targetID)

	n.node.TransferLeadership(ctx, n.id, targetID)

	return nil
}

func (n *Node) GetLeader() uint64 {
	return n.node.Status().Lead
}

func (n *Node) GetStatus() raft.Status {
	return n.node.Status()
}

func (n *Node) updateTransportPeers(cc raftpb.ConfChange) {
	if n.transport == nil {
		return
	}

	switch cc.Type {

	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		if cc.NodeID != n.id {
			peerURL, ok := n.peerURLs[cc.NodeID]

			if !ok {
				member := n.memberStore.Get(cc.NodeID)
				if member != nil && len(member.PeerURLs) > 0 {
					peerURL = member.PeerURLs[0]
					ok = true
				}
			}

			if ok {
				n.transport.AddPeer(cc.NodeID, peerURL)
			}
		}

	case raftpb.ConfChangeRemoveNode:
		if cc.NodeID != n.id {
			n.transport.RemovePeer(cc.NodeID)
		}
	}
}

func (n *Node) maybeTriggerSnapshot() {
	if n.appliedIndex-n.snapshotIndex < snapshotInterval {
		return
	}

	n.logger.Info("Triggering snapshot",
		zap.Uint64("applied", n.appliedIndex),
		zap.Uint64("last_snap", n.snapshotIndex),
	)

	snap, err := n.storage.CreateSnapshot(n.appliedIndex, nil, nil)
	if err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			return
		}
		n.logger.Error("Failed to create snapshot", zap.Error(err))
	}

	if n.snapshotter != nil {
		if err := n.snapshotter.SaveSnap(snap); err != nil {
			n.logger.Error("Failed to save snapshot", zap.Error(err))
			return
		}
		n.snapshotter.Purge(3)
	}

	if n.wal != nil {
		n.wal.SaveSnapshot(snap.Metadata.Index, snap.Metadata.Term)
	}

	compactIdx := uint64(1)
	if n.appliedIndex > snapshotInterval {
		compactIdx = n.appliedIndex - snapshotInterval
	}
	if err := n.storage.Compact(compactIdx); err != nil && errors.Is(err, raft.ErrCompacted) {
		n.logger.Error("Failed to compact", zap.Error(err))
	}

	n.snapshotIndex = n.appliedIndex
}
