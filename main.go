package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pawan-87/MyKVStore/pkg/mvcc"
	"github.com/pawan-87/MyKVStore/pkg/raftnode"
	"github.com/pawan-87/MyKVStore/pkg/server"
	"github.com/pawan-87/MyKVStore/pkg/storage"

	"go.etcd.io/raft/v3"
	"go.uber.org/zap"
)

func main() {
	var (
		id             = flag.Uint64("id", 1, "Node ID (must be unique in the cluster)")
		listenClient   = flag.String("listen-client", "127.0.0.1:2379", "Client gRPC listen address")
		listenPeer     = flag.String("listen-peer", "http://127.0.0.1:12380", "Peer HTTP listen URL (for Raft transport)")
		initialCluster = flag.String("initial-cluster", "1=http://127.0.0.1:12380", "Initial cluster:\"1=http://host1:port,2=http://host2:port,....\"")
		dataDir        = flag.String("data-dir", "", "Data directory for WAL, snapshots, BoltDB (default:/tmp/mykvstore/node{id})")
		join           = flag.Bool("join", false, "Join an existing cluster (set for nodes added via MemberAdd)")
	)
	flag.Parse()

	if *dataDir == "" {
		*dataDir = fmt.Sprintf("/tmp/mykvstore/node%d", *id)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting MyKVStore",
		zap.Uint64("id", *id),
		zap.String("client-addr", *listenClient),
		zap.String("peer-url", *listenPeer),
		zap.String("data-dir", *dataDir),
		zap.Bool("join", *join),
	)

	peers, peerURLs := parseCluster(*initialCluster)

	logger.Info("Cluster configuration",
		zap.Int("peers", len(peers)),
		zap.Any("peer_urls", peerURLs),
	)

	dbPath := fmt.Sprintf("%s/db", *dataDir)
	backend := storage.NewBoltBackend(dbPath)
	if err := backend.Open(); err != nil {
		logger.Fatal("Failed to open BoltDB", zap.Error(err))
	}
	defer backend.Close()

	logger.Info("BoltDB opened", zap.String("path", dbPath))

	// In-memory index + BoltDB
	mvccStore := mvcc.NewStore(backend)
	logger.Info("In-memory index MVCC store initialized")

	// Raft library requires a MemoryStorage for entries/hardstate
	memStorage := raft.NewMemoryStorage()
	logger.Info("raft NewMemoryStorage  initialized")

	raftNode := raftnode.NewNode(&raftnode.Config{
		ID:      *id,
		Peers:   peers,
		Join:    *join,
		Storage: memStorage,
		Store:   mvccStore,

		PeerURLs:  peerURLs,
		ListenURL: *listenPeer,
		DataDir:   *dataDir,
		Logger:    logger,

		ElectionTick:  10,
		HeartbeatTick: 1,
	})

	mykvstoreServer := server.NewConfig(&server.Config{
		RaftNode:   raftNode,
		MVCCStore:  mvccStore,
		Backend:    backend,
		Logger:     logger,
		ListenAddr: *listenPeer,
		ClusterID:  1,
		MemberID:   *id,
		PeerURL:    *listenPeer,
	})

	raftNode.Start()
	logger.Info("Raft node started")

	if err := mykvstoreServer.Start(*listenClient); err != nil {
		logger.Fatal("Failed to start gRPC server", zap.Error(err))
	}
	logger.Info("gRPC server started", zap.String("addr", *listenClient))

	fmt.Printf("\n✅ MyKVStore node %d is running\n", *id)
	fmt.Printf("   Client:  %s\n", *listenClient)
	fmt.Printf("   Peer:    %s\n", *listenPeer)
	fmt.Printf("   DataDir: %s\n\n", *dataDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	_ = <-sigCh

	logger.Info("Received signal, shutting down...")

	mykvstoreServer.Stop()
	raftNode.Stop()

	logger.Info("MyKVStore stopped")
}

func parseCluster(cluster string) ([]raft.Peer, map[uint64]string) {
	var peers []raft.Peer
	peerURLs := make(map[uint64]string)

	for _, part := range strings.Split(cluster, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			log.Fatalf("Invalid cluster entry (expected id=url): %q", part)
		}

		idStr := part[:eqIdx]
		url := part[eqIdx+1:]

		var nodeID uint64
		if _, err := fmt.Sscanf(idStr, "%d", &nodeID); err != nil {
			log.Fatalf("Invalid nodeID %q: %v", idStr, err)
		}

		peers = append(peers, raft.Peer{ID: nodeID})
		peerURLs[nodeID] = url
	}

	return peers, peerURLs
}
