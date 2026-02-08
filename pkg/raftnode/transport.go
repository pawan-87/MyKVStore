package raftnode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

type StepFunc func(cxt context.Context, msg raftpb.Message) error

type ReportUnreachable func(id uint64)

// Transport  handles sending and receiving Raft messages between peers.
type Transport struct {
	mu     sync.RWMutex
	nodeID uint64
	peers  map[uint64]string // nodeID -> http://host:port

	// stepFunc is called when a message is received from a peer.
	stepFunc StepFunc

	// reportUnreachable tells raft the peer is down
	reportUnreachable ReportUnreachable

	logger     *zap.Logger
	httpServer *http.Server
	httpClient *http.Client
	stopC      chan struct{}
	doneC      chan struct{}
}

func NewTransport(nodeID uint64, logger *zap.Logger) *Transport {
	return &Transport{
		nodeID: nodeID,
		peers:  make(map[uint64]string),
		logger: logger,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		stopC: make(chan struct{}),
		doneC: make(chan struct{}),
	}
}

// Start begins listening for incoming raft messages on the give URL
func (t *Transport) Start(listenURL string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", t.raftHandler)

	u, err := url.Parse(listenURL)
	if err != nil {
		return err
	}

	listen, err := net.Listen("tcp", u.Host)
	if err != nil {
		return fmt.Errorf("transport: listen %s: %w", listenURL, err)
	}

	t.httpServer = &http.Server{Handler: mux}

	go func() {
		defer close(t.doneC)
		if err := t.httpServer.Serve(listen); errors.Is(err, http.ErrServerClosed) {
			t.logger.Error("Transport HTTP server error", zap.Error(err))
		}
	}()

	t.logger.Info("Raft transport started (HTTP pipeline mode)",
		zap.String("listen", listenURL),
	)

	return nil
}

func (t *Transport) Stop() {
	close(t.stopC)
	if t.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		t.httpServer.Shutdown(ctx)
	}
	<-t.doneC
}

// raftHandler handles incoming raft message
func (t *Transport) raftHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	body := io.LimitReader(r.Body, 64*1024*1024)
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		return
	}

	var msg raftpb.Message
	if err := msg.Unmarshal(data); err != nil {
		http.Error(w, "error unmarshalling raft message", http.StatusBadRequest)
		return
	}

	if t.stepFunc != nil {
		if err := t.stepFunc(context.TODO(), msg); err != nil {
			http.Error(w, "error processing raft message", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *Transport) AddPeer(id uint64, rawURL string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = rawURL
	t.logger.Info("Transport: peer added", zap.Uint64("id", id), zap.String("url", rawURL))
}

func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
	t.logger.Info("Transport: peer removed", zap.Uint64("id", id))
}

// Send sends raft messages to their target peers
func (t *Transport) Send(messages []raftpb.Message) {
	for _, msg := range messages {
		if msg.To == 0 {
			continue
		}
		if msg.To == t.nodeID {
			continue
		}

		t.mu.RLock()
		peerURL, ok := t.peers[msg.To]
		t.mu.RUnlock()

		if !ok {
			t.logger.Debug("No peer URL for node", zap.Uint64("to", msg.To))
			continue
		}

		go t.pipeline(msg, peerURL)
	}
}

func (t *Transport) Receive(msg raftpb.Message) {
	// received message !
}

// pipeline send one message via HTTP POST
func (t *Transport) pipeline(msg raftpb.Message, peerURL string) {
	data, err := msg.Marshal()
	if err != nil {
		t.logger.Error("transport: marshal error", zap.Error(err))
		return
	}

	url := peerURL + "/raft"
	resp, err := t.httpClient.Post(url, "application/protobuf", bytes.NewReader(data))
	if err != nil {
		t.logger.Warn("Transport: failed to send message",
			zap.Uint64("to", msg.To),
			zap.String("url", url),
			zap.Stringer("type", msg.Type),
			zap.Error(err),
		)
		if t.reportUnreachable != nil {
			t.reportUnreachable(msg.To)
		}
		return
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		if t.reportUnreachable != nil {
			t.reportUnreachable(msg.To)
		}
	}
}
