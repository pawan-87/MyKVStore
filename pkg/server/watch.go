package server

import (
	"io"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/watch"
	"sync"

	"go.uber.org/zap"
)

func (s *Server) Watch(stream mykvstoreserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	// map of active watches for this stream
	watches := make(map[watch.WatchID]*watch.Watch)
	var mu sync.Mutex

	// Channel for sending responses
	sendChan := make(chan *mykvstoreserverpb.WatchResponse, 10)

	// Goroutine to send responses to client
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for resp := range sendChan {
			if err := stream.Send(resp); err != nil {
				s.logger.Error("Failed to send watch response", zap.Error(err))
				return
			}
		}
	}()

	// Goroutine to receive requests from client
	recvDone := make(chan struct{})
	go func() {
		defer close(recvDone)
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				s.logger.Error("Failed to receive watch request", zap.Error(err))
				return
			}

			s.handleWatchRequest(req, watches, &mu, sendChan)
		}
	}()

	// Wait for completion or cancellation
	select {
	case <-recvDone:
		// Client closed stream or error
	case <-sendDone:
		// Send error
	case <-ctx.Done():
		// Context cancelled
	}

	mu.Lock()
	for id := range watches {
		s.watchManager.CancelWatch(id)
	}
	mu.Unlock()

	close(sendChan)

	return nil
}

func (s *Server) handleWatchRequest(
	req *mykvstoreserverpb.WatchRequest,
	watches map[watch.WatchID]*watch.Watch,
	mu *sync.Mutex,
	sendChan chan *mykvstoreserverpb.WatchResponse,
) {
	switch r := req.RequestUnion.(type) {

	case *mykvstoreserverpb.WatchRequest_CreateRequest:
		s.handleWatchCreate(r.CreateRequest, watches, mu, sendChan)

	case *mykvstoreserverpb.WatchRequest_CancelRequest:
		s.handleWatchCancel(r.CancelRequest, watches, mu, sendChan)

	case *mykvstoreserverpb.WatchRequest_ProgressRequest:
		s.handleWatchProgress(watches, mu, sendChan)
	}
}

func (s *Server) handleWatchCreate(req *mykvstoreserverpb.WatchCreateRequest, watches map[watch.WatchID]*watch.Watch, mu *sync.Mutex, sendChan chan *mykvstoreserverpb.WatchResponse) {
	s.logger.Debug("Creating watch",
		zap.ByteString("key", req.Key),
		zap.ByteString("range_end", req.RangeEnd),
		zap.Int64("start_revision", req.StartRevision),
	)

	w := s.watchManager.CreateWatch(
		req.Key,
		req.RangeEnd,
		req.StartRevision,
		req.Filters,
		req.PrevKv,
	)

	mu.Lock()
	watches[w.ID] = w
	mu.Unlock()

	sendChan <- &mykvstoreserverpb.WatchResponse{
		Header:  s.makeHeader(),
		WatchId: int64(w.ID),
		Created: true,
	}

	go func() {
		for event := range w.EventChan {
			resp := &mykvstoreserverpb.WatchResponse{
				Header:  s.makeHeader(),
				WatchId: int64(w.ID),
				Events:  event.Events,
			}

			select {
			case sendChan <- resp:
			case <-w.StopChan:
				return
			}
		}
	}()
}

func (s *Server) handleWatchCancel(req *mykvstoreserverpb.WatchCancelRequest, watches map[watch.WatchID]*watch.Watch, mu *sync.Mutex, sendChan chan *mykvstoreserverpb.WatchResponse) {
	watchID := watch.WatchID(req.WatchId)

	s.logger.Debug("Canceling watch", zap.Int64("watch_id", req.WatchId))

	mu.Lock()
	delete(watches, watchID)
	mu.Unlock()

	s.watchManager.CancelWatch(watchID)

	sendChan <- &mykvstoreserverpb.WatchResponse{
		Header:   s.makeHeader(),
		WatchId:  req.WatchId,
		Canceled: true,
	}
}

func (s *Server) handleWatchProgress(watches map[watch.WatchID]*watch.Watch, mu *sync.Mutex, sendChan chan *mykvstoreserverpb.WatchResponse) {
	mu.Lock()
	defer mu.Unlock()

	for id := range watches {
		sendChan <- &mykvstoreserverpb.WatchResponse{
			Header:  s.makeHeader(),
			WatchId: int64(id),
		}
	}
}
