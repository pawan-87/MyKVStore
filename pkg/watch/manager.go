package watch

import (
	"mykvstore/mykvstoreserverpb"
	mvcc2 "mykvstore/pkg/mvcc"
	"sync"

	"go.uber.org/zap"
)

type WatchManager struct {
	mu sync.RWMutex

	watches map[WatchID]*Watch

	nextID WatchID

	mvccStore *mvcc2.Store

	logger *zap.Logger
}

func NewWatchManager(mvccStore *mvcc2.Store, logger *zap.Logger) *WatchManager {
	return &WatchManager{
		watches:   make(map[WatchID]*Watch),
		nextID:    1,
		mvccStore: mvccStore,
		logger:    logger,
	}
}

func (wm *WatchManager) CreateWatch(key, rangeEnd []byte, startRev int64, filters []mykvstoreserverpb.WatchCreateRequest_FilterType, prevKV bool) *Watch {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	id := wm.nextID
	wm.nextID++

	watch := NewWatch(id, key, rangeEnd, startRev)
	watch.Filters = filters
	watch.PrevKV = prevKV

	wm.watches[id] = watch

	wm.logger.Debug("Created watch",
		zap.Int64("watch_id", int64(id)),
		zap.ByteString("key", key),
		zap.Int64("start_revision", startRev),
	)

	if startRev > 0 {
		go wm.sendHistoricalEvents(watch)
	}

	return watch
}

func (wm *WatchManager) CancelWatch(id WatchID) {
	wm.mu.Lock()
	watch, exists := wm.watches[id]
	delete(wm.watches, id)
	wm.mu.Unlock()

	if exists {
		watch.Stop()
		close(watch.EventChan)
		wm.logger.Debug("Canceled watch", zap.Int64("watch_id", int64(id)))
	}
}

// Notify notifies all matching watches of an event
func (wm *WatchManager) Notify(key []byte, eventType mykvstoreserverpb.Event_EventType, kv, prevKV *mvcc2.KeyValue) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	pbKV := &mykvstoreserverpb.KeyValue{
		Key:            kv.Key,
		Value:          kv.Value,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}

	event := &mykvstoreserverpb.Event{
		Type: eventType,
		Kv:   pbKV,
	}

	for _, watch := range wm.watches {
		if watch.Matches(key) && watch.ShouldSendEvent(eventType) {
			// Include prev_kv if requested
			if watch.PrevKV && pbKV != nil {
				event.PrevKv = pbKV
			}

			watchEvent := &WatchEvent{
				Events:   []*mykvstoreserverpb.Event{event},
				Revision: kv.ModRevision,
			}

			watch.SendEvent(watchEvent)
		}
	}

	wm.logger.Debug("Notified watches",
		zap.ByteString("kye", key),
		zap.String("event_type", eventType.String()),
		zap.Int("watch_count", len(wm.watches)),
	)
}

func (wm *WatchManager) sendHistoricalEvents(watch *Watch) {
	// TODO: implement
}
