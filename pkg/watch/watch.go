package watch

import (
	"bytes"
	"mykvstore/mykvstoreserverpb"
)

type WatchID int64

type Watch struct {
	ID             WatchID
	Key            []byte
	RangeEnd       []byte
	StartRev       int64
	Filters        []mykvstoreserverpb.WatchCreateRequest_FilterType
	PrevKV         bool
	ProgressNotify bool

	// Channel to send events
	EventChan chan *WatchEvent

	// Stop signal
	StopChan chan struct{}
}

type WatchEvent struct {
	Events []*mykvstoreserverpb.Event

	IsProgressNotify bool

	// Current revision
	Revision int64
}

func NewWatch(id WatchID, key, rangeEnd []byte, startRev int64) *Watch {
	return &Watch{
		ID:        id,
		Key:       key,
		RangeEnd:  rangeEnd,
		StartRev:  startRev,
		EventChan: make(chan *WatchEvent, 100),
		StopChan:  make(chan struct{}),
	}
}

// Matches returns true if the key matches this watch
func (w *Watch) Matches(key []byte) bool {
	// Single key watch
	if len(w.RangeEnd) == 0 {
		return bytes.Equal(key, w.Key)
	}

	// Range watch: [key, rangeEnd]
	return bytes.Compare(key, w.Key) >= 0 &&
		bytes.Compare(key, w.RangeEnd) < 0
}

func (w *Watch) Stop() {
	close(w.StopChan)
}

func (w *Watch) ShouldSendEvent(eventType mykvstoreserverpb.Event_EventType) bool {
	if len(w.Filters) == 0 {
		return true
	}

	for _, filter := range w.Filters {
		if filter == mykvstoreserverpb.WatchCreateRequest_NOPUT &&
			eventType == mykvstoreserverpb.Event_PUT {
			return false
		}
		if filter == mykvstoreserverpb.WatchCreateRequest_NODELETE &&
			eventType == mykvstoreserverpb.Event_DELETE {
			return false
		}
	}

	return true
}

// SendEvent sends event to watch
func (w *Watch) SendEvent(event *WatchEvent) {
	select {
	case w.EventChan <- event:

	case <-w.StopChan:
	}
}
