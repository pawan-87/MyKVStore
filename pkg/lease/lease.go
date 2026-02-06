package lease

import (
	"sync"
	"time"
)

type LeaseID int64

type Lease struct {
	ID  LeaseID
	TTL int64 // seconds

	mu         sync.RWMutex
	expiryTime time.Time
	keys       map[string]struct{} // attached keys

	keepAliveChan chan struct{}
}

func NewLease(id LeaseID, ttl int64) *Lease {
	return &Lease{
		ID:            id,
		TTL:           ttl,
		expiryTime:    time.Now().Add(time.Duration(ttl) * time.Second),
		keys:          make(map[string]struct{}),
		keepAliveChan: make(chan struct{}, 1),
	}
}

// Remaining returns remaining TTL in seconds
func (l *Lease) Remaining() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	remaining := time.Until(l.expiryTime).Seconds()
	if remaining < 0 {
		return 0
	}

	return int64(remaining)
}

// IsExpired returns true if the lease has expired
func (l *Lease) IsExpired() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return time.Now().After(l.expiryTime)
}

func (l *Lease) Refresh() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expiryTime = time.Now().Add(time.Duration(l.TTL) * time.Second)
}

func (l *Lease) AttachKey(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys[key] = struct{}{}
}

func (l *Lease) DetachKey(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.keys, key)
}

func (l *Lease) Keys() []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	keys := make([]string, 0, len(l.keys))
	for k := range l.keys {
		keys = append(keys, k)
	}

	return keys
}
