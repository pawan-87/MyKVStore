package lease

import (
	"encoding/json"
	"sync"
	"time"
)

type LeaseID int64

type Lease struct {
	ID        LeaseID
	TTL       int64     // seconds
	GrantTime time.Time // When lease was granted (for persistence)

	mu         sync.RWMutex
	expiryTime time.Time
	keys       map[string]struct{} // attached keys

	keepAliveChan chan struct{}
}

func NewLease(id LeaseID, ttl int64) *Lease {
	return &Lease{
		ID:            id,
		TTL:           ttl,
		GrantTime:     time.Now(),
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

func (l *Lease) RemainingTTL() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	remaining := time.Until(l.expiryTime).Seconds()
	if remaining < 0 {
		return 0
	}

	return int64(remaining)
}

func (l *Lease) MarshalJSON() ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	keysSlice := make([]string, 0, len(l.keys))
	for k := range l.keys {
		keysSlice = append(keysSlice, k)
	}

	return json.Marshal(struct {
		ID        LeaseID   `json:"id"`
		TTL       int64     `json:"ttl"`
		GrantTime time.Time `json:"grant_time"`
		Keys      []string  `json:"keys"`
	}{
		ID:        l.ID,
		TTL:       l.TTL,
		GrantTime: l.GrantTime,
		Keys:      keysSlice,
	})
}

func (l *Lease) UnmarshalJSON(data []byte) error {
	var temp struct {
		ID        LeaseID   `json:"id"`
		TTL       int64     `json:"ttl"`
		GrantTime time.Time `json:"grant_time"`
		Keys      []string  `json:"keys"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	l.ID = temp.ID
	l.TTL = temp.TTL
	l.GrantTime = temp.GrantTime
	l.keys = make(map[string]struct{})
	for _, k := range temp.Keys {
		l.keys[k] = struct{}{}
	}
	l.keepAliveChan = make(chan struct{}, 1)

	// calculate expiry time based on grant time and TTL
	elapsed := time.Since(l.GrantTime)
	remaining := time.Duration(l.TTL)*time.Second - elapsed
	if remaining > 0 {
		l.expiryTime = time.Now().Add(remaining)
	} else {
		l.expiryTime = time.Now()
	}

	return nil
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
