package lease

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Lessor struct {
	mu     sync.RWMutex
	leases map[LeaseID]*Lease

	nextID atomic.Int64

	stopCh chan struct{}
	doneCh chan struct{}

	expireCallback func(leaseID LeaseID, keys []string)

	logger *zap.Logger
}

func NewLessor(logger *zap.Logger) *Lessor {
	return &Lessor{
		leases: make(map[LeaseID]*Lease),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		logger: logger,
	}
}

// Start starts the expiration checker
func (ls *Lessor) Start() {
	go ls.runExpirationChecker()
}

func (ls *Lessor) Stop() {
	close(ls.stopCh)
	<-ls.doneCh
}

func (ls *Lessor) SetExpiredCallback(callback func(leaseID LeaseID, keys []string)) {
	ls.expireCallback = callback
}

func (ls *Lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("ttl must be positive")
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	// Generate ID if not provided
	if id == 0 {
		id = LeaseID(ls.nextID.Add(1))
	}

	// Check if already exists
	if _, exists := ls.leases[id]; exists {
		return nil, fmt.Errorf("lease %d already exists", id)
	}

	lease := NewLease(id, ttl)
	ls.leases[id] = lease

	ls.logger.Debug("Lease granted",
		zap.Int64("lease_id", int64(id)),
		zap.Int64("ttl", ttl),
	)

	return lease, nil
}

// Revoke revokes a lease and returns its keys
func (ls *Lessor) Revoke(id LeaseID) ([]string, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lease, exists := ls.leases[id]
	if !exists {
		return nil, fmt.Errorf("lease not found")
	}

	keys := lease.Keys()
	delete(ls.leases, id)

	ls.logger.Debug("Lease revoked",
		zap.Int64("lease_id", int64(id)),
		zap.Int("keys", len(keys)),
	)

	return keys, nil
}

func (ls *Lessor) Renew(id LeaseID) (int64, error) {
	ls.mu.RLock()
	lease, exists := ls.leases[id]
	ls.mu.RUnlock()

	if !exists {
		return 0, fmt.Errorf("lease not found")
	}

	if lease.IsExpired() {
		return 0, fmt.Errorf("lease expired")
	}

	lease.Refresh()

	ls.logger.Debug("Lease renewed",
		zap.Int64("lease_id", int64(id)),
		zap.Int64("ttl", lease.TTL),
	)

	return lease.TTL, nil
}

func (ls *Lessor) Lookup(id LeaseID) *Lease {
	ls.mu.RLock()
	defer ls.mu.RUnlock()
	return ls.leases[id]
}

func (ls *Lessor) AttachKey(id LeaseID, key string) error {
	ls.mu.RLock()
	lease, exists := ls.leases[id]
	defer ls.mu.RUnlock()

	if !exists {
		return fmt.Errorf("lease not found")
	}

	lease.AttachKey(key)

	ls.logger.Debug("Key attached to lease",
		zap.Int64("lease_id", int64(id)),
		zap.String("key", key),
	)

	return nil
}

func (ls *Lessor) DetachKey(id LeaseID, key string) {
	ls.mu.RLock()
	lease, exists := ls.leases[id]
	ls.mu.RUnlock()

	if !exists {
		return
	}

	lease.DetachKey(key)
}

func (ls *Lessor) runExpirationChecker() {
	defer close(ls.doneCh)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ls.checkExpiredLeases()
		case <-ls.stopCh:
			return
		}
	}
}

func (ls *Lessor) checkExpiredLeases() {
	ls.mu.Lock()

	var expired []LeaseID
	for id, lease := range ls.leases {
		if lease.IsExpired() {
			expired = append(expired, id)
		}
	}

	// Remove expired leases
	expiredLeases := make([]*Lease, 0, len(expired))
	for _, id := range expired {
		lease := ls.leases[id]
		expiredLeases = append(expiredLeases, lease)
		delete(ls.leases, id)
	}

	ls.mu.Unlock()

	if ls.expireCallback != nil {
		for _, lease := range expiredLeases {
			keys := lease.Keys()
			if len(keys) > 0 {
				ls.logger.Info("Lease expired",
					zap.Int64("lease_id", int64(lease.ID)),
					zap.Int("keys", len(keys)),
				)
				ls.expireCallback(lease.ID, keys)
			}
		}
	}
}
