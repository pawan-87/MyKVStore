package lease

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pawan-87/MyKVStore/pkg/storage"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type Lessor struct {
	backend *storage.BoltBackend

	mu     sync.RWMutex
	leases map[LeaseID]*Lease

	nextID atomic.Int64

	stopCh chan struct{}
	doneCh chan struct{}

	expireCallback func(leaseID LeaseID, keys []string)

	logger *zap.Logger
}

func NewLessor(backend *storage.BoltBackend, logger *zap.Logger) *Lessor {
	return &Lessor{
		backend: backend,
		leases:  make(map[LeaseID]*Lease),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
		logger:  logger,
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

func (ls *Lessor) saveLease(lease *Lease) error {
	data, err := json.Marshal(lease)
	if err != nil {
		return err
	}

	leaseKey := make([]byte, 8)
	binary.BigEndian.PutUint64(leaseKey, uint64(lease.ID))

	return ls.backend.Put([]byte("lease"), leaseKey, data)
}

func (ls *Lessor) deleteLease(id LeaseID) error {
	leaseKey := make([]byte, 8)
	binary.BigEndian.PutUint64(leaseKey, uint64(id))

	return ls.backend.Delete([]byte("lease"), leaseKey)
}

func (ls *Lessor) Restore() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	return ls.backend.ForEach([]byte("lease"), func(k, v []byte) error {
		lease := &Lease{}
		if err := json.Unmarshal(v, lease); err != nil {
			return err
		}

		// Calculate remaining TTL
		elapsed := time.Since(lease.GrantTime)
		remaining := time.Duration(lease.TTL)*time.Second - elapsed

		if remaining > 0 {
			ls.leases[lease.ID] = lease

			ls.logger.Info("Restored lease",
				zap.Int64("id", int64(lease.ID)),
				zap.Duration("remaining", remaining),
			)
		} else {
			// Lease already expired, skip restoration
			ls.logger.Info("Skipped expired lease",
				zap.Int64("id", int64(lease.ID)),
			)
		}

		return nil
	})
}

func (ls *Lessor) SetExpiredCallback(callback func(leaseID LeaseID, keys []string)) {
	ls.expireCallback = callback
}

// ApplyGrant applies a lease grant (called after Raft commit on ALL nodes)
func (ls *Lessor) ApplyGrant(id LeaseID, ttl int64) (*Lease, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if existing := ls.leases[id]; existing != nil {
		return existing, nil
	}

	lease := NewLease(id, ttl)

	// 1. Store in bbolt
	if err := ls.saveLease(lease); err != nil {
		return nil, err
	}

	// 2. Cache in memor
	ls.leases[id] = lease

	ls.logger.Info("Lease granted (applied)",
		zap.Int64("id", int64(id)),
		zap.Int64("ttl", ttl),
	)

	return lease, nil
}

func (ls *Lessor) ApplyRevoke(id LeaseID) ([]string, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lease := ls.leases[id]
	if lease == nil {
		return nil, fmt.Errorf("lease not found")
	}

	keys := lease.Keys()

	if err := ls.deleteLease(id); err != nil {
		return nil, err
	}

	delete(ls.leases, id)

	ls.logger.Debug("Lease revoked (applied)",
		zap.Int64("id", int64(id)),
		zap.Int("keys", len(keys)),
	)

	return keys, nil
}

func (ls *Lessor) ApplyKeepAlive(id LeaseID) (int64, error) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lease := ls.leases[id]
	if lease == nil {
		return 0, fmt.Errorf("lease not found")
	}

	lease.Refresh()

	if err := ls.saveLease(lease); err != nil {
		return 0, err
	}

	ttl := lease.RemainingTTL()

	ls.logger.Debug("Lease kept alive (applied)",
		zap.Int64("id", int64(id)),
		zap.Int64("remaining_ttl", ttl),
	)

	return ttl, nil
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
