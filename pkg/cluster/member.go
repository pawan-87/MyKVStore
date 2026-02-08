package cluster

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pawan-87/MyKVStore/pkg/storage"
	"sync"

	"go.uber.org/zap"
)

var (
	membersBucketName        = []byte("members")
	membersRemovedBucketName = []byte("members_removed")
)

type Member struct {
	ID         uint64   `json:"id"`
	Name       string   `json:"name"`
	PeerURLs   []string `json:"peer_urls"`
	ClientURLs []string `json:"client_urls"`
	IsLearner  bool     `json:"is_learner"`
}

type MemberStore struct {
	mu      sync.RWMutex
	members map[uint64]*Member
	removed map[uint64]bool

	backend storage.Storage
	logger  *zap.Logger
}

// loadFromBackend reads all members from bbolt into the in-memory cache
func (ms *MemberStore) loadFromBackend() error {
	err := ms.backend.ForEach(membersBucketName, func(k, v []byte) error {
		var m Member
		if err := json.Unmarshal(v, &m); err != nil {
			ms.logger.Warn("Failed to unmarshal members", zap.Error(err))
			return nil
		}
		ms.members[m.ID] = &m
		return nil
	})

	if err != nil {
		ms.logger.Debug("No existing members in backend")
	}

	// Load removed members IDs
	_ = ms.backend.ForEach(membersRemovedBucketName, func(k, v []byte) error {
		// key will be ID which will be Endian format (8 bytes)
		if len(k) == 8 {
			id := binary.BigEndian.Uint64(k)
			ms.removed[id] = true
		}
		return nil
	})

	ms.logger.Info("Loaded members from backend",
		zap.Int("active", len(ms.members)),
		zap.Int("removed", len(ms.removed)),
	)

	return nil
}

func NewMemberStore(backend storage.Storage, logger *zap.Logger) *MemberStore {
	ms := &MemberStore{
		members: make(map[uint64]*Member),
		removed: make(map[uint64]bool),
		backend: backend,
		logger:  logger,
	}

	if err := ms.loadFromBackend(); err != nil {
		ms.logger.Warn("Failed to load members from backend", zap.Error(err))
	}

	return ms
}

func memberKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	return key
}

func (ms *MemberStore) saveMemberToBackend(m *Member) error {
	data, err := json.Marshal(m)

	if err != nil {
		return fmt.Errorf("failed to marshal members %d: %w", m.ID, err)
	}

	if err := ms.backend.Put(membersBucketName, memberKey(m.ID), data); err != nil {
		return fmt.Errorf("failed to save members %d to backend: %w", m.ID, err)
	}

	return nil
}

func (ms *MemberStore) deleteMemberFromBackend(id uint64) error {
	key := memberKey(id)

	if err := ms.backend.Delete(membersBucketName, key); err != nil {
		return fmt.Errorf("failed to delete members %d from backend: %w", id, err)
	}

	if err := ms.backend.Put(membersRemovedBucketName, key, []byte("ture")); err != nil {
		return fmt.Errorf("failed to record removed members %d: %w", id, err)
	}

	return nil
}

func (ms *MemberStore) Add(member *Member) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.members[member.ID]; exists {
		return fmt.Errorf("members %d already exists", member.ID)
	}

	if ms.removed[member.ID] {
		return fmt.Errorf("members %d was previously remvoed", member.ID)
	}

	if err := ms.saveMemberToBackend(member); err != nil {
		return err
	}

	// Update in-memory cache
	ms.members[member.ID] = member

	ms.logger.Info("Member added",
		zap.Uint64("id", member.ID),
		zap.String("name", member.Name),
		zap.Strings("peer_urls", member.PeerURLs),
		zap.Bool("is_learner", member.IsLearner),
	)

	return nil
}

func (ms *MemberStore) Remove(id uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.members[id]; !exists {
		return fmt.Errorf("member %d not found", id)
	}

	if err := ms.deleteMemberFromBackend(id); err != nil {
		return err
	}

	delete(ms.members, id)
	ms.removed[id] = true

	ms.logger.Info("Member removed", zap.Uint64("id", id))

	return nil
}

func (ms *MemberStore) Update(id uint64, peerURLs, clientURLs []string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	member, exists := ms.members[id]
	if !exists {
		return fmt.Errorf("members %d not found", id)
	}

	if len(peerURLs) > 0 {
		member.PeerURLs = peerURLs
	}
	if len(clientURLs) > 0 {
		member.ClientURLs = clientURLs
	}

	if err := ms.saveMemberToBackend(member); err != nil {
		return err
	}

	ms.logger.Info("Member updated",
		zap.Uint64("id", id),
		zap.Strings("peer_urls", member.PeerURLs),
	)

	return nil
}

func (ms *MemberStore) Promote(id uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	member, exists := ms.members[id]
	if !exists {
		return fmt.Errorf("members %d not found", id)
	}

	if !member.IsLearner {
		return fmt.Errorf("members %d is already a voting memeber", id)
	}

	member.IsLearner = false

	if err := ms.saveMemberToBackend(member); err != nil {
		return err
	}

	ms.logger.Info("Member promoted to voter", zap.Uint64("id", id))

	return nil
}

func (ms *MemberStore) Get(id uint64) *Member {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.members[id]
}

func (ms *MemberStore) List() []*Member {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	members := make([]*Member, 0, len(ms.members))
	for _, m := range ms.members {
		members = append(members, m)
	}
	return members
}

func (ms *MemberStore) IDs() []uint64 {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	ids := make([]uint64, 0, len(ms.members))
	for id := range ms.members {
		ids = append(ids, id)
	}
	return ids
}

func (ms *MemberStore) IsRemoved(id uint64) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.removed[id]
}
