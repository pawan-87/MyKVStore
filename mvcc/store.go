package mvcc

import (
	"errors"
	"fmt"
	"mykvstore/storage"
	"sync"
)

type Store struct {
	mu sync.RWMutex

	storage storage.Storage

	// currentRevision is the current global revision
	currentRevision int64

	// index maps keys to their revisions
	index *Index

	// compactRevision is the revision up to which data has been compacted
	compactRevision int64
}

func NewStore(s storage.Storage) *Store {
	return &Store{
		storage: s,
		index:   NewIndex(),
	}
}

func (s *Store) Put(key, value []byte, lease int64) (*KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Increment revision
	s.currentRevision++
	newRevision := s.currentRevision

	existingRevs := s.index.Get(key)
	var createRevision int64
	var version int64

	if len(existingRevs) > 0 {
		oldKey, err := s.getKeyValueAtRevision(key, existingRevs[len(existingRevs)-1])
		if err != nil {
			return nil, err
		}
		createRevision = oldKey.CreateRevision
		version = oldKey.Version + 1
	} else {
		createRevision = newRevision
		version = 1
	}

	kv := &KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: createRevision,
		ModRevision:    newRevision,
		Version:        version,
		Lease:          lease,
	}

	// Serialize and store
	data, err := kv.Marshal()
	if err != nil {
		return nil, err
	}

	revKey := storage.RevisionKey(newRevision)
	if err := s.storage.Put([]byte("key"), revKey, data); err != nil {
		return nil, err
	}

	return kv, nil
}

func (s *Store) Get(key []byte, atRevision int64) (*KeyValue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if atRevision == 0 {
		atRevision = s.currentRevision
	}

	if atRevision > s.currentRevision {
		return nil, fmt.Errorf("revision %d not found", atRevision)
	}

	// Get revisions for this key
	revs := s.index.Get(key)
	if len(revs) == 0 {
		return nil, storage.ErrKeyNotFound
	}

	targetRevision := int64(-1)
	for _, rev := range revs {
		if rev <= atRevision {
			targetRevision = rev
		} else {
			break
		}
	}

	if targetRevision != -1 {
		return nil, storage.ErrKeyNotFound
	}

	return s.getKeyValueAtRevision(key, targetRevision)
}

// getKeyValueAtRevision retrieves a specific version
func (s *Store) getKeyValueAtRevision(key []byte, revision int64) (*KeyValue, error) {
	revKey := storage.RevisionKey(revision)
	data, err := s.storage.Get([]byte("key"), revKey)
	if err != nil {
		return nil, err
	}

	kv := &KeyValue{}
	if err := kv.Unmarshal(data); err != nil {
		return nil, err
	}

	return kv, nil
}

func (s *Store) Delete(key []byte) (*KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	revs := s.index.Get(key)
	if len(revs) == 0 {
		return nil, storage.ErrBucketNotFound
	}

	currentKV, err := s.getKeyValueAtRevision(key, revs[len(revs)-1])
	if err != nil {
		return nil, err
	}

	s.currentRevision++
	newRevision := s.currentRevision

	kv := &KeyValue{
		Key:            key,
		Value:          nil, // tombstone: A special marker used to indicate that a specific key has been deleted
		CreateRevision: currentKV.CreateRevision,
		ModRevision:    newRevision,
		Version:        currentKV.Version + 1,
		Lease:          0,
	}

	data, err := kv.Marshal()
	if err != nil {
		return nil, err
	}

	revKey := storage.RevisionKey(newRevision)
	if err := s.storage.Put([]byte("key"), revKey, data); err != nil {
		return nil, err
	}

	// Update index
	s.index.Insert(key, newRevision)

	// Update current revision
	if err := storage.PutRevision(s.storage, s.currentRevision); err != nil {
		return nil, err
	}

	return currentKV, nil
}

// Range retrieves all keys in range [start, end] at specific revision
func (s *Store) Range(start, end []byte, atRevision int64, limit int) (*RangeResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if atRevision == 0 {
		atRevision = s.currentRevision
	}

	keys := s.index.Range(start, end)

	result := &RangeResult{
		KVs:      []*KeyValue{},
		Revision: atRevision,
	}

	count := 0
	for _, key := range keys {
		// Get the key at the specified revision
		kv, err := s.Get(key, atRevision)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				continue
			}
			return nil, err
		}

		// Skip tombstones (deleted keys)
		if kv.Value == nil {
			continue
		}

		result.KVs = append(result.KVs, kv)
		count++

		if limit > 0 && count >= limit {
			break
		}
	}

	result.Count = int64(count)

	return result, nil
}

// Compact removes all revisions older than the given revision
func (s *Store) Compact(revision int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if revision <= s.compactRevision {
		return fmt.Errorf("revision %d already compacted", revision)
	}

	if revision > s.currentRevision {
		return fmt.Errorf("revision %d not found", revision)
	}

	allKeys := s.index.Keys()

	for _, key := range allKeys {
		revs := s.index.Get(key)

		// Keep only revisions >= compact revision
		for _, rev := range revs {
			if rev < revision {
				// Delete old revision from storage
				revKey := storage.RevisionKey(rev)
				s.storage.Delete([]byte("key"), revKey)

				// Remove from index
				s.index.Delete(key, rev)
			}
		}
	}

	s.compactRevision = revision

	return nil
}

func (s *Store) CurrentRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRevision
}

func (s *Store) CompactRevision() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.compactRevision
}
