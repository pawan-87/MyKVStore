package mvcc

import (
	"bytes"
	"sort"
	"sync"
)

type Index struct {
	mu sync.RWMutex

	// tree maps keys to their revisions (sorted)
	tree map[string][]int64

	// keys maintains sorted list of all keys (for range queries)
	keys [][]byte
}

func NewIndex() *Index {
	return &Index{
		tree: make(map[string][]int64),
		keys: [][]byte{},
	}
}

func (idx *Index) Insert(key []byte, revision int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	keyStr := string(key)

	// Add revision to key's list
	revs := idx.tree[keyStr]
	revs = append(revs, revision)
	sort.Slice(revs, func(i, j int) bool {
		return revs[i] < revs[j]
	})
	idx.tree[keyStr] = revs

	// Add key to sorted keys list if new
	if len(revs) == 1 {
		idx.keys = append(idx.keys, key)
		sort.Slice(idx.keys, func(i, j int) bool {
			return bytes.Compare(idx.keys[i], idx.keys[j]) < 0
		})
	}
}

// Get returns all revisions for a key
func (idx *Index) Get(key []byte) []int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	revs := idx.tree[string(key)]
	if revs == nil {
		return []int64{}
	}

	result := make([]int64, len(revs))
	copy(result, revs)

	return result
}

// Delete removes a specific revision for key
func (idx *Index) Delete(key []byte, revision int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	keyStr := string(key)
	revs := idx.tree[keyStr]

	// Remove revision
	var newRevs []int64
	for _, rev := range revs {
		if rev != revision {
			newRevs = append(newRevs, rev)
		}
	}

	if len(newRevs) == 0 {
		// No more revisions, remove key
		delete(idx.tree, keyStr)

		// Remove from keys list
		var newKeys [][]byte
		for _, k := range idx.keys {
			if !bytes.Equal(k, key) {
				newKeys = append(newKeys, k)
			}
		}

		idx.keys = newKeys
	} else {
		idx.tree[keyStr] = newRevs
	}
}

// Range returns all keys in range [start, end]
func (idx *Index) Range(start, end []byte) [][]byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result [][]byte

	for _, key := range idx.keys {
		if start != nil && bytes.Compare(key, start) < 0 {
			continue
		}
		if end != nil && bytes.Compare(key, end) >= 0 {
			break
		}
		result = append(result, key)
	}

	return result
}

// Keys returns all keys
func (idx *Index) Keys() [][]byte {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([][]byte, len(idx.keys))
	copy(result, idx.keys)

	return result
}

// Count returns the number of keys
func (idx *Index) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.keys)
}
