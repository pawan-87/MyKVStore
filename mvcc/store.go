package mvcc

import (
	"mykvstore/storage"
	"sync"
)

type Store struct {
	mu sync.RWMutex

	storage storage.Storage

	// currentRevision is the current global revision
	currentRevision int64

	// compactRevision is the revision up to which data has been compacted
	compactRevision int64
}

func NewStore(s storage.Storage) *Store {
	return &Store{
		storage: s,
	}
}
