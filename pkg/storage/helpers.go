package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func PutRevision(s Storage, revision int64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(revision))
	return s.Put([]byte("meta"), []byte("revision"), value)
}

func RevisionKey(revision int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(revision))
	return b
}

func ParseRevisionKey(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid revision key length: got %d, want 8", len(b))
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func GetRevision(s Storage) (int64, error) {
	data, err := s.Get([]byte("meta"), []byte("revision"))

	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(data)), nil
}
