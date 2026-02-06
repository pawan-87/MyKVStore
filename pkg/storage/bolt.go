package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BoltBackend struct {
	path string
	db   *bolt.DB
}

func NewBoltBackend(path string) *BoltBackend {
	return &BoltBackend{
		path: path,
	}
}

func (b *BoltBackend) Open() error {
	dir := filepath.Dir(b.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	db, err := bolt.Open(b.path, 0600, &bolt.Options{
		Timeout:        1 * time.Second,
		NoSync:         false,
		NoFreelistSync: false,
		FreelistType:   bolt.FreelistArrayType,
		NoGrowSync:     false,
	})

	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	b.db = db

	if err := b.createDefaultBuckets(); err != nil {
		b.db.Close()
		return err
	}

	return nil
}

func (b *BoltBackend) createDefaultBuckets() error {
	return b.db.Update(func(tx *bolt.Tx) error {
		buckets := []string{"key", "meta", "lease", "auth", "members"}
		for _, bucket := range buckets {
			if _, err := tx.CreateBucket([]byte(bucket)); err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
			}
		}
		return nil
	})
}

func (b *BoltBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func (b *BoltBackend) Get(bucket, key []byte) ([]byte, error) {
	var value []byte

	err := b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		v := b.Get(key)
		if v == nil {
			return ErrKeyNotFound
		}

		value = make([]byte, len(v))
		copy(value, v)

		return nil
	})

	return value, err
}

func (b *BoltBackend) Put(bucket, key, value []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}

		return b.Put(key, value)
	})
}

func (b *BoltBackend) Delete(bucket, key []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		return b.Delete(key)
	})
}

func (b *BoltBackend) CreateBucket(name []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(name)
		if errors.Is(err, bolt.ErrBucketExists) {
			return ErrBucketExists
		}
		return err
	})
}

func (b *BoltBackend) DeleteBucket(name []byte) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(name)
	})
}

// ForEach iterates over all keys in a bucket
func (b *BoltBackend) ForEach(bucket []byte, fn func(k, v []byte) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if b == nil {
			return ErrBucketNotFound
		}
		return b.ForEach(fn)
	})
}
