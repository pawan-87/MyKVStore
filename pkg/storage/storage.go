package storage

import (
	"errors"
)

// Storage defines the interface for persistent storage
type Storage interface {
	Open() error

	Close() error

	Get(bucket, key []byte) ([]byte, error)

	Put(bucket, key, value []byte) error

	Delete(bucket, key []byte) error

	// CreateBucket creates a new bucket
	CreateBucket(name []byte) error

	DeleteBucket(name []byte) error

	ForEach(bucket []byte, fn func(k, v []byte) error) error
}

type OpType int

const (
	OpPut OpType = iota
	OpDelete
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrBucketNotFound = errors.New("bucket not found")
	ErrBucketExists   = errors.New("bucket already exists")
	ErrTxClosed       = errors.New("transaction closed")
	ErrDatabaseClosed = errors.New("database closed")
)
