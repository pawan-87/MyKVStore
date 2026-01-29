package storage

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
}
