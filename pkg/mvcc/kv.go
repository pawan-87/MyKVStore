package mvcc

import "encoding/json"

type KeyValue struct {
	Key []byte `json:"key"`

	Value []byte `json:"value"`

	// CreateRevision is the revision when the key was created
	CreateRevision int64 `json:"create_revision"`

	// ModRevision is the revision when the key was last modified
	ModRevision int64 `json:"mod_revision"`

	// Version is the number of modification to this key
	Version int64 `json:"version"`

	// Lease is the lease ID attached to this key (0 if no lease)
	Lease int64 `json:"lease,omitempty"`
}

func (kv *KeyValue) Marshal() ([]byte, error) {
	return json.Marshal(kv)
}

func (kv *KeyValue) Unmarshal(data []byte) error {
	return json.Unmarshal(data, kv)
}

func (kv *KeyValue) Copy() *KeyValue {
	return &KeyValue{
		Key:            append([]byte(nil), kv.Key...),
		Value:          append([]byte(nil), kv.Value...),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
		Lease:          kv.Lease,
	}
}

type RangeResult struct {
	KVs      []*KeyValue
	Count    int64
	Revision int64
}
