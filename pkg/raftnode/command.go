package raftnode

import (
	"encoding/json"
	"fmt"
)

type CommandType int

const (
	CommandPut CommandType = iota
	CommandDelete
	CommandCompact
	CommandLeaseGrant
	CommandLeaseRevoke
	CommandLeaseKeepAlive
	CommandTxn
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   []byte      `json:"key,omitempty"`
	Value []byte      `json:"value,omitempty"`
	Lease int64       `json:"lease,omitempty"`

	// For compact command
	Revision int64 `json:"revision,omitempty"`

	// ReqID is used to match apply result back to the waiting caller
	ReqID uint64 `json:"req_id,omitempty"`

	LeaseID  int64 `json:"lease_id,omitempty"`
	LeaseTTL int64 `json:"lease_ttl,omitempty"`

	TxnRequest []byte `json:"txn_request,omitempty"`
}

func (c *Command) Encode() ([]byte, error) {
	return json.Marshal(c)
}

func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("failed to decode command: %w", err)
	}
	return &cmd, nil
}

func NewPutCommand(key, value []byte, lease int64, reqID uint64) *Command {
	return &Command{
		Type:  CommandPut,
		Key:   key,
		Value: value,
		Lease: lease,
		ReqID: reqID,
	}
}

func NewDeleteCommand(key []byte, reqID uint64) *Command {
	return &Command{
		Type:  CommandDelete,
		Key:   key,
		ReqID: reqID,
	}
}

func NewCompactCommand(revision int64, reqID uint64) *Command {
	return &Command{
		Type:     CommandCompact,
		Revision: revision,
		ReqID:    reqID,
	}
}

func NewLeaseGrantCommand(leaseID, ttl int64, reqID uint64) *Command {
	return &Command{
		Type:     CommandLeaseGrant,
		LeaseID:  leaseID,
		LeaseTTL: ttl,
		ReqID:    reqID,
	}
}

func NewLeaseRevokeCommand(leaseID int64, reqID uint64) *Command {
	return &Command{
		Type:    CommandLeaseRevoke,
		LeaseID: leaseID,
		ReqID:   reqID,
	}
}

func NewLeaseKeepAliveCommand(leaseID int64, reqID uint64) *Command {
	return &Command{
		Type:    CommandLeaseKeepAlive,
		LeaseID: leaseID,
		ReqID:   reqID,
	}
}

func NewTxnCommand(txnRequest []byte, reqID uint64) *Command {
	return &Command{
		Type:       CommandTxn,
		TxnRequest: txnRequest,
		ReqID:      reqID,
	}
}
