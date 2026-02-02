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
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   []byte      `json:"key,omitempty"`
	Value []byte      `json:"value,omitempty"`
	Lease int64       `json:"lease,omitempty"`

	Revision int64 `json:"revision,omitempty"`
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

func NewPutCommand(key, value []byte, lease int64) *Command {
	return &Command{
		Type:  CommandPut,
		Key:   key,
		Value: value,
		Lease: lease,
	}
}

func NewDeleteCommand(key []byte) *Command {
	return &Command{
		Type: CommandDelete,
		Key:  key,
	}
}

func NewCompactCommand(revision int64) *Command {
	return &Command{
		Type:     CommandCompact,
		Revision: revision,
	}
}
