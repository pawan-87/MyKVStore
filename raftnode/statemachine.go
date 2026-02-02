package raftnode

import (
	"fmt"
	"mykvstore/mvcc"
)

type StateMachine interface {
	// Apply applies a command to the state machine
	Apply(cmd *Command) error

	// Snapshot creates a snapshot of the state machine
	Snapshot() ([]byte, error)

	// Restore restores the state machine from a snapshot
	Restore(snapshot []byte) error
}

type MVCCStateMachine struct {
	store *mvcc.Store
}

func NewMVCCStateMachine(store *mvcc.Store) *MVCCStateMachine {
	return &MVCCStateMachine{
		store: store,
	}
}

func (sm MVCCStateMachine) Apply(cmd *Command) error {
	switch cmd.Type {
	case CommandPut:
		_, err := sm.store.Put(cmd.Key, cmd.Value, cmd.Lease)
		return err

	case CommandDelete:
		_, err := sm.store.Delete(cmd.Key)
		return err

	case CommandCompact:
		return sm.store.Compact(cmd.Revision)

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (sm MVCCStateMachine) Snapshot() ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (sm MVCCStateMachine) Restore(snapshot []byte) error {
	//TODO implement me
	panic("implement me")
}
