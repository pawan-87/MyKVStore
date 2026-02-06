package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type ReqID uint64

type ReqIDGen struct {
	memberID uint64
	next     atomic.Uint64
}

func NewReqIDGen(memberID uint64) *ReqIDGen {
	return &ReqIDGen{memberID: memberID}
}

func (rid *ReqIDGen) Next() uint64 {
	seq := rid.next.Add(1)
	id := uint64(rid.memberID) << 32
	id = id | seq
	return id
}

// ApplyResult is delivered when the proposal is applied to the state machine
type ApplyResult struct {
	// Data returned from the proposal (if any)
	Data interface{}

	// Err is non-nil if apply failed
	Err error
}

type ApplyWait struct {
	mu sync.Mutex
	m  map[ReqID]chan ApplyResult
}

func NewApplyWait() *ApplyWait {
	return &ApplyWait{
		m: make(map[ReqID]chan ApplyResult),
	}
}

func (aw *ApplyWait) Register(id ReqID) <-chan ApplyResult {
	aw.mu.Lock()
	defer aw.mu.Unlock()
	ch := make(chan ApplyResult, 1)
	aw.m[id] = ch
	return ch
}

// Trigger delivers the apply result for reqID and cleans up the waiter
func (aw *ApplyWait) Trigger(id ReqID, res ApplyResult) {
	aw.mu.Lock()
	ch, ok := aw.m[id]
	delete(aw.m, id)
	aw.mu.Unlock()

	if !ok {
		return
	}

	select {
	case ch <- res:
	default:
	}

	close(ch)
}

func (aw *ApplyWait) Cancel(id ReqID) {
	aw.mu.Lock()
	ch, ok := aw.m[id]
	delete(aw.m, id)
	aw.mu.Unlock()
	if ok {
		close(ch)
	}
}

func Wait(ctx context.Context, ch <-chan ApplyResult, timeout time.Duration) (ApplyResult, error) {
	timer := time.NewTicker(timeout)
	defer timer.Stop()

	select {
	case res, ok := <-ch:
		if !ok {
			return ApplyResult{}, fmt.Errorf("request cancelled")
		}
		if res.Err != nil {
			return ApplyResult{}, res.Err
		}
		return res, nil

	case <-timer.C:
		return ApplyResult{}, fmt.Errorf("request timeout")

	case <-ctx.Done():
		return ApplyResult{}, ctx.Err()
	}
}
