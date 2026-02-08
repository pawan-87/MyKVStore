package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mykvstore/mykvstoreserverpb"
	"mykvstore/pkg/lease"
	"mykvstore/pkg/mvcc"
	"mykvstore/pkg/storage"
)

func (s *Server) executeTxnRequest(ctx context.Context, req *mykvstoreserverpb.TxnRequest) (*mykvstoreserverpb.TxnResponse, error) {
	// Evaluate comparisons
	succeeded, err := s.evaluateComparisons(req.Compare)
	if err != nil {
		return nil, err
	}

	// Execute appropriate operations
	var ops []*mykvstoreserverpb.RequestOp
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	responses, err := s.executeTxnOps(ctx, ops)
	if err != nil {
		return nil, err
	}

	return &mykvstoreserverpb.TxnResponse{
		Header:    s.makeHeader(),
		Succeeded: succeeded,
		Responses: responses,
	}, nil
}

func (s *Server) executeTxnOps(ctx context.Context, ops []*mykvstoreserverpb.RequestOp) ([]*mykvstoreserverpb.ResponseOp, error) {
	responses := make([]*mykvstoreserverpb.ResponseOp, len(ops))

	for i, op := range ops {
		resp, err := s.executeOp(ctx, op)
		if err != nil {
			return nil, err
		}
		responses[i] = resp
	}

	return responses, nil
}

func (s *Server) executeOp(ctx context.Context, op *mykvstoreserverpb.RequestOp) (*mykvstoreserverpb.ResponseOp, error) {

	switch req := op.Request.(type) {
	case *mykvstoreserverpb.RequestOp_RequestRange:
		rangeResp, err := s.executeRange(ctx, req.RequestRange)
		if err != nil {
			return nil, err
		}
		return &mykvstoreserverpb.ResponseOp{
			Response: &mykvstoreserverpb.ResponseOp_ResponseRange{
				ResponseRange: rangeResp,
			},
		}, nil

	case *mykvstoreserverpb.RequestOp_RequestPut:
		putResp, err := s.executePut(ctx, req.RequestPut)
		if err != nil {
			return nil, err
		}
		return &mykvstoreserverpb.ResponseOp{
			Response: &mykvstoreserverpb.ResponseOp_ResponsePut{
				ResponsePut: putResp,
			},
		}, nil

	case *mykvstoreserverpb.RequestOp_RequestDeleteRange:
		delResp, err := s.executeDeleteRange(ctx, req.RequestDeleteRange)
		if err != nil {
			return nil, err
		}

		return &mykvstoreserverpb.ResponseOp{
			Response: &mykvstoreserverpb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: delResp,
			},
		}, nil

	case *mykvstoreserverpb.RequestOp_RequestTxn:
		// Nested transaction
		txnResp, err := s.executeTxnRequest(ctx, req.RequestTxn)
		if err != nil {
			return nil, err
		}
		return &mykvstoreserverpb.ResponseOp{
			Response: &mykvstoreserverpb.ResponseOp_ResponseTxn{
				ResponseTxn: txnResp,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unkown request type")
	}
}

// executeRange executes a Range operation
func (s *Server) executeRange(ctx context.Context, req *mykvstoreserverpb.RangeRequest) (*mykvstoreserverpb.RangeResponse, error) {
	result, err := s.mvccStore.Range(req.Key, req.RangeEnd, req.Revision, int(req.Limit))
	if err != nil {
		return nil, err
	}

	kvs := make([]*mykvstoreserverpb.KeyValue, 0, len(result.KVs))
	for _, kv := range result.KVs {
		if kv.Value == nil {
			continue // skip tombstones
		}
		kvs = append(kvs, &mykvstoreserverpb.KeyValue{
			Key:            kv.Key,
			Value:          kv.Value,
			CreateRevision: kv.CreateRevision,
			ModRevision:    kv.ModRevision,
			Version:        kv.Version,
			Lease:          kv.Lease,
		})
	}

	return &mykvstoreserverpb.RangeResponse{
		Header: s.makeHeader(),
		Kvs:    kvs,
		More:   false,
		Count:  result.Count,
	}, nil
}

func (s *Server) executePut(ctx context.Context, req *mykvstoreserverpb.PutRequest) (*mykvstoreserverpb.PutResponse, error) {
	var prevKV *mykvstoreserverpb.KeyValue

	if req.PrevKv {
		kv, err := s.mvccStore.Get(req.Key, 0)
		if err == nil && kv.Value != nil {
			prevKV = &mykvstoreserverpb.KeyValue{
				Key:            kv.Key,
				Value:          kv.Value,
				CreateRevision: kv.CreateRevision,
				ModRevision:    kv.ModRevision,
				Version:        kv.Version,
				Lease:          kv.Lease,
			}
		}
	}

	_, err := s.mvccStore.Put(req.Key, req.Value, req.Lease)
	if err != nil {
		return nil, err
	}

	// Attach to lease if specified
	if req.Lease != 0 {
		s.lessor.AttachKey(lease.LeaseID(req.Lease), string(req.Key))
	}

	return &mykvstoreserverpb.PutResponse{
		Header: s.makeHeader(),
		PrevKv: prevKV,
	}, nil
}

func (s *Server) executeDeleteRange(ctx context.Context, req *mykvstoreserverpb.DeleteRangeRequest) (*mykvstoreserverpb.DeleteRangeResponse, error) {
	var prevKvs []*mykvstoreserverpb.KeyValue
	if req.PrevKv {
		result, err := s.mvccStore.Range(req.Key, req.RangeEnd, 0, 0)
		if err == nil {
			for _, kv := range result.KVs {
				if kv.Value != nil {
					prevKvs = append(prevKvs, &mykvstoreserverpb.KeyValue{
						Key:            kv.Key,
						Value:          kv.Value,
						CreateRevision: kv.CreateRevision,
						ModRevision:    kv.ModRevision,
						Version:        kv.Version,
						Lease:          kv.Lease,
					})
				}
			}
		}
	}

	if len(req.RangeEnd) == 0 {
		_, err := s.mvccStore.Delete(req.Key)
		if err != nil {
			return nil, err
		}

		return &mykvstoreserverpb.DeleteRangeResponse{
			Header:  s.makeHeader(),
			Deleted: int64(len(prevKvs)),
			PrevKvs: prevKvs,
		}, nil
	}

	result, err := s.mvccStore.Range(req.Key, req.RangeEnd, 0, 0)
	if err != nil {
		return nil, err
	}

	deleted := int64(0)
	for _, kv := range result.KVs {
		if kv.Value != nil {
			_, err := s.mvccStore.Delete(kv.Key)
			if err == nil {
				deleted++
			}
		}
	}

	return &mykvstoreserverpb.DeleteRangeResponse{
		Header:  s.makeHeader(),
		Deleted: deleted,
		PrevKvs: prevKvs,
	}, nil
}

// compareKey evaluates a single comparison
func (s *Server) compareKey(cmp *mykvstoreserverpb.Compare) (bool, error) {
	kv, err := s.mvccStore.Get(cmp.Key, 0)
	if err != nil && !errors.Is(err, storage.ErrKeyNotFound) {
		return false, err
	}

	keyExists := err == nil && kv.Value != nil

	// Determine the target value for comparison
	var targetValue int64
	var currentValue int64

	switch cmp.Target {
	case mykvstoreserverpb.Compare_VERSION:
		if keyExists {
			currentValue = kv.Version
		} else {
			currentValue = 0
		}
		targetValue = cmp.Version

	case mykvstoreserverpb.Compare_CREATE:
		if keyExists {
			currentValue = kv.CreateRevision
		} else {
			currentValue = 0
		}
		targetValue = cmp.CreateRevision

	case mykvstoreserverpb.Compare_MOD:
		if keyExists {
			currentValue = kv.ModRevision
		} else {
			currentValue = 0
		}
		targetValue = cmp.ModRevision

	case mykvstoreserverpb.Compare_VALUE:
		return s.compareValue(cmp, kv)

	case mykvstoreserverpb.Compare_LEASE:
		if keyExists {
			currentValue = kv.Lease
		} else {
			currentValue = 0
		}
		targetValue = cmp.Lease

	default:
		return false, fmt.Errorf("unknow compare target: %v", cmp.Target)
	}

	return s.compareResult(cmp.Result, currentValue, targetValue), nil
}

func (s *Server) compareValue(cmp *mykvstoreserverpb.Compare, kv *mvcc.KeyValue) (bool, error) {
	var currentValue []byte
	if kv != nil && kv.Value != nil {
		currentValue = kv.Value
	}

	compareResult := bytes.Compare(currentValue, cmp.Value)

	switch cmp.Result {
	case mykvstoreserverpb.Compare_EQUAL:
		return compareResult == 0, nil

	case mykvstoreserverpb.Compare_NOT_EQUAL:
		return compareResult != 0, nil

	case mykvstoreserverpb.Compare_GRATER:
		return compareResult > 0, nil

	case mykvstoreserverpb.Compare_LESS:
		return compareResult < 0, nil

	default:
		return false, fmt.Errorf("unknow compare result: %v", cmp.Result)
	}
}

func (s *Server) compareResult(result mykvstoreserverpb.Compare_CompareResult, current int64, target int64) bool {
	switch result {

	case mykvstoreserverpb.Compare_EQUAL:
		return current == target

	case mykvstoreserverpb.Compare_NOT_EQUAL:
		return current != target

	case mykvstoreserverpb.Compare_GRATER:
		return current > target

	case mykvstoreserverpb.Compare_LESS:
		return current < target

	default:
		return false
	}
}

func (s *Server) evaluateComparisons(comparisons []*mykvstoreserverpb.Compare) (bool, error) {
	if len(comparisons) == 0 {
		return true, nil
	}

	for _, cmp := range comparisons {
		result, err := s.compareKey(cmp)
		if err != nil {
			return false, err
		}

		if !result {
			return false, nil
		}
	}

	return true, nil
}

func (s *Server) EvaluateComparisons(req *mykvstoreserverpb.TxnRequest) (bool, error) {
	return s.evaluateComparisons(req.Compare)
}

func (s *Server) ExecuteOp(ctx context.Context, op *mykvstoreserverpb.RequestOp) (*mykvstoreserverpb.ResponseOp, error) {
	return s.executeOp(ctx, op)
}
