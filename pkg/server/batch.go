package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/store"
)

// SetBatch ...
func (s *ImmuServer) SetBatch(ctx context.Context, kvl *schema.KVList) (*schema.Index, error) {
	s.Logger.Debugf("set batch %d", len(kvl.KVs))

	ind, err := s.getDbIndexFromCtx(ctx, "SetBatch")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).SetBatch(kvl)
}

// GetBatch ...
func (s *ImmuServer) GetBatch(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	ind, err := s.getDbIndexFromCtx(ctx, "GetBatch")
	if err != nil {
		return nil, err
	}

	for _, key := range kl.Keys {
		item, err := s.dbList.GetByIndex(ind).Get(key)
		if err == nil || err == store.ErrKeyNotFound {
			if item != nil {
				list.Items = append(list.Items, item)
			}
		} else {
			return nil, err
		}
	}

	return list, nil
}

func (s *ImmuServer) SetBatchAtomicOperations(ctx context.Context, operations *schema.AtomicOperations) (*schema.Index, error) {
	panic("implement me")
}
