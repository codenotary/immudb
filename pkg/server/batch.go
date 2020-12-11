package server

import (
	"context"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// GetAll ...
func (s *ImmuServer) GetAll(ctx context.Context, kl *schema.KeyList) (*schema.ItemList, error) {
	list := &schema.ItemList{}
	ind, err := s.getDbIndexFromCtx(ctx, "GetAll")
	if err != nil {
		return nil, err
	}

	for _, key := range kl.Keys {
		item, err := s.dbList.GetByIndex(ind).Get(&schema.KeyRequest{Key: key})
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

func (s *ImmuServer) ExecAllOps(ctx context.Context, operations *schema.Ops) (*schema.TxMetadata, error) {
	s.Logger.Debugf("set batch atomic operations")

	ind, err := s.getDbIndexFromCtx(ctx, "ExecAllOps")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ExecAllOps(operations)
}
