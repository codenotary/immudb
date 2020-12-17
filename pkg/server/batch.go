package server

import (
	"context"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// GetAll ...
func (s *ImmuServer) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.ItemList, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(ctx, "GetAll")
	if err != nil {
		return nil, err
	}

	list := &schema.ItemList{}

	for _, key := range req.Keys {
		item, err := s.dbList.GetByIndex(ind).Get(&schema.KeyRequest{Key: key, SinceTx: req.SinceTx})
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

func (s *ImmuServer) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	s.Logger.Debugf("set atomic operations")

	ind, err := s.getDbIndexFromCtx(ctx, "ExecAll")
	if err != nil {
		return nil, err
	}

	return s.dbList.GetByIndex(ind).ExecAll(req)
}
