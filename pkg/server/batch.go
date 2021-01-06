package server

import (
	"context"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// GetAll ...
func (s *ImmuServer) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	ind, err := s.getDbIndexFromCtx(ctx, "GetAll")
	if err != nil {
		return nil, err
	}

	list := &schema.Entries{}

	for _, key := range req.Keys {
		e, err := s.dbList.GetByIndex(ind).Get(&schema.KeyRequest{Key: key, SinceTx: req.SinceTx})
		if err != nil {
			return nil, err
		}
		list.Entries = append(list.Entries, e)
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
