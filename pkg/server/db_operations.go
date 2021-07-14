package server

import (
	"context"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/golang/protobuf/ptypes/empty"
)

// CurrentState ...
func (s *ImmuServer) CurrentState(ctx context.Context, _ *empty.Empty) (*schema.ImmutableState, error) {
	db, err := s.getDBFromCtx(ctx, "CurrentState")
	if err != nil {
		return nil, err
	}

	state, err := db.CurrentState()
	if err != nil {
		return nil, err
	}

	state.Db = db.GetOptions().GetDbName()

	if s.Options.SigningKey != "" {
		err = s.StateSigner.Sign(state)
		if err != nil {
			return nil, err
		}
	}

	return state, nil
}

// Set ...
func (s *ImmuServer) Set(ctx context.Context, kv *schema.SetRequest) (*schema.TxMetadata, error) {
	db, err := s.getDBFromCtx(ctx, "Set")
	if err != nil {
		return nil, err
	}

	return db.Set(kv)
}

// VerifiableSet ...
func (s *ImmuServer) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableSet")
	if err != nil {
		return nil, err
	}

	vtx, err := db.VerifiableSet(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// Get ...
func (s *ImmuServer) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	db, err := s.getDBFromCtx(ctx, "Get")
	if err != nil {
		return nil, err
	}

	return db.Get(req)
}

// VerifiableGet ...
func (s *ImmuServer) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableGet")
	if err != nil {
		return nil, err
	}

	vEntry, err := db.VerifiableGet(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vEntry.VerifiableTx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vEntry.VerifiableTx.Signature = newState.Signature
	}

	return vEntry, nil
}

// Scan ...
func (s *ImmuServer) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	db, err := s.getDBFromCtx(ctx, "Scan")
	if err != nil {
		return nil, err
	}

	return db.Scan(req)
}

// Count ...
func (s *ImmuServer) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, errors.New("Functionality not yet supported")
}

// CountAll ...
func (s *ImmuServer) CountAll(ctx context.Context, _ *empty.Empty) (*schema.EntryCount, error) {
	return nil, errors.New("Functionality not yet supported")
}

// TxByID ...
func (s *ImmuServer) TxById(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	db, err := s.getDBFromCtx(ctx, "TxByID")
	if err != nil {
		return nil, err
	}

	return db.TxByID(req)
}

// VerifiableTxByID ...
func (s *ImmuServer) VerifiableTxById(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableTxByID")
	if err != nil {
		return nil, err
	}

	vtx, err := db.VerifiableTxByID(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// TxScan ...
func (s *ImmuServer) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	db, err := s.getDBFromCtx(ctx, "TxScan")
	if err != nil {
		return nil, err
	}

	return db.TxScan(req)
}

// History ...
func (s *ImmuServer) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	db, err := s.getDBFromCtx(ctx, "History")
	if err != nil {
		return nil, err
	}

	return db.History(req)
}

// SetReference ...
func (s *ImmuServer) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
	db, err := s.getDBFromCtx(ctx, "SetReference")
	if err != nil {
		return nil, err
	}

	return db.SetReference(req)
}

// VerifibleSetReference ...
func (s *ImmuServer) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableSetReference")
	if err != nil {
		return nil, err
	}

	vtx, err := db.VerifiableSetReference(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// ZAdd ...
func (s *ImmuServer) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxMetadata, error) {
	db, err := s.getDBFromCtx(ctx, "ZAdd")
	if err != nil {
		return nil, err
	}

	return db.ZAdd(req)
}

// ZScan ...
func (s *ImmuServer) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	db, err := s.getDBFromCtx(ctx, "ZScan")
	if err != nil {
		return nil, err
	}

	return db.ZScan(req)
}

// VerifiableZAdd ...
func (s *ImmuServer) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	db, err := s.getDBFromCtx(ctx, "VerifiableZAdd")
	if err != nil {
		return nil, err
	}

	vtx, err := db.VerifiableZAdd(req)
	if err != nil {
		return nil, err
	}

	if s.Options.SigningKey != "" {
		md := schema.TxMetadataFrom(vtx.DualProof.TargetTxMetadata)
		alh := md.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetOptions().GetDbName(),
			TxId:   md.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		vtx.Signature = newState.Signature
	}

	return vtx, nil
}

// DEPRECATED: use CompactIndex
func (s *ImmuServer) CleanIndex(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {
	return s.CompactIndex(ctx, e)
}

func (s *ImmuServer) CompactIndex(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	db, err := s.getDBFromCtx(ctx, "CompactIndex")
	if err != nil {
		return nil, err
	}

	err = db.CompactIndex()
	return &empty.Empty{}, err
}

// GetAll ...
func (s *ImmuServer) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	db, err := s.getDBFromCtx(ctx, "GetAll")
	if err != nil {
		return nil, err
	}

	list := &schema.Entries{}

	for _, key := range req.Keys {
		e, err := db.Get(&schema.KeyRequest{Key: key, SinceTx: req.SinceTx})
		if err != nil {
			return nil, err
		}
		list.Entries = append(list.Entries, e)
	}

	return list, nil
}

func (s *ImmuServer) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxMetadata, error) {
	db, err := s.getDBFromCtx(ctx, "ExecAll")
	if err != nil {
		return nil, err
	}

	return db.ExecAll(req)
}
