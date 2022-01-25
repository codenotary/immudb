package database

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/state"
)

type dbStateProvider struct {
	db DB
}

func NewDBStateProvider(db DB) state.StateProvider {
	return &dbStateProvider{db: db}
}

func (r *dbStateProvider) CurrentState(ctx context.Context) (*schema.ImmutableState, error) {
	return r.db.CurrentState()
}
