package database

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/state"
	"github.com/rs/xid"
	"strings"
	"sync"
)

type inMemoryStateService struct {
	stateProvider state.StateProvider
	cache         cache.Cache
	serverUUID    string
	sync.RWMutex
}

func NewInMemoryStateService(db DB) (*inMemoryStateService, error) {
	return &inMemoryStateService{
		stateProvider: NewDBStateProvider(db),
		cache:         cache.NewInMemoryCache(),
		serverUUID:    xid.New().String(),
	}, nil
}

func (r *inMemoryStateService) GetState(ctx context.Context, db string) (*schema.ImmutableState, error) {
	r.Lock()
	defer r.Unlock()

	state, err := r.cache.Get(r.serverUUID, db)
	if err == nil {
		return state, nil
	}
	if !strings.HasPrefix(err.Error(), "no roots found for server") {
		return nil, err
	}

	state, err = r.stateProvider.CurrentState(ctx)
	if err != nil {
		return nil, err
	}

	if err := r.cache.Set(r.serverUUID, db, state); err != nil {
		return nil, err
	}

	return state, nil
}

func (r *inMemoryStateService) SetState(db string, state *schema.ImmutableState) error {
	r.Lock()
	defer r.Unlock()

	return r.cache.Set(r.serverUUID, db, state)
}

func (r *inMemoryStateService) CacheLock() error {
	return fmt.Errorf("not implemented")
}

func (r *inMemoryStateService) CacheUnlock() error {
	return fmt.Errorf("not implemented")
}
