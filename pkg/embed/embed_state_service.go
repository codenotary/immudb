package embed

import (
	"context"
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/state"
	"sync"
)

type embedStateService struct {
	stateProvider state.StateProvider
	cache         cache.Cache
	serverUUID    string
	sync.RWMutex
}

func NewEmbedStateService(opts *client.Options) (*embedStateService, error) {
	cache, err := NewImmudbStateCache(opts)
	if err != nil {
		return nil, err
	}
	return &embedStateService{
		cache: cache,
	}, nil
}

func (r *embedStateService) InjectDB(db database.DB) {
	r.stateProvider = database.NewDBStateProvider(db)
}

func (r *embedStateService) InjectUUID(uuid string) {
	r.serverUUID = uuid
}

func (r *embedStateService) GetState(ctx context.Context, db string) (*schema.ImmutableState, error) {
	r.Lock()
	defer r.Unlock()

	state, err := r.cache.Get(r.serverUUID, db)
	if err == nil {
		return state, nil
	}
	if err != cache.ErrPrevStateNotFound {
		return nil, err
	}

	state, err = r.stateProvider.CurrentState(ctx)
	if err != nil {
		return nil, err
	}

	if err = r.cache.Set(r.serverUUID, db, state); err != nil {
		return nil, err
	}

	return state, nil
}

func (r *embedStateService) SetState(db string, state *schema.ImmutableState) error {
	r.Lock()
	defer r.Unlock()

	return r.cache.Set(r.serverUUID, db, state)
}

func (r *embedStateService) CacheLock() error {
	return fmt.Errorf("not implemented")
}

func (r *embedStateService) CacheUnlock() error {
	return fmt.Errorf("not implemented")
}
