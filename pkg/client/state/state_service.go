/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package state

import (
	"context"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/logger"
)

// StateService the root service interface
type StateService interface {
	GetState(ctx context.Context, db string) (*schema.ImmutableState, error)
	SetState(db string, state *schema.ImmutableState) error
}

type stateService struct {
	stateProvider StateProvider
	uuidProvider  UUIDProvider
	cache         cache.Cache
	serverUUID    string
	logger        logger.Logger
	sync.RWMutex
}

// NewStateService ...
func NewStateService(cache cache.Cache,
	logger logger.Logger,
	stateProvider StateProvider,
	uuidProvider UUIDProvider) (StateService, error) {

	serverUuid, err := uuidProvider.CurrentUUID(context.Background())
	if err != nil {
		if err != ErrNoServerUuid {
			return nil, err
		}
		logger.Warningf(err.Error())
	}

	return &stateService{
		stateProvider: stateProvider,
		uuidProvider:  uuidProvider,
		cache:         cache,
		logger:        logger,
		serverUUID:    serverUuid,
	}, nil
}

func (r *stateService) GetState(ctx context.Context, db string) (*schema.ImmutableState, error) {
	defer r.Unlock()
	r.Lock()

	if state, err := r.cache.Get(r.serverUUID, db); err == nil {
		return state, nil
	}

	if state, err := r.stateProvider.CurrentState(ctx); err != nil {
		return nil, err
	} else {
		if err := r.cache.Set(r.serverUUID, db, state); err != nil {
			return nil, err
		}
		return state, nil
	}
}

func (r *stateService) SetState(db string, state *schema.ImmutableState) error {
	defer r.Unlock()
	r.Lock()

	return r.cache.Set(r.serverUUID, db, state)
}
