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

package rootservice

import (
	"context"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/logger"
)

// RootService the root service interface
type RootService interface {
	GetRoot(ctx context.Context, databasename string) (*schema.Root, error)
	SetRoot(root *schema.Root, databasename string) error
}

type rootservice struct {
	rootProvider RootProvider
	uuidProvider UuidProvider
	cache        cache.Cache
	serverUuid   string
	logger       logger.Logger
	sync.RWMutex
}

// NewRootService ...
func NewRootService(cache cache.Cache,
	logger logger.Logger,
	rootProvider RootProvider,
	uuidProvider UuidProvider) RootService {

	serverUuid, err := uuidProvider.CurrentUuid(context.Background())
	if err != nil {
		if err != ErrNoServerUuid {
			return nil // TODO OGG: check with Michele if this was intended or a mistake
		}
		logger.Warningf(err.Error())
	}
	return &rootservice{
		rootProvider: rootProvider,
		uuidProvider: uuidProvider,
		cache:        cache,
		logger:       logger,
		serverUuid:   serverUuid,
	}
}

func (r *rootservice) GetRoot(ctx context.Context, databasename string) (*schema.Root, error) {
	defer r.Unlock()
	r.Lock()
	if root, err := r.cache.Get(r.serverUuid, databasename); err == nil {
		return root, nil
	}
	if root, err := r.rootProvider.CurrentRoot(ctx); err != nil {
		return nil, err
	} else {
		if err := r.cache.Set(root, r.serverUuid, databasename); err != nil {
			return nil, err
		}
		return root, nil
	}
}

func (r *rootservice) SetRoot(root *schema.Root, databasename string) error {
	defer r.Unlock()
	r.Lock()
	return r.cache.Set(root, r.serverUuid, databasename)
}
