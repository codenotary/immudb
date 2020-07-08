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

package cache

import (
	"fmt"
	"sync"

	"github.com/codenotary/immudb/pkg/api/schema"
)

type inMemoryCache struct {
	roots map[string]map[string]*schema.Root
	lock  *sync.RWMutex
}

// NewInMemoryCache returns a new in-memory cache
func NewInMemoryCache() Cache {
	return &inMemoryCache{
		roots: map[string]map[string]*schema.Root{},
		lock:  new(sync.RWMutex),
	}
}

func (imc *inMemoryCache) Get(serverUUID string, databasename string) (*schema.Root, error) {
	serverRoots, ok := imc.roots[serverUUID]
	if !ok {
		return nil, fmt.Errorf("no roots found for server %s", serverUUID)
	}
	root, ok := serverRoots[databasename]
	if !ok {
		return nil, fmt.Errorf(
			"no root found for server %s and database %s", serverUUID, databasename)
	}
	return root, nil
}

func (imc *inMemoryCache) Set(root *schema.Root, serverUUID string, databasename string) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()
	if _, ok := imc.roots[serverUUID]; !ok {
		imc.roots[serverUUID] = map[string]*schema.Root{databasename: root}
		return nil
	}
	imc.roots[serverUUID][databasename] = root
	return nil
}
