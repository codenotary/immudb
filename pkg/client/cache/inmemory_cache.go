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
	states map[string]map[string]*schema.ImmutableState
	lock   *sync.RWMutex
}

// NewInMemoryCache returns a new in-memory cache
func NewInMemoryCache() Cache {
	return &inMemoryCache{
		states: map[string]map[string]*schema.ImmutableState{},
		lock:   new(sync.RWMutex),
	}
}

func (imc *inMemoryCache) Get(serverUUID string, dbName string) (*schema.ImmutableState, error) {
	serverStates, ok := imc.states[serverUUID]
	if !ok {
		return nil, fmt.Errorf("no roots found for server %s", serverUUID)
	}
	state, ok := serverStates[dbName]
	if !ok {
		return nil, fmt.Errorf(
			"no state found for server %s and database %s", serverUUID, dbName)
	}
	return state, nil
}

func (imc *inMemoryCache) Set(state *schema.ImmutableState, serverUUID string, dbName string) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()
	if _, ok := imc.states[serverUUID]; !ok {
		imc.states[serverUUID] = map[string]*schema.ImmutableState{dbName: state}
		return nil
	}
	imc.states[serverUUID][dbName] = state
	return nil
}
