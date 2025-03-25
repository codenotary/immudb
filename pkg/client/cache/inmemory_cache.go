/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	serverUUID string
	states     map[string]map[string]*schema.ImmutableState
	identities map[string]string
	lock       sync.RWMutex
}

// NewInMemoryCache returns a new in-memory cache
func NewInMemoryCache() Cache {
	return &inMemoryCache{
		states:     map[string]map[string]*schema.ImmutableState{},
		identities: map[string]string{},
	}
}

func (imc *inMemoryCache) Get(serverUUID, db string) (*schema.ImmutableState, error) {
	serverStates, ok := imc.states[serverUUID]
	if !ok {
		return nil, fmt.Errorf("no roots found for server %s", serverUUID)
	}
	state, ok := serverStates[db]
	if !ok {
		return nil, fmt.Errorf(
			"no state found for server %s and database %s", serverUUID, db)
	}
	return state, nil
}

func (imc *inMemoryCache) Set(serverUUID, db string, state *schema.ImmutableState) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()
	if _, ok := imc.states[serverUUID]; !ok {
		imc.states[serverUUID] = map[string]*schema.ImmutableState{db: state}
		return nil
	}
	imc.states[serverUUID][db] = state
	return nil
}

func (imc *inMemoryCache) Lock(serverUUID string) (err error) {
	return ErrNotImplemented
}

func (imc *inMemoryCache) Unlock() (err error) {
	return ErrNotImplemented
}

func (imc *inMemoryCache) ServerIdentityCheck(serverIdentity, serverUUID string) error {
	imc.lock.Lock()
	defer imc.lock.Unlock()

	if previousUUID, ok := imc.identities[serverIdentity]; ok {
		// Server with this identity was seen before, ensure it did not change
		if previousUUID != serverUUID {
			return ErrServerIdentityValidationFailed
		}
		return nil
	}

	imc.identities[serverIdentity] = serverUUID
	return nil
}
