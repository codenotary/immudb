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
	"errors"

	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrCacheNotLocked = errors.New("cache is not locked")
var ErrCacheAlreadyLocked = errors.New("cache is already locked")
var ErrServerIdentityValidationFailed = errors.New("failed to validate the identity of the server")

// Cache the cache interface
type Cache interface {
	Get(serverUUID, db string) (*schema.ImmutableState, error)
	Set(serverUUID, db string, state *schema.ImmutableState) error
	Lock(serverUUID string) error
	Unlock() error

	// ServerIdentityCheck check validates that a server with given identity can use given server uuid
	//
	// `serverIdentity` must uniquely identify given immudb server instance.
	// Go SDK passes `host:port` pair as the server identity however the Cache interface implementation
	// must not do any assumptions about the structure of this data.
	ServerIdentityCheck(serverIdentity, serverUUID string) error
}

// HistoryCache the history cache interface
type HistoryCache interface {
	Cache
	Walk(serverUUID string, db string, f func(*schema.ImmutableState) interface{}) ([]interface{}, error)
}
