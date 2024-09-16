/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package database

import (
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	op := DefaultOptions().AsReplica(true)

	require.Equal(t, op.GetDBRootPath(), DefaultOptions().dbRootPath)
	require.Equal(t, op.GetTxPoolSize(), DefaultOptions().readTxPoolSize)
	require.False(t, op.syncReplication)

	rootpath := "rootpath"
	storeOpts := store.DefaultOptions()

	op = DefaultOptions().
		WithDBRootPath(rootpath).
		WithStoreOptions(storeOpts).
		WithReadTxPoolSize(789).
		WithSyncReplication(true).
		WithTruncationFrequency(1 * time.Hour)

	require.Equal(t, op.GetDBRootPath(), rootpath)
	require.Equal(t, op.GetTxPoolSize(), 789)
	require.True(t, op.syncReplication)
	require.Equal(t, op.TruncationFrequency, 1*time.Hour)

	require.Equal(t, storeOpts, op.storeOpts)
}
