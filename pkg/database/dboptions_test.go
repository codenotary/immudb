/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package database

import (
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	op := DefaultOption().AsReplica(true)

	require.Equal(t, op.GetDBRootPath(), DefaultOption().dbRootPath)
	require.False(t, op.GetCorruptionChecker())
	require.Equal(t, op.GetTxPoolSize(), DefaultOption().readTxPoolSize)
	require.False(t, op.syncReplication)

	rootpath := "rootpath"
	storeOpts := store.DefaultOptions()

	op = DefaultOption().
		WithDBRootPath(rootpath).
		WithCorruptionChecker(true).
		WithStoreOptions(storeOpts).
		WithReadTxPoolSize(789).
		WithSyncReplication(true)

	require.Equal(t, op.GetDBRootPath(), rootpath)
	require.True(t, op.GetCorruptionChecker())
	require.Equal(t, op.GetTxPoolSize(), 789)
	require.True(t, op.syncReplication)

	require.Equal(t, storeOpts, op.storeOpts)
}
