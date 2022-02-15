/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package replication

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	_, err := NewTxReplicator(nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	rOpts := DefaultOptions().
		WithMasterDatabase("defaultdb").
		WithMasterAddress("127.0.0.1").
		WithMasterPort(3322).
		WithFollowerUsername("immudb").
		WithFollowerPassword("immudb").
		WithStreamChunkSize(DefaultChunkSize)

	logger := logger.NewSimpleLogger("logger", os.Stdout)

	db, err := database.NewDB("replicated_defaultdb", database.DefaultOption().AsReplica(true), logger)
	require.NoError(t, err)

	defer os.RemoveAll(db.GetOptions().GetDBRootPath())

	txReplicator, err := NewTxReplicator(db, rOpts, logger)
	require.NoError(t, err)

	err = txReplicator.Stop()
	require.ErrorIs(t, err, ErrAlreadyStopped)

	err = txReplicator.Start()
	require.NoError(t, err)

	err = txReplicator.Start()
	require.ErrorIs(t, err, ErrAlreadyRunning)

	err = txReplicator.Stop()
	require.NoError(t, err)
}
