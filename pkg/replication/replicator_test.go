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

package replication

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	path := t.TempDir()

	_, err := NewTxReplicator(xid.New(), nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	rOpts := DefaultOptions().
		WithPrimaryDatabase("defaultdb").
		WithPrimaryHost("127.0.0.1").
		WithPrimaryPort(3322).
		WithPrimaryUsername("immudb").
		WithPrimaryPassword("immudb").
		WithStreamChunkSize(DefaultChunkSize)

	logger := logger.NewSimpleLogger("logger", os.Stdout)

	db, err := database.NewDB("replicated_defaultdb", nil, database.DefaultOption().AsReplica(true).WithDBRootPath(path), logger)
	require.NoError(t, err)

	txReplicator, err := NewTxReplicator(xid.New(), db, rOpts, logger)
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
