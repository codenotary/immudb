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
	"context"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
)

func TestReplication(t *testing.T) {
	path := t.TempDir()

	rOpts := DefaultOptions().
		WithPrimaryDatabase("defaultdb").
		WithPrimaryHost("127.0.0.1").
		WithPrimaryPort(3322).
		WithPrimaryUsername("immudb").
		WithPrimaryPassword("immudb").
		WithStreamChunkSize(DefaultChunkSize)

	logger := logger.NewSimpleLogger("logger", os.Stdout)

	db, err := database.NewDB("replicated_defaultdb", nil, database.DefaultOptions().AsReplica(true).WithDBRootPath(path), logger)
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

func TestReplicationIsAbortedOnServerVersionMismatch(t *testing.T) {
	path := t.TempDir()

	clientMock := &immuClientMock{}

	rOpts := DefaultOptions().
		WithPrimaryDatabase("defaultdb").
		WithPrimaryHost("127.0.0.1").
		WithPrimaryPort(3322).
		WithPrimaryUsername("immudb").
		WithPrimaryPassword("immudb").
		WithStreamChunkSize(DefaultChunkSize).
		WithClientFactoryFunc(func(s string, i int) client.ImmuClient {
			return &immuClientMock{}
		})

	logger := logger.NewSimpleLogger("logger", os.Stdout)

	db, err := database.NewDB("replicated_defaultdb", nil, database.DefaultOptions().AsReplica(true).WithDBRootPath(path), logger)
	require.NoError(t, err)

	txReplicator, err := NewTxReplicator(xid.New(), db, rOpts, logger)
	txReplicator.client = clientMock
	require.NoError(t, err)

	err = txReplicator.Start()
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 10) // make sure replication stopped

	err = txReplicator.Stop()
	require.ErrorIs(t, err, ErrAlreadyStopped)
	require.ErrorIs(t, txReplicator.Error(), ErrPrimaryServerVersionMismatch)
}

type immuClientMock struct {
	client.ImmuClient
}

func (c *immuClientMock) OpenSession(ctx context.Context, user []byte, pass []byte, database string) (err error) {
	return nil
}

func (c *immuClientMock) ServerInfo(ctx context.Context, req *schema.ServerInfoRequest) (*schema.ServerInfoResponse, error) {
	return &schema.ServerInfoResponse{
		Version: "test",
	}, nil
}

func (c *immuClientMock) CloseSession(ctx context.Context) error {
	return nil
}
