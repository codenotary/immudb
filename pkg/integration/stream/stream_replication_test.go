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

package integration

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_ExportAndReplicateTx(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	cliOpts := ic.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())})

	client := ic.NewClient().WithOptions(cliOpts)

	err := client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	defer client.CloseSession(context.Background())

	_, err = client.ExportTx(context.Background(), nil)
	require.Equal(t, ic.ErrIllegalArguments, err)

	hdr, err := client.Set(context.Background(), []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	_, err = client.CreateDatabaseV2(context.Background(), "replicateddb", &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica: &schema.NullableBool{Value: true},
		},
	})
	require.NoError(t, err)

	rclient := ic.NewClient().WithOptions(cliOpts)

	err = rclient.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "replicateddb")
	require.NoError(t, err)

	defer rclient.CloseSession(context.Background())

	rmd := metadata.Pairs(
		"skip-integrity-check", strconv.FormatBool(true),
		"wait-for-indexing", strconv.FormatBool(false),
	)
	rctx := metadata.NewOutgoingContext(context.Background(), rmd)

	for i := uint64(1); i <= hdr.Id; i++ {
		exportTxStream, err := client.ExportTx(context.Background(), &schema.ExportTxRequest{
			Tx:                 i,
			SkipIntegrityCheck: true,
		})
		require.NoError(t, err)

		replicateTxStream, err := rclient.ReplicateTx(rctx)
		require.NoError(t, err)

		for {
			txChunk, err := exportTxStream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			err = replicateTxStream.Send(txChunk)
			require.NoError(t, err)
		}

		hdr, err := replicateTxStream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, i, hdr.Id)
	}

	replicatedEntry, err := rclient.GetAt(rctx, []byte("key1"), hdr.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), replicatedEntry.Value)
	require.Equal(t, hdr.Id, replicatedEntry.Tx)

	err = client.CloseSession(context.Background())
	require.NoError(t, err)

	_, err = client.ExportTx(context.Background(), &schema.ExportTxRequest{Tx: 1})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = rclient.CloseSession(context.Background())
	require.NoError(t, err)

	_, err = rclient.ReplicateTx(rctx)
	require.ErrorIs(t, err, ic.ErrNotConnected)
}
