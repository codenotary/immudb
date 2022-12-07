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

package integration

import (
	"context"
	"io"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	client, err := ic.NewImmuClient(cliOpts)
	require.NoError(t, err)

	lr, err := client.Login(context.Background(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	err = client.CreateDatabase(ctx, &schema.DatabaseSettings{
		DatabaseName:    "replicateddb",
		Replica:         true,
		PrimaryDatabase: "defaultdb",
	})
	require.NoError(t, err)

	replicatedMD, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: "replicateddb"})
	require.NoError(t, err)

	defaultMD, err := client.UseDatabase(ctx, &schema.Database{DatabaseName: "defaultdb"})
	require.NoError(t, err)

	md = metadata.Pairs("authorization", defaultMD.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.ExportTx(ctx, nil)
	require.ErrorIs(t, err, ic.ErrIllegalArguments)

	txmd, err := client.Set(ctx, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	rmd := metadata.Pairs("authorization", replicatedMD.Token)
	rctx := metadata.NewOutgoingContext(context.Background(), rmd)

	for i := uint64(1); i <= 2; i++ {
		exportTxStream, err := client.ExportTx(ctx, &schema.ExportTxRequest{Tx: i})
		require.NoError(t, err)

		replicateTxStream, err := client.ReplicateTx(rctx)
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

		rtxmd, err := replicateTxStream.CloseAndRecv()
		require.NoError(t, err)
		require.Equal(t, i, rtxmd.Id)
	}

	replicatedEntry, err := client.GetAt(rctx, []byte("key1"), txmd.Id)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), replicatedEntry.Value)
	require.Equal(t, txmd.Id, replicatedEntry.Tx)

	err = client.Logout(rctx)
	require.NoError(t, err)

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.ExportTx(ctx, &schema.ExportTxRequest{Tx: 1})
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ReplicateTx(rctx)
	require.ErrorIs(t, err, ic.ErrNotConnected)
}
