// +build streams
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

package client

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_StreamTxs(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)

	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.StreamTxs(ctx, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = client.Set(ctx, []byte("key1"), []byte("value1"))
	require.NoError(t, err)

	txStream, err := client.StreamTxs(ctx, &schema.TxRequest{Tx: 1})
	require.NoError(t, err)

	tx, err := txStream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), tx.Metadata.Id)

	go func() {
		time.Sleep(1 * time.Second)
		client.Set(ctx, []byte("key2"), []byte("value2"))
	}()

	tx, err = txStream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(2), tx.Metadata.Id)

	err = client.Logout(ctx)
	require.NoError(t, err)
}
