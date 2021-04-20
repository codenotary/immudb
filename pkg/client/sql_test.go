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

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_SQL(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)
	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.SQLExec(ctx, "CREATE TABLE table1(id INTEGER, title VARCHAR, active BOOLEAN, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	_, err = client.SQLExec(ctx, "UPSERT INTO table1(id, title, active) VALUES (1, 'title1', true), (2, 'title2', false), (3, 'title3', NULL)", nil)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["active"] = nil

	res, err := client.SQLQuery(ctx, "SELECT t.id as d FROM (table1 as t) WHERE id <= 3 AND active = @active", params)
	require.NoError(t, err)
	require.NotNil(t, res)
}
