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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_SQL(t *testing.T) {
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

	_, err = client.SQLExec(ctx, `CREATE TABLE table1(
		id INTEGER, 
		title VARCHAR, 
		active BOOLEAN, 
		payload BLOB, 
		PRIMARY KEY id
		);`, nil)
	require.NoError(t, err)

	params := make(map[string]interface{})
	params["id"] = 1
	params["title"] = "title1"
	params["active"] = true
	params["payload"] = []byte{1, 2, 3}

	_, err = client.SQLExec(ctx, "INSERT INTO table1(id, title, active, payload) VALUES (@id, @title, @active, @payload), (2, 'title2', false, NULL), (3, NULL, NULL, x'AED0393F')", params)
	require.NoError(t, err)

	res, err := client.SQLQuery(ctx, "SELECT t.id as id, title FROM (table1 as t) WHERE id <= 3 AND active = @active", params, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	_, err = client.SQLQuery(ctx, "SELECT id as uuid FROM table1", nil, true)
	require.NoError(t, err)

	for _, row := range res.Rows {
		err := client.VerifyRow(ctx, row, "table1", row.Values[0])
		require.Equal(t, sql.ErrColumnDoesNotExist, err)
	}

	res, err = client.SQLQuery(ctx, "SELECT id, title, active, payload FROM table1 WHERE id <= 3 AND active = @active", params, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	for _, row := range res.Rows {
		err := client.VerifyRow(ctx, row, "table1", row.Values[0])
		require.NoError(t, err)

		row.Values[1].Value = &schema.SQLValue_S{S: "tampered title"}

		err = client.VerifyRow(ctx, row, "table1", row.Values[0])
		require.Equal(t, sql.ErrCorruptedData, err)
	}

	res, err = client.SQLQuery(ctx, "SELECT id, active FROM table1", nil, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	for _, row := range res.Rows {
		err := client.VerifyRow(ctx, row, "table1", row.Values[0])
		require.NoError(t, err)
	}

	res, err = client.SQLQuery(ctx, "SELECT active FROM table1 WHERE id = 1", nil, true)
	require.NoError(t, err)
	require.NotNil(t, res)

	for _, row := range res.Rows {
		err := client.VerifyRow(ctx, row, "table1", &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}})
		require.NoError(t, err)
	}
}
