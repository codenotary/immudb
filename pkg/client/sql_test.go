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
	"errors"
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

	res, err = client.ListTables(ctx)
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Len(t, res.Rows, 1)
	require.Len(t, res.Columns, 1)
	require.Equal(t, "VARCHAR", res.Columns[0].Type)
	require.Equal(t, "TABLE", res.Columns[0].Name)
	require.Equal(t, "table1", res.Rows[0].Values[0].GetS())

	res, err = client.DescribeTable(ctx, "table1")
	require.NoError(t, err)
	require.NotNil(t, res)

	require.Equal(t, "COLUMN", res.Columns[0].Name)
	require.Equal(t, "VARCHAR", res.Columns[0].Type)

	colsCheck := map[string]bool{
		"id": false, "title": false, "active": false, "payload": false,
	}
	require.Len(t, res.Rows, len(colsCheck))
	for _, row := range res.Rows {
		colsCheck[row.Values[0].GetS()] = true
	}
	for c, found := range colsCheck {
		require.True(t, found, c)
	}

	tx2, err := client.SQLExec(ctx, `
		UPSERT INTO table1(id, title, active, payload)
		VALUES (2, 'title2-updated', false, NULL)
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, tx2)

	res, err = client.SQLQuery(ctx, "SELECT title FROM table1 WHERE id=2", nil, true)
	require.NoError(t, err)
	require.Equal(t, "title2-updated", res.Rows[0].Values[0].GetS())

	err = client.UseSnapshot(ctx, 0, tx2.Dtxs[0].Id)
	require.NoError(t, err)

	res, err = client.SQLQuery(ctx, "SELECT title FROM table1 WHERE id=2", nil, true)
	require.NoError(t, err)
	require.Equal(t, "title2", res.Rows[0].Values[0].GetS())
}

func TestImmuClient_SQL_Errors(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client, err := NewImmuClient(DefaultOptions().WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "", map[string]interface{}{
		"param1": struct{}{},
	})
	require.True(t, errors.Is(err, sql.ErrInvalidValue))

	_, err = client.SQLQuery(context.Background(), "", map[string]interface{}{
		"param1": struct{}{},
	}, false)
	require.True(t, errors.Is(err, sql.ErrInvalidValue))

	err = client.VerifyRow(context.Background(), &schema.Row{
		Columns: []string{"col1"},
		Values:  []*schema.SQLValue{},
	}, "table1", &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	err = client.VerifyRow(context.Background(), nil, "", nil)
	require.True(t, errors.Is(err, ErrIllegalArguments))

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "", nil)
	require.True(t, errors.Is(err, ErrNotConnected))

	err = client.UseSnapshot(context.Background(), 1, 2)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.SQLQuery(context.Background(), "", nil, false)
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.ListTables(context.Background())
	require.True(t, errors.Is(err, ErrNotConnected))

	_, err = client.DescribeTable(context.Background(), "")
	require.True(t, errors.Is(err, ErrNotConnected))

	err = client.VerifyRow(context.Background(), &schema.Row{
		Columns: []string{"col1"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
	}, "table1", &schema.SQLValue{Value: &schema.SQLValue_N{N: 1}})
	require.True(t, errors.Is(err, ErrNotConnected))
}

func TestDecodeRowErrors(t *testing.T) {

	type tMap map[uint64]sql.SQLValueType

	for _, d := range []struct {
		n        string
		data     []byte
		colTypes map[uint64]sql.SQLValueType
	}{
		{
			"No data",
			nil,
			nil,
		},
		{
			"Short buffer",
			[]byte{1},
			tMap{},
		},
		{
			"Short buffer on type",
			[]byte{0, 0, 0, 1, 0, 0, 1},
			tMap{},
		},
		{
			"Missing type",
			[]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1},
			tMap{},
		},
		{
			"Invalid value",
			[]byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0},
			tMap{
				1: sql.VarcharType,
			},
		},
	} {
		t.Run(d.n, func(t *testing.T) {
			row, err := decodeRow(d.data, d.colTypes)
			require.True(t, errors.Is(err, sql.ErrCorruptedData))
			require.Nil(t, row)
		})
	}
}

func TestVerifyAgainst(t *testing.T) {

	// Missing column type
	err := verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values:  []*schema.SQLValue{{Value: nil}},
	}, map[uint64]*schema.SQLValue{}, map[string]uint64{})
	require.True(t, errors.Is(err, sql.ErrColumnDoesNotExist))

	// Nil value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values:  []*schema.SQLValue{{Value: nil}},
	}, map[uint64]*schema.SQLValue{}, map[string]uint64{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Missing decoded value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint64]*schema.SQLValue{}, map[string]uint64{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Invalid decoded value
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint64]*schema.SQLValue{
		0: {Value: nil},
	}, map[string]uint64{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Not comparable types
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint64]*schema.SQLValue{
		0: {Value: &schema.SQLValue_S{S: "1"}},
	}, map[string]uint64{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrNotComparableValues))

	// Different values
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint64]*schema.SQLValue{
		0: {Value: &schema.SQLValue_N{N: 2}},
	}, map[string]uint64{
		"c1": 0,
	})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	// Successful verify
	err = verifyRowAgainst(&schema.Row{
		Columns: []string{"c1"},
		Values: []*schema.SQLValue{
			{Value: &schema.SQLValue_N{N: 1}},
		},
	}, map[uint64]*schema.SQLValue{
		0: {Value: &schema.SQLValue_N{N: 1}},
	}, map[string]uint64{
		"c1": 0,
	})
	require.NoError(t, err)
}
