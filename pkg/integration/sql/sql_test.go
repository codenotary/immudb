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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestImmuClient_SQL(t *testing.T) {
	dir, err := ioutil.TempDir("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	defer os.Remove(".state-")

	options := server.DefaultOptions().
		WithDir(dir).
		WithAuth(true).
		WithSigningKey("./../../../test/signer/ec1.key")

	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	clientOpts := ic.DefaultOptions().
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}).
		WithServerSigningPubKey("./../../../test/signer/ec1.pub")

	client, err := ic.NewImmuClient(clientOpts)
	require.NoError(t, err)

	lr, err := client.Login(context.TODO(), []byte(`immudb`), []byte(`immudb`))
	require.NoError(t, err)

	md := metadata.Pairs("authorization", lr.Token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.SQLExec(ctx, `
		CREATE TABLE table1(
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

	t.Run("insert with params", func(t *testing.T) {
		_, err := client.SQLExec(ctx, `
			INSERT INTO table1(id, title, active, payload)
			VALUES
				(@id, @title, @active, @payload),
				(2, 'title2', false, NULL),
				(3, NULL, NULL, x'AED0393F')
			`, params)
		require.NoError(t, err)
	})

	t.Run("verify row", func(t *testing.T) {
		res, err := client.SQLQuery(ctx, `
			SELECT t.id as id, title
			FROM table1 t
			WHERE id <= 3 AND active = @active
			`, params, true)
		require.NoError(t, err)
		require.NotNil(t, res)

		for _, row := range res.Rows {
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.Equal(t, sql.ErrColumnDoesNotExist, err)
		}

		for i := len(res.Rows); i > 0; i-- {
			row := res.Rows[i-1]
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.Equal(t, sql.ErrColumnDoesNotExist, err)
		}

		res, err = client.SQLQuery(ctx, `
			SELECT id, title, active, payload
			FROM table1
			WHERE id <= 3 AND active = @active
			`, params, true)
		require.NoError(t, err)
		require.NotNil(t, res)

		for _, row := range res.Rows {
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.NoError(t, err)

			row.Values[1].Value = &schema.SQLValue_S{S: "tampered title"}

			err = client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.Equal(t, sql.ErrCorruptedData, err)
		}

		res, err = client.SQLQuery(ctx, "SELECT id, active FROM table1", nil, true)
		require.NoError(t, err)
		require.NotNil(t, res)

		for _, row := range res.Rows {
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.NoError(t, err)
		}

		res, err = client.SQLQuery(ctx, "SELECT active FROM table1 WHERE id = 1", nil, true)
		require.NoError(t, err)
		require.NotNil(t, res)

		for _, row := range res.Rows {
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}})
			require.NoError(t, err)
		}

	})

	t.Run("list tables", func(t *testing.T) {
		res, err := client.ListTables(ctx)
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
	})

	t.Run("upsert", func(t *testing.T) {
		tx2, err := client.SQLExec(ctx, `
			UPSERT INTO table1(id, title, active, payload)
			VALUES (2, 'title2-updated', false, NULL)
		`, nil)
		require.NoError(t, err)
		require.NotNil(t, tx2)

		res, err := client.SQLQuery(ctx, "SELECT title FROM table1 WHERE id=2", nil, true)
		require.NoError(t, err)
		require.Equal(t, "title2-updated", res.Rows[0].Values[0].GetS())

		res, err = client.SQLQuery(ctx, fmt.Sprintf(`
			SELECT title
			FROM table1 BEFORE TX %d
			WHERE id=2
			`, tx2.Txs[0].Header.Id), nil, true)
		require.NoError(t, err)
		require.Equal(t, "title2", res.Rows[0].Values[0].GetS())
	})

	t.Run("verify row after alter table", func(t *testing.T) {
		t.Run("not a primary key", func(t *testing.T) {
			_, err := client.SQLExec(ctx, `
				ALTER TABLE table1 RENAME COLUMN title TO title2
				`, nil)
			require.NoError(t, err)

			res, err := client.SQLQuery(ctx, "SELECT id, active, title2 FROM table1", nil, true)
			require.NoError(t, err)
			require.NotNil(t, res)

			for _, row := range res.Rows {
				err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
				require.NoError(t, err)
			}
		})

		t.Run("primary key", func(t *testing.T) {
			_, err := client.SQLExec(ctx, `
				ALTER TABLE table1 RENAME COLUMN id TO id2
				`, nil)
			require.NoError(t, err)

			res, err := client.SQLQuery(ctx, "SELECT id2, active, title2 FROM table1", nil, true)
			require.NoError(t, err)
			require.NotNil(t, res)

			for _, row := range res.Rows {
				err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
				require.NoError(t, err)
			}
		})

		t.Run("add column key", func(t *testing.T) {
			_, err := client.SQLExec(ctx, `
				ALTER TABLE table1 ADD COLUMN id INTEGER
				`, nil)
			require.NoError(t, err)

			res, err := client.SQLQuery(ctx, "SELECT id2, id, active, title2 FROM table1", nil, true)
			require.NoError(t, err)
			require.NotNil(t, res)

			for _, row := range res.Rows {
				err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
				require.NoError(t, err)
			}
		})
	})
}

func TestImmuClient_SQL_Errors(t *testing.T) {
	dir, err := ioutil.TempDir("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	defer os.Remove(".state-")

	options := server.DefaultOptions().
		WithDir(dir).
		WithAuth(true)

	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	cliOpts := ic.DefaultOptions().
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	client, err := ic.NewImmuClient(cliOpts)
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
	}, "table1", []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}})
	require.True(t, errors.Is(err, sql.ErrCorruptedData))

	err = client.VerifyRow(context.Background(), nil, "", nil)
	require.True(t, errors.Is(err, ic.ErrIllegalArguments))

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "", nil)
	require.True(t, errors.Is(err, ic.ErrNotConnected))

	_, err = client.SQLQuery(context.Background(), "", nil, false)
	require.True(t, errors.Is(err, ic.ErrNotConnected))

	_, err = client.ListTables(context.Background())
	require.True(t, errors.Is(err, ic.ErrNotConnected))

	_, err = client.DescribeTable(context.Background(), "")
	require.True(t, errors.Is(err, ic.ErrNotConnected))

	err = client.VerifyRow(context.Background(), &schema.Row{
		Columns: []string{"col1"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
	}, "table1", []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}})
	require.True(t, errors.Is(err, ic.ErrNotConnected))
}
