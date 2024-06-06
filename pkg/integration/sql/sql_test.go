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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
)

func TestImmuClient_SQL(t *testing.T) {
	options := server.DefaultOptions().
		WithDir(t.TempDir()).
		WithAuth(true).
		WithSigningKey("./../../../test/signer/ec1.key")

	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	client, err := bs.NewAuthenticatedClient(ic.
		DefaultOptions().
		WithDir(t.TempDir()).
		WithServerSigningPubKey("./../../../test/signer/ec1.pub"),
	)
	require.NoError(t, err)
	defer client.CloseSession(context.Background())

	ctx := context.Background()

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
			require.ErrorIs(t, err, sql.ErrColumnDoesNotExist)
		}

		for i := len(res.Rows); i > 0; i-- {
			row := res.Rows[i-1]
			err := client.VerifyRow(ctx, row, "table1", []*schema.SQLValue{row.Values[0]})
			require.ErrorIs(t, err, sql.ErrColumnDoesNotExist)
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
			require.ErrorIs(t, err, sql.ErrCorruptedData)
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

		t.Run("add column", func(t *testing.T) {
			_, err := client.SQLExec(ctx, `
				ALTER TABLE table1 ADD COLUMN id INTEGER
				`, nil)
			require.NoError(t, err)

			_, err = client.SQLExec(ctx, `
				INSERT INTO table1(id2, id, active, title2) VALUES(4, 44, false, 'new row')
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

		t.Run("drop column", func(t *testing.T) {
			_, err := client.SQLExec(ctx, `
				ALTER TABLE table1 DROP COLUMN id
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
	})
}

func TestImmuClient_SQLQueryReader(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir()).WithMaxResultSize(2)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	ctx := context.Background()

	client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(t.TempDir()))
	require.NoError(t, err)
	defer client.CloseSession(ctx)

	_, err = client.SQLExec(ctx, `
		CREATE TABLE test_table (
			id INTEGER AUTO_INCREMENT,
			value INTEGER,
			data JSON,

			PRIMARY KEY (id)
		);
	`, nil)
	require.NoError(t, err)

	for n := 0; n < 10; n++ {
		name := fmt.Sprintf("name%d", n)

		_, err := client.SQLExec(
			ctx,
			"INSERT INTO test_table(value, data) VALUES (@value, @data)",
			map[string]interface{}{
				"value": n + 10,
				"data":  fmt.Sprintf(`{"name": "%s"}`, name),
			})
		require.NoError(t, err)
	}

	reader, err := client.SQLQueryReader(ctx, "SELECT * FROM test_table WHERE value < 0", nil)
	require.NoError(t, err)

	_, err = reader.Read()
	require.Error(t, err)

	require.False(t, reader.Next())
	_, err = reader.Read()
	require.ErrorIs(t, err, sql.ErrNoMoreRows)

	_, err = client.SQLQuery(ctx, "SELECT * FROM test_table", nil, false)
	require.ErrorContains(t, err, database.ErrResultSizeLimitReached.Error())

	reader, err = client.SQLQueryReader(ctx, "SELECT * FROM test_table", nil)
	require.NoError(t, err)

	cols := reader.Columns()
	require.Equal(t, cols[0].Name, "(test_table.id)")
	require.Equal(t, cols[0].Type, sql.IntegerType)
	require.Equal(t, cols[1].Name, "(test_table.value)")
	require.Equal(t, cols[1].Type, sql.IntegerType)
	require.Equal(t, cols[2].Name, "(test_table.data)")
	require.Equal(t, cols[2].Type, sql.JSONType)

	n := 0
	for reader.Next() {
		row, err := reader.Read()
		require.NoError(t, err)
		require.Len(t, row, 3)

		name := fmt.Sprintf("name%d", n)

		var data interface{}
		err = json.Unmarshal([]byte(row[2].(string)), &data)
		require.NoError(t, err)

		require.Equal(t, int64(n+1), row[0])
		require.Equal(t, int64(n+10), row[1])
		require.Equal(t, map[string]interface{}{"name": name}, data)
		n++
	}

	require.Equal(t, n, 10)
	require.NoError(t, reader.Close())
	require.ErrorIs(t, reader.Close(), sql.ErrAlreadyClosed)

	reader, err = client.SQLQueryReader(ctx, "SELECT * FROM test_table", nil)
	require.NoError(t, err)

	require.True(t, reader.Next())
	require.NoError(t, reader.Close())
	require.False(t, reader.Next())

	row, err := reader.Read()
	require.Nil(t, row)
	require.ErrorIs(t, err, sql.ErrAlreadyClosed)
}

func TestImmuClient_SQL_UserStmts(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(t.TempDir()))
	require.NoError(t, err)
	defer client.CloseSession(context.Background())

	users, err := client.SQLQuery(context.Background(), "SHOW USERS", nil, true)
	require.NoError(t, err)
	require.Len(t, users.Rows, 1)

	_, err = client.SQLExec(context.Background(), "CREATE USER user1 WITH PASSWORD 'user1Password!' READWRITE", nil)
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "CREATE USER user2 WITH PASSWORD 'user2Password!' READ", nil)
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "CREATE USER user3 WITH PASSWORD 'user3Password!' ADMIN", nil)
	require.NoError(t, err)

	users, err = client.SQLQuery(context.Background(), "SHOW USERS", nil, true)
	require.NoError(t, err)
	require.Len(t, users.Rows, 4)

	user1Client := bs.NewClient(ic.DefaultOptions())

	err = user1Client.OpenSession(
		context.Background(),
		[]byte("user1"),
		[]byte("user1Password!"),
		"defaultdb",
	)
	require.NoError(t, err)

	err = user1Client.CloseSession(context.Background())
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "ALTER USER user1 WITH PASSWORD 'user1Password!!' READ", nil)
	require.NoError(t, err)

	err = user1Client.OpenSession(
		context.Background(),
		[]byte("user1"),
		[]byte("user1Password!"),
		"defaultdb",
	)
	require.ErrorContains(t, err, "invalid user name or password")

	err = user1Client.OpenSession(
		context.Background(),
		[]byte("user1"),
		[]byte("user1Password!!"),
		"defaultdb",
	)
	require.NoError(t, err)

	err = user1Client.CloseSession(context.Background())
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "ALTER USER user1 WITH PASSWORD 'user1Password!!' READWRITE", nil)
	require.NoError(t, err)

	users, err = client.SQLQuery(context.Background(), "SHOW USERS", nil, true)
	require.NoError(t, err)
	require.Len(t, users.Rows, 4)

	_, err = client.SQLExec(context.Background(), "DROP USER user1", nil)
	require.NoError(t, err)

	users, err = client.SQLQuery(context.Background(), "SHOW USERS", nil, true)
	require.NoError(t, err)
	require.Len(t, users.Rows, 3)

	err = user1Client.OpenSession(
		context.Background(),
		[]byte("user1"),
		[]byte("user1Password!!"),
		"defaultdb",
	)
	require.ErrorContains(t, err, "user is not active")

	_, err = client.SQLExec(context.Background(), "CREATE USER user2 WITH PASSWORD 'user2Password!' READWRITE", nil)
	require.ErrorContains(t, err, "user already exists")

	_, err = client.SQLExec(context.Background(), "ALTER USER user4 WITH PASSWORD 'user4Password!!' READWRITE", nil)
	require.ErrorContains(t, err, "not found")

	_, err = user1Client.SQLExec(context.Background(), "SHOW USERS", nil)
	require.ErrorContains(t, err, "not connected")

	_, err = user1Client.SQLExec(context.Background(), "CREATE USER user4 WITH PASSWORD 'user4Password!' READWRITE", nil)
	require.ErrorContains(t, err, "not connected")

	_, err = user1Client.SQLExec(context.Background(), "ALTER USER user4 WITH PASSWORD 'user4Password!!' READWRITE", nil)
	require.ErrorContains(t, err, "not connected")

	_, err = user1Client.SQLExec(context.Background(), "DROP USER user3", nil)
	require.ErrorContains(t, err, "not connected")
}

func TestImmuClient_SQL_Errors(t *testing.T) {
	options := server.DefaultOptions().WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(t.TempDir()))
	require.NoError(t, err)
	defer client.CloseSession(context.Background())

	_, err = client.SQLExec(context.Background(), "", map[string]interface{}{
		"param1": struct{}{},
	})
	require.ErrorIs(t, err, sql.ErrInvalidValue)

	_, err = client.SQLQuery(context.Background(), "", map[string]interface{}{
		"param1": struct{}{},
	}, false)
	require.ErrorIs(t, err, sql.ErrInvalidValue)

	err = client.VerifyRow(context.Background(), &schema.Row{
		Columns: []string{"col1"},
		Values:  []*schema.SQLValue{},
	}, "table1", []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}})
	require.ErrorIs(t, err, sql.ErrCorruptedData)

	err = client.VerifyRow(context.Background(), nil, "", nil)
	require.ErrorIs(t, err, ic.ErrIllegalArguments)

	t.Run("sql operations should fail with a cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := client.SQLExec(ctx, "BEGIN TRANSACTION; COMMIT;", nil)
		require.Contains(t, err.Error(), context.Canceled.Error())

		_, err = client.SQLQuery(ctx, "SELECT * FROM table1", nil, true)
		require.Contains(t, err.Error(), context.Canceled.Error())
	})

	err = client.Disconnect()
	require.NoError(t, err)

	_, err = client.SQLExec(context.Background(), "", nil)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.SQLQuery(context.Background(), "", nil, false)
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.ListTables(context.Background())
	require.ErrorIs(t, err, ic.ErrNotConnected)

	_, err = client.DescribeTable(context.Background(), "")
	require.ErrorIs(t, err, ic.ErrNotConnected)

	err = client.VerifyRow(context.Background(), &schema.Row{
		Columns: []string{"col1"},
		Values:  []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}},
	}, "table1", []*schema.SQLValue{{Value: &schema.SQLValue_N{N: 1}}})
	require.ErrorIs(t, err, ic.ErrNotConnected)
}

func TestQueryTxMetadata(t *testing.T) {
	options := server.DefaultOptions().
		WithDir(t.TempDir()).
		WithLogRequestMetadata(true)

	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	client, err := bs.NewAuthenticatedClient(ic.DefaultOptions().WithDir(t.TempDir()))
	require.NoError(t, err)
	defer client.CloseSession(context.Background())

	_, err = client.SQLExec(
		context.Background(),
		`CREATE TABLE mytable(
			id INTEGER,
			data JSON,

			PRIMARY KEY (id)
		)`,
		nil,
	)
	require.NoError(t, err)

	_, err = client.SQLExec(
		context.Background(),
		`INSERT INTO mytable(id, data) VALUES (1, '{"name": "John Doe"}')`,
		nil,
	)
	require.NoError(t, err)

	it, err := client.SQLQueryReader(
		context.Background(),
		"SELECT _tx_metadata FROM mytable",
		nil,
	)
	require.NoError(t, err)

	require.True(t, it.Next())

	row, err := it.Read()
	require.NoError(t, err)

	var md map[string]interface{}
	err = json.Unmarshal([]byte(row[0].(string)), &md)
	require.NoError(t, err)

	require.Equal(
		t,
		map[string]interface{}{"usr": "immudb", "ip": "bufconn"},
		md,
	)
}
