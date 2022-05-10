/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestTransaction_SetAndGet(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	// tx mode
	tx, err := client.NewTx(context.TODO())
	require.NoError(t, err)
	err = tx.SQLExec(context.TODO(), `CREATE TABLE table1(
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

	err = tx.SQLExec(context.TODO(), "INSERT INTO table1(id, title, active, payload) VALUES (@id, @title, @active, @payload), (2, 'title2', false, NULL), (3, NULL, NULL, x'AED0393F')", params)
	require.NoError(t, err)

	res, err := tx.SQLQuery(context.TODO(), "SELECT t.id as id, title FROM table1 t WHERE id <= 3 AND active = @active", params)
	require.NoError(t, err)
	require.NotNil(t, res)

	txH, err := tx.Commit(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, txH)

	err = client.CloseSession(context.TODO())
	require.NoError(t, err)
}

func TestTransaction_Rollback(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	_, err = client.SQLExec(context.TODO(), "CREATE DATABASE db1;", nil)
	require.NoError(t, err)

	_, err = client.SQLExec(context.TODO(), "USE db1;", nil)
	require.NoError(t, err)

	res, err := client.SQLQuery(context.TODO(), "SELECT * FROM databases();", nil, true)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "defaultdb", res.Rows[0].Values[0].GetS())
	require.Equal(t, "db1", res.Rows[1].Values[0].GetS())

	tx, err := client.NewTx(context.TODO())
	require.NoError(t, err)

	err = tx.SQLExec(context.TODO(), `CREATE TABLE table1(
		id INTEGER,
		PRIMARY KEY id
		);`, nil)
	require.NoError(t, err)

	err = tx.Rollback(context.TODO())
	require.NoError(t, err)

	err = tx.Rollback(context.TODO())
	require.Error(t, err)
	require.Equal(t, "no transaction found", err.Error())

	tx1, err := client.NewTx(context.TODO())
	require.NoError(t, err)

	res, err = tx1.SQLQuery(context.TODO(), "SELECT * FROM table1", nil)
	require.Error(t, err)
	require.Equal(t, "table does not exist (table1)", err.Error())
	require.Nil(t, res)

	err = client.CloseSession(context.TODO())
	require.NoError(t, err)
}

func TestTransaction_MultipleReadWriteError(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	tx1, err := client.NewTx(context.TODO())
	require.NoError(t, err)

	tx2, err := client.NewTx(context.TODO())
	require.Error(t, err)
	require.Nil(t, tx2)

	_, err = tx1.Commit(context.TODO())
	require.NoError(t, err)
}

func TestTransaction_ChangingDBOnSessionNoError(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	txDefaultDB, err := client.NewTx(context.TODO())
	err = txDefaultDB.SQLExec(context.TODO(), `CREATE TABLE tableDefaultDB(id INTEGER,PRIMARY KEY id);`, nil)
	require.NoError(t, err)

	client2 := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))
	err = client2.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	err = client2.CreateDatabase(context.TODO(), &schema.DatabaseSettings{DatabaseName: "db2"})
	require.NoError(t, err)
	_, err = client2.UseDatabase(context.TODO(), &schema.Database{DatabaseName: "db2"})
	require.NoError(t, err)
	txDb2, err := client2.NewTx(context.TODO())
	require.NoError(t, err)
	err = txDb2.SQLExec(context.TODO(), `CREATE TABLE tableDB2(id INTEGER,PRIMARY KEY id);`, nil)
	require.NoError(t, err)
	err = txDb2.SQLExec(context.TODO(), "INSERT INTO tableDB2(id) VALUES (1)", nil)
	require.NoError(t, err)

	txh1, err := txDefaultDB.Commit(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, txh1.Header.Ts)

	txh2, err := txDb2.Commit(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, txh2.Header.Ts)

	_, err = client.UseDatabase(context.TODO(), &schema.Database{DatabaseName: "db2"})
	require.NoError(t, err)
	ris, err := client.SQLQuery(context.TODO(), `SELECT * FROM tableDB2;`, nil, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(ris.Rows))

	err = client.CloseSession(context.TODO())
	require.NoError(t, err)
	err = client2.CloseSession(context.TODO())
	require.NoError(t, err)
}

func TestTransaction_MultiNoErr(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	ctx := context.Background()

	err := client.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	tx, err := client.NewTx(ctx)
	require.NoError(t, err)

	err = tx.SQLExec(ctx, `
		CREATE TABLE IF NOT EXISTS balance(
			id INTEGER,
			balance INTEGER,
			PRIMARY KEY(id)
		)
		`, nil)
	require.NoError(t, err)

	err = tx.SQLExec(ctx, `
		UPSERT INTO balance(id, balance) VALUES(1,100),(2,1500)
		`, nil)
	require.NoError(t, err)

	_, err = tx.Commit(ctx)
	require.NoError(t, err)

	tx, err = client.NewTx(ctx)
	require.NoError(t, err)

	qr, err := tx.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	qr, err = client.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil, true)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	updateStmt := func(id, price int) (context.Context, string, map[string]interface{}) {
		return ctx,
			"UPDATE balance SET balance = balance - @price WHERE id = @id AND balance - @price >= 0",
			map[string]interface{}{
				"id":    id,
				"price": price,
			}
	}

	res, err := client.SQLExec(updateStmt(1, 10))
	require.NoError(t, err)
	require.EqualValues(t, res.Txs[0].UpdatedRows, 1)

	qr, err = tx.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	qr, err = client.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil, true)
	require.NoError(t, err)
	require.EqualValues(t, 90, qr.Rows[0].Values[0].GetN())

	err = tx.SQLExec(updateStmt(1, 10))
	require.NoError(t, err)
	_, err = tx.Commit(ctx)
	require.EqualError(t, err, "tx read conflict")
	require.Equal(t, err.(errors.ImmuError).Code(), errors.CodInFailedSqlTransaction)

	txn, err := client.NewTx(ctx)
	require.NoError(t, err)

	_, err = txn.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil)
	require.NoError(t, err)

	err = txn.Rollback(ctx)
	require.NoError(t, err)

	err = client.CloseSession(ctx)
	require.NoError(t, err)

	_, err = client.NewTx(ctx)
	require.Error(t, err)
}

func TestTransaction_HandlingReadConflict(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.NewClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	ctx := context.Background()

	err := client.OpenSession(ctx, []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)

	tx, err := client.NewTx(ctx)
	require.NoError(t, err)

	err = tx.SQLExec(ctx, `
		CREATE TABLE IF NOT EXISTS balance(
			id INTEGER,
			balance INTEGER,
			PRIMARY KEY(id)
		)
		`, nil)
	require.NoError(t, err)

	err = tx.SQLExec(ctx, `
		UPSERT INTO balance(id, balance) VALUES(1,100),(2,1500)
		`, nil)
	require.NoError(t, err)

	_, err = tx.Commit(ctx)
	require.NoError(t, err)

	tx, err = client.NewTx(ctx)
	require.NoError(t, err)

	qr, err := tx.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	qr, err = client.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil, true)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	updateStmt := func(id, price int) (context.Context, string, map[string]interface{}) {
		return ctx,
			"UPDATE balance SET balance = balance - @price WHERE id = @id AND balance - @price >= 0",
			map[string]interface{}{
				"id":    id,
				"price": price,
			}
	}

	res, err := client.SQLExec(updateStmt(1, 10))
	require.NoError(t, err)
	require.EqualValues(t, res.Txs[0].UpdatedRows, 1)

	qr, err = tx.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil)
	require.NoError(t, err)
	require.EqualValues(t, 100, qr.Rows[0].Values[0].GetN())

	qr, err = client.SQLQuery(ctx, "SELECT balance FROM balance WHERE id = 1", nil, true)
	require.NoError(t, err)
	require.EqualValues(t, 90, qr.Rows[0].Values[0].GetN())

	err = tx.SQLExec(updateStmt(1, 10))
	require.NoError(t, err)
	_, err = tx.Commit(ctx)
	require.EqualError(t, err, "tx read conflict")

	err = client.CloseSession(ctx)
	require.NoError(t, err)
}
