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

package integration

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	immuErrors "github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"os"
	"testing"
)

func TestTransaction_SetAndGet(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.DefaultClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	serverUUID, sessionID, err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	require.NotNil(t, serverUUID)
	require.NotNil(t, sessionID)
	// tx mode
	tx, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)
	err = tx.TxSQLExec(context.TODO(), `CREATE TABLE table1(
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

	err = tx.TxSQLExec(context.TODO(), "INSERT INTO table1(id, title, active, payload) VALUES (@id, @title, @active, @payload), (2, 'title2', false, NULL), (3, NULL, NULL, x'AED0393F')", params)
	require.NoError(t, err)

	res, err := tx.TxSQLQuery(context.TODO(), "SELECT t.id as id, title FROM table1 t WHERE id <= 3 AND active = @active", params, true)
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

	client := ic.DefaultClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	_, _, err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")

	tx, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)
	err = tx.TxSQLExec(context.TODO(), `CREATE TABLE table1(
		id INTEGER,
		PRIMARY KEY id
		);`, nil)
	require.NoError(t, err)

	err = tx.Rollback(context.TODO())
	require.NoError(t, err)

	err = client.CloseSession(context.TODO())
	require.NoError(t, err)

	tx, err = client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)

	res, err := tx.TxSQLQuery(context.TODO(), "SELECT * FROM table1", nil, true)
	require.Error(t, err)
	require.NotNil(t, res)

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

	client := ic.DefaultClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	serverUUID, sessionID, err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	require.NotNil(t, serverUUID)
	require.NotNil(t, sessionID)
	tx1, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)
	tx2, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.Error(t, err)
	_, err = tx1.Commit(context.TODO())
	require.Nil(t, tx2)
}

func TestTransaction_MultipleReadAnsOneReasWrite(t *testing.T) {
	options := server.DefaultOptions()
	bs := servertest.NewBufconnServer(options)

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	bs.Start()
	defer bs.Stop()

	client := ic.DefaultClient().WithOptions(ic.DefaultOptions().WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()}))

	serverUUID, sessionID, err := client.OpenSession(context.TODO(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(t, err)
	require.NotNil(t, serverUUID)
	require.NotNil(t, sessionID)

	tx1, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)
	err = tx1.TxSQLExec(context.TODO(), `CREATE TABLE table1(id INTEGER,PRIMARY KEY id);`, nil)
	_, err = tx1.Commit(context.TODO())
	require.NoError(t, err)

	tx1, err = client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_WRITE})
	require.NoError(t, err)
	err = tx1.TxSQLExec(context.TODO(), `CREATE TABLE table2(id INTEGER,PRIMARY KEY id);`, nil)
	require.NoError(t, err)

	tx2, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_ONLY})
	err = tx2.TxSQLExec(context.TODO(), `CREATE TABLE table1(id INTEGER,PRIMARY KEY id);`, nil)
	require.Error(t, err)
	require.Equal(t, err.(immuErrors.ImmuError).Error(), "read write transaction not ongoing")
	_, err = tx2.TxSQLQuery(context.TODO(), "SELECT * FROM table1;", nil, true)
	require.NoError(t, err)

	tx3, err := client.BeginTx(context.TODO(), &ic.TxOptions{TxMode: schema.TxMode_READ_ONLY})
	require.NoError(t, err)
	_, err = tx3.TxSQLQuery(context.TODO(), "SELECT * FROM table1;", nil, true)
	require.NoError(t, err)
}
