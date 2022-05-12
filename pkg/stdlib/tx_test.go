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

package stdlib

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc/status"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestConn_BeginTx(t *testing.T) {
	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0)

	server := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	server.Initialize()

	defer server.Stop()
	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	go func() {
		server.Start()
	}()

	time.Sleep(500 * time.Millisecond)

	port := server.Listener.Addr().(*net.TCPAddr).Port

	db, err := sql.Open("immudb", fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)

	table1 := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table1))
	require.NoError(t, err)
	require.NotNil(t, result)

	tx, err := db.Begin()
	require.NoError(t, err)

	table := getRandomTableName()
	result, err = tx.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (1, 1000, 6000, 'title 1', x'%s', true)", table, blobContent))
	require.Error(t, err)
	st, _ := status.FromError(err)
	require.Equal(t, fmt.Sprintf("table does not exist (%s)", table), st.Message())

	err = tx.Commit()
	require.NoError(t, err)

	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (2, 2000, 3000, 'title 2', x'%s', false)", table, blobContent2))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s where isPresent=? and id=? and amount=? and total=? and title=?", table), false, 2, 2000, 3000, "title 2").Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)
	require.Equal(t, int64(2000), amount)
	require.Equal(t, "title 2", title)
	require.Equal(t, []byte("my blob content2"), content)
	require.Equal(t, false, isPresent)
}

func TestTx_Rollback(t *testing.T) {
	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0)

	server := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	server.Initialize()

	defer server.Stop()
	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	go func() {
		server.Start()
	}()

	time.Sleep(500 * time.Millisecond)

	port := server.Listener.Addr().(*net.TCPAddr).Port

	db, err := sql.Open("immudb", fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := tx.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = tx.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id) VALUES (2)", table))
	require.NoError(t, err)

	rows, err := tx.QueryContext(context.TODO(), fmt.Sprintf("SELECT * FROM %s", table))
	require.NoError(t, err)
	require.NotNil(t, rows)

	err = tx.Rollback()
	require.NoError(t, err)

	_, err = db.QueryContext(context.TODO(), fmt.Sprintf("SELECT * FROM %s", table))
	st, _ := status.FromError(err)
	require.Equal(t, fmt.Sprintf("table does not exist (%s)", table), st.Message())
}

func TestTx_Errors(t *testing.T) {
	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0)

	server := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	server.Initialize()

	defer server.Stop()
	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	go func() {
		server.Start()
	}()

	time.Sleep(500 * time.Millisecond)

	port := server.Listener.Addr().(*net.TCPAddr).Port

	db, err := sql.Open("immudb", fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.ExecContext(context.TODO(), "this is really wrong")
	require.Error(t, err)

	_, err = tx.QueryContext(context.TODO(), "this is also very wrong")
	require.Error(t, err)
}
