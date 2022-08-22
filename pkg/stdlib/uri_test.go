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

package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"

	"github.com/stretchr/testify/require"
)

func testServer(t *testing.T) (port int, cleanup func()) {
	path, err := ioutil.TempDir(os.TempDir(), "test_data")
	require.NoError(t, err)

	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(path)

	server := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	server.Initialize()

	go func() {
		server.Start()
	}()

	// TODO: Use a better method to wait for the test server
	time.Sleep(500 * time.Millisecond)

	port = server.Listener.Addr().(*net.TCPAddr).Port
	return port, func() {
		server.Stop()
		os.RemoveAll(options.Dir)
		os.Remove(".state-")
	}
}

func TestDriver_Open(t *testing.T) {
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3324/defaultdb")
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestParseConfig(t *testing.T) {
	connString := "immudb://immudb:immudb@127.0.0.1:3324/defaultdb"
	ris, err := ParseConfig(connString)
	require.NoError(t, err)
	require.NotNil(t, ris)
	require.Equal(t, "immudb", ris.Username)
	require.Equal(t, "immudb", ris.Password)
	require.Equal(t, "defaultdb", ris.Database)
	require.Equal(t, "127.0.0.1", ris.Address)
	require.Equal(t, 3324, ris.Port)
}

func TestParseConfig_InsecureVerify(t *testing.T) {
	connString := "immudb://immudb:immudb@127.0.0.1:3324/defaultdb?sslmode=insecure-verify"
	ris, err := ParseConfig(connString)
	require.NoError(t, err)
	require.NotNil(t, ris)
	require.Equal(t, "immudb", ris.Username)
	require.Equal(t, "immudb", ris.Password)
	require.Equal(t, "defaultdb", ris.Database)
	require.Equal(t, "127.0.0.1", ris.Address)
	require.Equal(t, 3324, ris.Port)
}

func TestParseConfig_Require(t *testing.T) {
	connString := "immudb://immudb:immudb@127.0.0.1:3324/defaultdb?sslmode=require"
	ris, err := ParseConfig(connString)
	require.NoError(t, err)
	require.NotNil(t, ris)
	require.Equal(t, "immudb", ris.Username)
	require.Equal(t, "immudb", ris.Password)
	require.Equal(t, "defaultdb", ris.Database)
	require.Equal(t, "127.0.0.1", ris.Address)
	require.Equal(t, 3324, ris.Port)
}

func TestParseConfigErrs(t *testing.T) {
	connString := "immudb://immudb:immudb@127.0.0.1:aaa/defaultdb"
	_, err := ParseConfig(connString)
	require.Error(t, err)

	connString = "AAAA://immudb:immudb@127.0.0.1:123/defaultdb"
	_, err = ParseConfig(connString)
	require.Error(t, err)

	connString = "AAAA://immudb:immudb@127.0.0.1:123/defaultdb?sslmode=invalid"
	_, err = ParseConfig(connString)
	require.Error(t, err)
}

func TestDriver_OpenSSLPrefer(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	d := immuDriver
	conn, err := d.Open(fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb", port))
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenSSLDisable(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	d := immuDriver
	conn, err := d.Open(fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenSSLRequire(t *testing.T) {
	t.Skip("TODO: internal server not running with ssl mode")

	port, cleanup := testServer(t)
	defer cleanup()

	d := immuDriver
	conn, err := d.Open(fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=require", port))
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func Test_SQLOpen(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	db, err := sql.Open("immudb", fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", "myTable"))
	require.NoError(t, err)
}

func Test_Open(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	db := Open(fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NotNil(t, db)

	_, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", "myTable"))
	require.NoError(t, err)
}
