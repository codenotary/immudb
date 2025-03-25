/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package stdlib

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConn(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	opts := client.DefaultOptions().
		WithDir(t.TempDir()).
		WithPort(port)
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	cli, err := client.NewImmuClient(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	_, err = cli.Login(ctx, []byte(opts.Username), []byte(opts.Password))
	require.NoError(t, err)

	c := Conn{
		immuClient: cli,
	}

	icli := c.GetImmuClient()
	require.IsType(t, new(client.ImmuClient), &icli)
}

func TestConnErr(t *testing.T) {
	c := Conn{
		immuClient: client.NewClient(),
		options:    client.DefaultOptions(),
	}

	_, err := c.Prepare("")
	require.ErrorIs(t, err, ErrNotImplemented)

	_, err = c.PrepareContext(context.Background(), "")
	require.ErrorIs(t, err, ErrNotImplemented)

	_, err = c.Begin()
	require.ErrorIs(t, err, driver.ErrBadConn)

	_, err = c.BeginTx(context.Background(), driver.TxOptions{})
	require.ErrorIs(t, err, driver.ErrBadConn)

	_, err = c.ExecContext(context.Background(), "", nil)
	require.ErrorIs(t, err, driver.ErrBadConn)

	_, err = c.QueryContext(context.Background(), "", nil)
	require.ErrorIs(t, err, driver.ErrBadConn)

	err = c.ResetSession(context.Background())
	require.ErrorIs(t, err, driver.ErrBadConn)

	ris := c.CheckNamedValue(nil)
	require.Nil(t, ris)
}

func TestConn_QueryContextErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := client.DefaultOptions().WithDir(t.TempDir())

	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.QueryContext(context.Background(), "query", 10.5)
	require.ErrorContains(t, err, "syntax error: unexpected IDENTIFIER")

	_, err = db.ExecContext(context.Background(), "INSERT INTO myTable(id, name) VALUES (2, 'immu2')")
	require.ErrorContains(t, err, "table does not exist (mytable)")

	_, err = db.QueryContext(context.Background(), "SELECT * FROM myTable")
	require.ErrorContains(t, err, "table does not exist (mytable)")
}

func TestConn_QueryContext(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	opts := client.DefaultOptions().WithDir(t.TempDir()).WithPort(port)
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	cli, err := client.NewImmuClient(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	_, err = cli.Login(ctx, []byte(opts.Username), []byte(opts.Password))
	require.NoError(t, err)

	c := Conn{
		immuClient: cli,
	}

	table := "mytable"
	result, err := c.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table), nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")
	argsV := []driver.NamedValue{
		{Name: "id", Value: 1},
		{Name: "amount", Value: 100},
		{Name: "total", Value: 200},
		{Name: "title", Value: "title 1"},
		{Name: "content", Value: binaryContent},
		{Name: "isPresent", Value: true},
	}
	_, err = c.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), argsV)
	require.NoError(t, err)

	rows, err := c.QueryContext(ctx, "SELECT * FROM myTable limit 1", nil)
	require.NoError(t, err)
	defer rows.Close()

	dst := make([]driver.Value, 6)
	rows.Next(dst)

	require.Equal(t, int64(1), dst[0])
	require.Equal(t, int64(100), dst[1])
	require.Equal(t, int64(200), dst[2])
	require.Equal(t, "title 1", dst[3])
	require.Equal(t, binaryContent, dst[4])
	require.Equal(t, true, dst[5])
}

func TestConn_QueryContextEmptyTable(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	opts := client.DefaultOptions().WithDir(t.TempDir()).WithPort(port)
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	cli, err := client.NewImmuClient(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	_, err = cli.Login(ctx, []byte(opts.Username), []byte(opts.Password))
	require.NoError(t, err)

	c := Conn{
		immuClient: cli,
	}

	table := "emptytable"
	result, err := c.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table), nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	rows, err := c.QueryContext(ctx, "SELECT * FROM emptytable limit 1", nil)
	require.NoError(t, err)
	defer rows.Close()

	cols := rows.Columns()
	require.Equal(t, len(cols), 6)
}

/*func TestConn_Ping(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := client.DefaultOptions()
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())})

	db := OpenDB(opts)
	defer db.Close()
	dri := db.Driver()

	conn, err := dri.Open(GetUri(opts))
	require.NoError(t, err)

	immuConn := conn.(driver.Pinger)

	err = immuConn.Ping(context.Background())
	require.NoError(t, err)
}*/
