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

package stdlib

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

func getRandomTableName() string {
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100)
	return fmt.Sprintf("table%d", r)
}

func TestOpenDB(t *testing.T) {
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

	opts.WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()
	_, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s(id INTEGER, name VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'immu1')", table))
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'immu2')", table))

	rows, err := db.QueryContext(context.TODO(), fmt.Sprintf("SELECT * FROM %s ", table))
	require.NoError(t, err)

	var id uint64
	var name string
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&id, &name)
	if err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), id)
	require.Equal(t, "immu1", name)
	rows.Next()
	err = rows.Scan(&id, &name)
	if err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(2), id)
	require.Equal(t, "immu2", name)

}

func TestDriverConnector_ConnectErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := client.DefaultOptions()

	db := OpenDB(opts)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	_, err := db.ExecContext(ctx, "this will not be executed")
	require.Error(t, err)
}

func TestDriverConnector_ConnectLoginErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := client.DefaultOptions()
	opts.Username = "wrongUsername"

	opts.WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.TODO(), "this will not be executed")
	require.Error(t, err)
}

func TestDriverConnector_ConnectUseDatabaseErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := client.DefaultOptions()
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "wrong db"
	opts.WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.TODO(), "this will not be executed")
	require.Error(t, err)
}

func TestConnector_Driver(t *testing.T) {
	c := Connector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}

func TestDriver_Open(t *testing.T) {
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3324/defaultdb")
	require.Errorf(t, err, "connection error: desc = \"transport: Error while dialing dial tcp 127.0.0.1:3324: connect: connection refused\"")
	require.Nil(t, conn)
}

func TestDriverConnector_Driver(t *testing.T) {
	c := Connector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}

func TestConn(t *testing.T) {
	c := Conn{
		conn:    client.DefaultClient(),
		options: client.DefaultOptions(),
		Token:   "token",
	}
	cli := c.GetImmuClient()
	require.IsType(t, new(client.ImmuClient), &cli)
	token := c.GetToken()
	require.Equal(t, "token", token)
}

func TestConnErr(t *testing.T) {
	c := Conn{
		conn:    client.DefaultClient(),
		options: client.DefaultOptions(),
		Token:   "token",
	}

	_, err := c.Prepare("")
	require.Error(t, err)
	_, err = c.PrepareContext(context.TODO(), "")
	require.Error(t, err)
	_, err = c.Begin()
	require.Error(t, err)
	_, err = c.BeginTx(context.TODO(), driver.TxOptions{})
	require.Error(t, err)

	_, err = c.ExecContext(context.TODO(), "", nil)
	require.Error(t, err)
	_, err = c.QueryContext(context.TODO(), "", nil)
	require.Error(t, err)
	err = c.Ping(context.TODO())
	require.Error(t, err)
	err = c.ResetSession(context.TODO())
	require.Error(t, err)
	ris := c.CheckNamedValue(nil)
	require.Nil(t, ris)
}

func TestConn_QueryContextErr(t *testing.T) {
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

	opts.WithDialOptions(&[]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.QueryContext(context.TODO(), "query", 10.5)
	require.Equal(t, ErrFloatValuesNotSupported, err)
	_, err = db.ExecContext(context.TODO(), "query", 10.5)
	require.Equal(t, ErrFloatValuesNotSupported, err)

	_, err = db.ExecContext(context.TODO(), "INSERT INTO myTable(id, name) VALUES (2, 'immu2')")
	require.Error(t, err)
	_, err = db.QueryContext(context.TODO(), "SELECT * FROM myTable")
	require.Error(t, err)
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

func TestParseConfigErrs(t *testing.T) {
	connString := "immudb://immudb:immudb@127.0.0.1:aaa/defaultdb"
	_, err := ParseConfig(connString)
	require.Error(t, err)
	connString = "AAAA://immudb:immudb@127.0.0.1:123/defaultdb"
	_, err = ParseConfig(connString)
	require.Error(t, err)
}

func TestGetUriErr(t *testing.T) {
	cliOpts := client.DefaultOptions().WithAddress(" ").WithUsername(" ")
	_, err := GetUri(cliOpts)
	require.Error(t, err)
}
func TestRows(t *testing.T) {
	r := Rows{
		index: 0,
		conn:  nil,
		rows:  nil,
	}

	ast := r.Columns()
	require.Nil(t, ast)
	st := r.ColumnTypeDatabaseTypeName(1)
	require.Equal(t, "", st)
	num, b := r.ColumnTypeLength(1)
	require.Equal(t, int64(math.MaxInt64), num)
	require.False(t, b)
	_, _, _ = r.ColumnTypePrecisionScale(1)
	ty := r.ColumnTypeScanType(1)
	require.Nil(t, ty)
}
