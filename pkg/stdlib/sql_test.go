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
	"math/rand"
	"os"
	"strings"
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

func TestRegisterConnConfig(t *testing.T) {
	registered := RegisterConnConfig(&Conn{})
	require.True(t, strings.Contains(registered, "registeredConnConfig"))
	UnregisterConnConfig(registered)
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
	_, err := d.Open("immudb")
	require.NoError(t, err)
}

func TestDriverConnector_Driver(t *testing.T) {
	c := driverConnector{
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
	err = c.CheckNamedValue(nil)
	require.Error(t, err)
}
