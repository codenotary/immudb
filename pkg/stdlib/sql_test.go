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
	"database/sql/driver"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()
	_, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s(id INTEGER, name VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'immu1')", table))
	require.NoError(t, err)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'immu2')", table))
	require.NoError(t, err)

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

	rowsw, err := db.QueryContext(context.TODO(), fmt.Sprintf("SELECT * FROM %s WHERE id = 2", table))
	require.NoError(t, err)

	rowsw.Next()

	err = rowsw.Scan(&id, &name)
	if err != nil {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(2), id)
	require.Equal(t, "immu2", name)

	require.False(t, rowsw.Next())
}

func TestQueryCapabilities(t *testing.T) {
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()
	result, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 1, 1000, 6000, "title 1", binaryContent, true)
	require.NoError(t, err)

	binaryContent2 := []byte("my blob content2")
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 2, 2000, 12000, "title 2", binaryContent2, true)
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s where isPresent=? and id=? and amount=? and total=? and title=?", table), true, 1, 1000, 6000, "title 1")
	require.NoError(t, err)
	defer rows.Close()

	rows.Next()

	err = rows.Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(1000), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
}

func TestQueryCapabilitiesWithPointers(t *testing.T) {
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()

	_, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER AUTO_INCREMENT,name VARCHAR,manager_id INTEGER,PRIMARY KEY ID)", table))
	require.NoError(t, err)

	table1 := getRandomTableName()

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER AUTO_INCREMENT,user_id INTEGER,name VARCHAR,PRIMARY KEY ID)", table1))
	require.NoError(t, err)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (name,manager_id) VALUES (?,?)", table), "name", 1)
	require.NoError(t, err)

	id := uint(1)
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (user_id,name) VALUES (?,?),(?,?) ", table1), &id, "name1", &id, "name2")
	require.NoError(t, err)
}

func TestNilValues(t *testing.T) {
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()

	result, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content) VALUES (?, ?, ?, ?, ?)", table), 1, nil, nil, nil, nil)
	require.NoError(t, err)

	var id int64
	var amount sql.NullInt64
	var title sql.NullString
	var content []byte

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT id, amount, title, content FROM %s where id=? and amount=? and total=? and title=?", table), 1, nil, nil, nil)
	require.NoError(t, err)
	defer rows.Close()

	rows.Next()

	err = rows.Scan(&id, &amount, &title, &content)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.False(t, title.Valid)
	require.False(t, amount.Valid)
	require.Nil(t, content)
}

type valuer struct {
	val interface{}
}

func (v *valuer) Value() (driver.Value, error) {
	return v.val.(driver.Value), nil
}

func TestDriverValuer(t *testing.T) {
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	table := getRandomTableName()

	result, err := db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")

	argsV := []interface{}{&valuer{1}, &valuer{100}, &valuer{200}, &valuer{"title 1"}, &valuer{binaryContent}, &valuer{true}}
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), argsV...)
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s ", table), argsV...)
	require.NoError(t, err)
	defer rows.Close()

	rows.Next()

	err = rows.Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(100), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
}

func TestImmuConnector_ConnectErr(t *testing.T) {
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

func TestImmuConnector_ConnectLoginErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	opts := client.DefaultOptions()
	opts.Username = "wrongUsername"

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.TODO(), "this will not be executed")
	require.Error(t, err)
}

func TestImmuConnector_ConnectUseDatabaseErr(t *testing.T) {
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
	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.TODO(), "this will not be executed")
	require.Error(t, err)
}

func TestImmuConnector_Driver(t *testing.T) {
	c := immuConnector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}

func TestConn(t *testing.T) {
	c := Conn{
		immuClient: client.NewClient(),
		options:    client.DefaultOptions(),
	}
	cli := c.GetImmuClient()
	require.IsType(t, new(client.ImmuClient), &cli)
}

func TestConnErr(t *testing.T) {
	c := Conn{
		immuClient: client.NewClient(),
		options:    client.DefaultOptions(),
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.QueryContext(context.TODO(), "query", 10.5)
	require.Error(t, err)

	_, err = db.ExecContext(context.TODO(), "INSERT INTO myTable(id, name) VALUES (2, 'immu2')")
	require.Error(t, err)

	_, err = db.QueryContext(context.TODO(), "SELECT * FROM myTable")
	require.Error(t, err)
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

	opts.WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure()})

	db := OpenDB(opts)
	defer db.Close()
	dri := db.Driver()

	conn, err := dri.Open(GetUri(opts))
	require.NoError(t, err)

	immuConn := conn.(driver.Pinger)

	err = immuConn.Ping(context.TODO())
	require.NoError(t, err)
}*/
