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
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getRandomTableName() string {
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100000)
	return fmt.Sprintf("table%d", r)
}

func testServerClient(t *testing.T) (*servertest.BufconnServer, *sql.DB) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	t.Cleanup(func() { bs.Stop() })

	opts := client.DefaultOptions()
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	opts.
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
		WithDir(t.TempDir())

	db := OpenDB(opts)
	t.Cleanup(func() { db.Close() })
	return bs, db
}

func TestOpenDB(t *testing.T) {
	_, db := testServerClient(t)

	table := getRandomTableName()
	_, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s(id INTEGER, name VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'immu1')", table))
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, name) VALUES (2, 'immu2')", table))
	require.NoError(t, err)

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM %s ", table))
	require.NoError(t, err)

	var id uint64
	var name string
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&id, &name)
	require.NoError(t, err)
	require.Equal(t, uint64(1), id)
	require.Equal(t, "immu1", name)

	rows.Next()
	err = rows.Scan(&id, &name)
	require.NoError(t, err)
	require.Equal(t, uint64(2), id)
	require.Equal(t, "immu2", name)

	rowsw, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM %s WHERE id = 2", table))
	require.NoError(t, err)

	rowsw.Next()

	err = rowsw.Scan(&id, &name)
	require.NoError(t, err)
	require.Equal(t, uint64(2), id)
	require.Equal(t, "immu2", name)

	require.False(t, rowsw.Next())
}

func TestQueryCapabilities(t *testing.T) {
	_, db := testServerClient(t)

	table := getRandomTableName()
	result, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, publicID UUID, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")
	uuidPublicID := uuid.UUID([16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent, publicID) VALUES (?, ?, ?, ?, ?, ?, ?)", table), 1, 1000, 6000, "title 1", binaryContent, true, uuidPublicID)
	require.NoError(t, err)

	binaryContent2 := []byte("my blob content2")
	uuidPublicID2 := uuid.UUID([16]byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x40, 0x06, 0x80, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent, publicID) VALUES (?, ?, ?, ?, ?, ?, ?)", table), 2, 2000, 12000, "title 2", binaryContent2, true, uuidPublicID2)
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte
	var publicID uuid.UUID

	rows, err := db.QueryContext(context.Background(), fmt.Sprintf("SELECT id, amount, title, content, isPresent, publicID FROM %s where isPresent=? and id=? and amount=? and total=? and title=?", table), true, 1, 1000, 6000, "title 1")
	require.NoError(t, err)
	defer rows.Close()

	rows.Next()

	err = rows.Scan(&id, &amount, &title, &content, &isPresent, &publicID)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(1000), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
	require.Equal(t, uuidPublicID, publicID)
}

func TestQueryCapabilitiesWithPointers(t *testing.T) {
	_, db := testServerClient(t)

	table := getRandomTableName()

	_, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER AUTO_INCREMENT,name VARCHAR,manager_id INTEGER,PRIMARY KEY ID)", table))
	require.NoError(t, err)

	table1 := getRandomTableName()

	_, err = db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER AUTO_INCREMENT,user_id INTEGER,name VARCHAR,PRIMARY KEY ID)", table1))
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (name,manager_id) VALUES (?,?)", table), "name", 1)
	require.NoError(t, err)

	id := uint(1)
	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (user_id,name) VALUES (?,?),(?,?) ", table1), &id, "name1", &id, "name2")
	require.NoError(t, err)
}

func TestNilValues(t *testing.T) {
	_, db := testServerClient(t)

	table := getRandomTableName()

	result, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content) VALUES (?, ?, ?, ?, ?)", table), 1, nil, nil, nil, nil)
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
	_, db := testServerClient(t)

	table := getRandomTableName()

	result, err := db.ExecContext(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	binaryContent := []byte("my blob content1")

	argsV := []interface{}{&valuer{1}, &valuer{100}, &valuer{200}, &valuer{"title 1"}, &valuer{binaryContent}, &valuer{true}}
	_, err = db.ExecContext(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), argsV...)
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
	opts := client.DefaultOptions().WithDir(t.TempDir()).WithAddress("some.host.that.does.not.exist")

	db := OpenDB(opts)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, "this will not be executed")
	require.Error(t, err)
	require.Regexp(t, "context deadline exceeded|Error while dialing", err.Error())
}

func TestImmuConnector_ConnectLoginErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := client.DefaultOptions()
	opts.Username = "wrong-username"
	opts.Password = "immudb"
	opts.Database = "defaultdb"

	opts.
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
		WithDir(t.TempDir())

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.Background(), "this will not be executed")
	require.ErrorContains(t, err, "invalid user name or password")
}

func TestImmuConnector_ConnectUseDatabaseErr(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithDir(t.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	opts := client.DefaultOptions()
	opts.Username = "immudb"
	opts.Password = "immudb"
	opts.Database = "wrong-db"

	opts.
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}).
		WithDir(t.TempDir())

	db := OpenDB(opts)
	defer db.Close()

	_, err := db.ExecContext(context.Background(), "this will not be executed")
	require.ErrorContains(t, err, "database does not exist")
}

func TestImmuConnector_Driver(t *testing.T) {
	c := immuConnector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}
