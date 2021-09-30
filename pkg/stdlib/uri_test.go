package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"
)

func TestDriver_Open(t *testing.T) {
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3324/defaultdb")
	require.Errorf(t, err, "connection error: desc = \"transport: Error while dialing dial tcp 127.0.0.1:3324: connect: connection refused\"")
	require.Nil(t, conn)
}

func TestDriver_OpenSSLPrefer(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", client.DefaultOptions().Port))
	}
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3322/defaultdb")
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenSSLDisable(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server  without tls at port %d to run this test.", client.DefaultOptions().Port))
	}
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3322/defaultdb?sslmode=disable")
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriver_OpenSSLRequire(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", client.DefaultOptions().Port))
	}
	d := immuDriver
	conn, err := d.Open("immudb://immudb:immudb@127.0.0.1:3322/defaultdb?sslmode=require")
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func Test_SQLOpen(t *testing.T) {
	_, err := net.DialTimeout("tcp", fmt.Sprintf(":%d", client.DefaultOptions().Port), 1*time.Second)
	if err != nil {
		t.Skip(fmt.Sprintf("Please launch an immudb server at port %d to run this test.", client.DefaultOptions().Port))
	}
	db, err := sql.Open("immudb", "immudb://immudb:immudb@127.0.0.1:3322/defaultdb?sslmode=disable")
	require.NoError(t, err)
	_, err = db.ExecContext(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", "myTable"))
	require.NoError(t, err)
}
