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

package server_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/jackc/pgx/v4"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPgsqlServer_SimpleQuery(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title) VALUES (1, 200, 'title 1')", table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s", table)).Scan(&id, &amount, &title)
	require.NoError(t, err)
}

func TestPgsqlServer_SimpleQueryBlob(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)

	blobContent := hex.EncodeToString([]byte("my blob content"))
	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title, content) VALUES (1, 200, 'title 1', x'%s')", table, blobContent))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var content string
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title, content FROM %s", table)).Scan(&id, &amount, &title, &content)
	require.NoError(t, err)
	contentDst := make([]byte, 1000)

	_, err = hex.Decode(contentDst, []byte(content))
	require.NoError(t, err)

}

func TestPgsqlServer_SimpleQueryBool(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title, isPresent) VALUES (1, 200, 'title 1', true)", table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title, isPresent FROM %s", table)).Scan(&id, &amount, &title, &isPresent)
	require.True(t, isPresent)
	require.NoError(t, err)
}

func TestPgsqlServer_SimpleQueryExecError(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Exec("ILLEGAL STATEMENT")
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryError(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryMissingDatabase(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb  password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryDatabaseNotExists(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=notexists password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryMissingUsername(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryMissingPassword(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryClosedConnError(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)
	bs.Stop()

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryTerminate(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title) VALUES (1, 200, 'title 1')", table))
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s", table)).Scan(&id, &amount, &title)
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryEmptyQueryMessage(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title) VALUES (1, 200, 'title 1')", table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s WHERE id=2", table)).Scan(&id, &amount, &title)
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQueryCreateOrUseDatabaseNotSupported(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Exec("CREATE DATABASE db")
	require.Error(t, err)
	_, err = db.Exec("USE DATABASE db")
	require.Error(t, err)

}

func TestPgsqlServer_SimpleQueryQueryExecError(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, title) VALUES (1, 200, 'title 1')", table))
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryQuerySSLConn(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")

	certPem := []byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`)
	keyPem := []byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`)

	cert, err := tls.X509KeyPair(certPem, keyPem)
	require.NoError(t, err)
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}

	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0).WithTLS(cfg)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=require user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
}

func TestPgsqlServer_SSLNotEnabled(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=require user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.Error(t, err)

}

func _TestPgsqlServer_SimpleQueryAsynch(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, amount, title) VALUES (1, 200, 'title 1')", table))
	require.NoError(t, err)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			var title string
			var id int64
			var amount int64
			err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s", table)).Scan(&id, &amount, &title)
			require.NoError(t, err)

		}(i)
	}
	wg.Wait()

}

func TestPgsqlServer_VersionStatement(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	var version string
	err = db.QueryRow(fmt.Sprintf("SELECT version()")).Scan(&version)
	require.NoError(t, err)
	require.Equal(t, pgmeta.PgsqlProtocolVersionMessage, version)
}

func TestPgsqlServerSetStatement(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Query(fmt.Sprintf("SET test=val"))
	require.NoError(t, err)
}

func TestPgsqlServer_SimpleQueryNilValues(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id) VALUES (1)", table))
	require.NoError(t, err)

	var id int64
	var amount sql.NullInt64
	var title sql.NullString
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s where title = null", table)).Scan(&id, &amount, &title)
	require.NoError(t, err)
	require.False(t, title.Valid)
	require.False(t, amount.Valid)
}

func getRandomTableName() string {
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(100)
	return fmt.Sprintf("table%d", r)
}

func TestPgsqlServer_ExtendedQueryPG(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount, title, total) VALUES (1, 1111, 'title 1', 1111)", table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title FROM %s where amount=? and total=? and title=?", table), 1111, 1111, "title 1").Scan(&id, &amount, &title)
	require.NoError(t, err)
}

func TestPgsqlServer_ExtendedQueryPGxNamedStatements(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	//db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=5432 sslmode=disable user=postgres dbname=postgres password=postgres"))

	require.NoError(t, err)
	defer db.Close(context.Background())

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title) VALUES (9999, 1111, 6666, 'title 1')", table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(context.Background(), fmt.Sprintf("SELECT id, amount, title FROM %s where total=? and amount=? and title=?", table), 6666, 1111, "title 1").Scan(&id, &amount, &title)
	require.NoError(t, err)
}

func TestPgsqlServer_ExtendedQueryPGxMultiFieldsPreparedStatements(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))

	require.NoError(t, err)
	defer db.Close(context.Background())

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (1, 1000, 6000, 'title 1', x'%s', true)", table, blobContent))
	require.NoError(t, err)
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (2, 2000, 3000, 'title 2', x'%s', false)", table, blobContent2))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte
	err = db.QueryRow(context.Background(), fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s where isPresent=? and id=? and amount=? and total=? and title=? and content=?", table), true, 1, 1000, 6000, "title 1", blobContent).Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(1000), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
}

func TestPgsqlServer_ExtendedQueryPGMultiFieldsPreparedStatements(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (1, 1000, 6000, 'title 1', x'%s', true)", table, blobContent))
	require.NoError(t, err)
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (2, 2000, 3000, 'title 2', x'%s', false)", table, blobContent2))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s where isPresent=? and id=? and amount=? and total=? and title=?", table), true, 1, 1000, 6000, "title 1").Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(1000), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
}

func TestPgsqlServer_ExtendedQueryPGMultiFieldsPreparedInsert(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 1, 1000, 6000, "title 1", fmt.Sprintf("%s", blobContent), true)
	require.NoError(t, err)
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 2, 2000, 12000, "title 2", fmt.Sprintf("%s", blobContent2), true)
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	var isPresent bool
	var content []byte
	err = db.QueryRow(context.Background(), fmt.Sprintf("SELECT id, amount, title, content, isPresent FROM %s where isPresent=? and id=? and amount=? and total=? and title=?", table), true, 1, 1000, 6000, "title 1").Scan(&id, &amount, &title, &content, &isPresent)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	require.Equal(t, int64(1000), amount)
	require.Equal(t, "title 1", title)
	require.Equal(t, binaryContent, content)
	require.Equal(t, true, isPresent)
}

func TestPgsqlServer_ExtendedQueryPGxMultiInsertStatements(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	//db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=5432 sslmode=disable user=postgres dbname=postgres password=postgres"))

	require.NoError(t, err)
	defer db.Close(context.Background())

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title) VALUES (1, 11, 33, 'title 1'); INSERT INTO %s (id, amount, total, title) VALUES (2, 22, 66, 'title 2');", table, table))
	require.NoError(t, err)

	var id int64
	var amount int64
	var title string
	err = db.QueryRow(context.Background(), fmt.Sprintf("SELECT id, amount, title FROM %s where total=? and amount=? and title=?", table), 33, 11, "title 1").Scan(&id, &amount, &title)
	require.NoError(t, err)
}

func TestPgsqlServer_ExtendedQueryPGMultiFieldsPreparedMultiInsertError(t *testing.T) {
	td, _ := ioutil.TempDir("", "_pgsql")
	options := server.DefaultOptions().WithDir(td).WithPgsqlServer(true).WithPgsqlServerPort(0)
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	defer os.RemoveAll(td)
	defer os.Remove(".state-")

	bs.WaitForPgsqlListener()

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", bs.Server.Srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?); INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table, table), 1, 1000, 6000, "title 1", fmt.Sprintf("%s", blobContent), true, 2, 2000, 12000, "title 2", fmt.Sprintf("%s", blobContent2), true)
	require.Error(t, errors.ErrMaxStmtNumberExceeded)
}
