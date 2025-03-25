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

package server_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	isql "github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/pgsql/errors"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/jackc/pgx/v4"
	pq "github.com/lib/pq"

	"github.com/stretchr/testify/require"
)

func TestPgsqlServer_SimpleQuery(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Exec("ILLEGAL STATEMENT")
	require.ErrorContains(t, err, "syntax error: unexpected IDENTIFIER at position 7")
}

func TestPgsqlServer_SimpleQueryQueryError(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.ErrorContains(t, err, isql.ErrTableDoesNotExist.Error())
}

func TestPgsqlServer_SimpleQueryQueryMissingDatabase(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb  password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.ErrorContains(t, err, errors.ErrDBNotprovided.Error())
}

func TestPgsqlServer_SimpleQueryQueryDatabaseNotExists(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=notexists password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.ErrorContains(t, err, errors.ErrDBNotExists.Error())
}

func TestPgsqlServer_SimpleQueryQueryMissingUsername(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.ErrorContains(t, err, errors.ErrInvalidUsernameOrPassword.Error())
}

func TestPgsqlServer_SimpleQueryQueryMissingPassword(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.ErrorContains(t, err, errors.ErrPwNotprovided.Error())
}

func TestPgsqlServer_SimpleQueryQueryClosedConnError(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	err = db.QueryRow("SELECT id, amount, title, isPresent FROM notExists").Scan()
	require.Error(t, err)
}

func TestPgsqlServer_SimpleQueryTerminate(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	require.ErrorContains(t, err, "sql: database is closed")
}

func TestPgsqlServer_SimpleQueryQueryEmptyQueryMessage(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestPgsqlServer_SimpleQueryQueryCreateOrUseDatabaseNotSupported(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Exec("CREATE DATABASE db")
	require.NoError(t, err)

	_, err = db.Exec("USE DATABASE db")
	require.ErrorContains(t, err, errors.ErrUseDBStatementNotSupported.Error())

}

func TestPgsqlServer_SimpleQueryQueryExecError(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)

	_, err = db.Exec(fmt.Sprintf("UPSERT INTO %s (id, title) VALUES (1, 200, 'title 1')", table))
	require.ErrorContains(t, err, isql.ErrInvalidNumberOfValues.Error())
}

func TestPgsqlServer_SimpleQueryQuerySSLConn(t *testing.T) {
	td := t.TempDir()

	pemServerCA, err := os.ReadFile("cert/ca-cert.pem")
	require.NoError(t, err)

	serverCert, err := tls.LoadX509KeyPair("cert/server-cert.pem", "cert/server-key.pem")
	require.NoError(t, err)

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(pemServerCA) {
		panic("failed to add client CA's certificate")
	}

	cfg := &tls.Config{
		RootCAs:      certPool,
		Certificates: []tls.Certificate{serverCert},
	}

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false).
		WithTLS(cfg)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err = srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=require user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.NoError(t, err)
}

func TestPgsqlServer_SSLNotEnabled(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=require user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, title VARCHAR, content BLOB, PRIMARY KEY id)", table))
	require.ErrorIs(t, err, pq.ErrSSLNotSupported)

}

func TestPgsqlServer_VersionStatement(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	require.NoError(t, err)
	require.Equal(t, pgmeta.PgsqlServerVersionMessage, version)

	_, err = db.Exec("DEALLOCATE \"_PLAN0x7fb2c0822800\"")
	require.NoError(t, err)
}

func TestPgsqlServerSetStatement(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	_, err = db.Query("SET test=val")
	require.NoError(t, err)
}

func TestPgsqlServer_SimpleQueryNilValues(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	r := rand.Intn(100000)
	return fmt.Sprintf("table%d", r)
}

func TestPgsqlServer_ExtendedQueryPG(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))

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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 1, 1000, 6000, "title 1", blobContent, true)
	require.NoError(t, err)
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table), 2, 2000, 12000, "title 2", blobContent2, true)
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
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
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	defer os.Remove(".state-")

	db, err := pgx.Connect(context.Background(), fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)

	table := getRandomTableName()
	result, err := db.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (id INTEGER, amount INTEGER, total INTEGER, title VARCHAR, content BLOB, isPresent BOOLEAN, PRIMARY KEY id)", table))
	require.NoError(t, err)
	require.NotNil(t, result)
	binaryContent := []byte("my blob content1")
	blobContent2 := hex.EncodeToString([]byte("my blob content2"))
	blobContent := hex.EncodeToString(binaryContent)
	_, err = db.Exec(context.Background(), fmt.Sprintf("INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?); INSERT INTO %s (id, amount, total, title, content, isPresent) VALUES (?, ?, ?, ?, ?, ?)", table, table), 1, 1000, 6000, "title 1", blobContent, true, 2, 2000, 12000, "title 2", blobContent2, true)
	require.ErrorContains(t, err, errors.ErrMaxStmtNumberExceeded.Error())
}

func TestPgsqlServer_InvalidTraffic(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)

	err := srv.Initialize()
	if err != nil {
		panic(err)
	}

	go func() {
		srv.Start()
	}()

	defer func() {
		srv.Stop()
	}()

	_, err = http.Get(fmt.Sprintf("http://localhost:%d", srv.PgsqlSrv.GetPort()))
	require.Error(t, err)
}
