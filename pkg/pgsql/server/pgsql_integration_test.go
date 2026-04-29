/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"github.com/jackc/pgx/v5"
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

	// Scan BYTEA into []byte — lib/pq decodes the PG canonical `\x<hex>`
	// text format back into raw bytes for us. Earlier the test scanned
	// into a string and hex-decoded the result, which only worked
	// because immudb was emitting raw hex without the `\x` prefix and
	// lib/pq fell back to escape-format (returning the ASCII of the
	// hex digits). After the text-format BYTEA fix
	// (data_row.renderValueAsByte), real PG behaviour is preserved.
	var id int64
	var amount int64
	var title string
	var content []byte
	err = db.QueryRow(fmt.Sprintf("SELECT id, amount, title, content FROM %s", table)).Scan(&id, &amount, &title, &content)
	require.NoError(t, err)
	require.Equal(t, "my blob content", string(content))
	_ = blobContent
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

// k3s/kine (and many pg clients) use `-- ping` as a liveness probe.
// PostgreSQL replies EmptyQueryResponse + ReadyForQuery; immudb must
// do the same so the connection stays usable for real queries after.
func TestPgsqlServer_CommentOnlyQueryReturnsEmptyQueryResponse(t *testing.T) {
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
	defer db.Close()

	// Pin a single connection so we can verify the session survives the
	// comment-only query and the transaction status stays 'Idle'.
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	// Comment-only queries: line, block, whitespace-only, and mixed.
	for _, q := range []string{"-- ping", "/* ping */", "   ", "-- a\n-- b\n", "/* a */ -- b\n"} {
		_, err = conn.ExecContext(context.Background(), q)
		require.NoError(t, err, "query %q should not error", q)
	}

	// Real query on the same connection must still work, proving that
	// ReadyForQuery was emitted correctly after each EmptyQueryResponse.
	table := getRandomTableName()
	_, err = conn.ExecContext(context.Background(),
		fmt.Sprintf("CREATE TABLE %s (id INTEGER, PRIMARY KEY id)", table))
	require.NoError(t, err)

	// Trailing-comment on a real query should also work (lexer skip).
	_, err = conn.ExecContext(context.Background(),
		fmt.Sprintf("UPSERT INTO %s (id) VALUES (1) -- trailing", table))
	require.NoError(t, err)
}

// F1 DB / generic PG dump regressions. Each of these used to fail
// against immudb's pgwire; see the PR that introduced the fixes.
func TestPgsqlServer_F1DBCompatRegressions(t *testing.T) {
	td := t.TempDir()

	options := server.DefaultOptions().
		WithDir(td).
		WithPort(0).
		WithPgsqlServer(true).
		WithPgsqlServerPort(0).
		WithMetricsServer(false).
		WithWebServer(false)

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	require.NoError(t, srv.Initialize())
	go srv.Start()
	defer srv.Stop()
	defer os.Remove(".state-")

	db, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", srv.PgsqlSrv.GetPort()))
	require.NoError(t, err)
	defer db.Close()

	t.Run("UNIQUE column constraint is enforced", func(t *testing.T) {
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, email VARCHAR(255) UNIQUE)", tbl))
		require.NoError(t, err)

		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, email) VALUES (1, 'a@x')", tbl))
		require.NoError(t, err)

		// Different key, same email — must be rejected by the unique
		// index created implicitly from the column constraint. Prior
		// behaviour (UNIQUE silently stripped) would have accepted it.
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, email) VALUES (2, 'a@x')", tbl))
		require.Error(t, err, "duplicate email should violate UNIQUE constraint")
	})

	t.Run("DATE 'yyyy-mm-dd' typed literal", func(t *testing.T) {
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, dob TIMESTAMP)", tbl))
		require.NoError(t, err)

		// The F1DB dump uses `DATE '2025-01-01'` — immudb previously
		// rejected it with "syntax error: unexpected '-'".
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, dob) VALUES (1, DATE '2025-01-01')", tbl))
		require.NoError(t, err)
	})

	t.Run("parametrized WHERE re-binds across calls (issue #1153)", func(t *testing.T) {
		// Pre-fix the boolean-expression substitute() methods (CmpBoolExp
		// / NumExp / NotBoolExp / BinBoolExp / Cast) mutated the AST in
		// place, replacing their child Param node with the bound value.
		// Combined with the per-session prepared-statement cache in the
		// pgsql wire layer, that pinned the WHERE clause to the FIRST
		// parameter value forever — every subsequent execution of the
		// same prepared SELECT silently filtered on the previous bind.
		// Repro: insert two rows with distinct keys, look them up by
		// parametrized SELECT in two consecutive calls, expect the
		// second call to return the second row (not the first).
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (k VARCHAR(20) NOT NULL, v VARCHAR(20), PRIMARY KEY(k))", tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (k, v) VALUES ('alice', 'one'), ('charlie', 'two')", tbl))
		require.NoError(t, err)

		var got string
		require.NoError(t, db.QueryRow(
			fmt.Sprintf("SELECT v FROM %s WHERE k = $1", tbl), "alice").Scan(&got))
		require.Equal(t, "one", got, "first parametrized lookup")

		err = db.QueryRow(
			fmt.Sprintf("SELECT v FROM %s WHERE k = $1", tbl), "charlie").Scan(&got)
		require.NoError(t, err)
		require.Equal(t, "two", got,
			"#1153: second lookup of the same prepared SELECT must rebind $1 — pre-fix this returned 'one' because CmpBoolExp.substitute mutated the cached AST")

		// Non-existent key must miss, not return some stale row.
		err = db.QueryRow(
			fmt.Sprintf("SELECT v FROM %s WHERE k = $1", tbl), "nobody").Scan(&got)
		require.ErrorIs(t, err, sql.ErrNoRows, "non-matching parametrized lookup must return no rows")
	})

	t.Run("TIMESTAMP parameter bind round-trips (issue #1149)", func(t *testing.T) {
		// JDBC's setTimestamp and lib/pq both emit the timestamp in
		// space-separated form with a `Z` (UTC) or `±HH:MM` suffix
		// when the value carries a timezone. The original report
		// ("value is not a timestamp: invalid value provided") was
		// caused by pgTextTimestamp not recognizing that shape.
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (username VARCHAR(50), created_at TIMESTAMP, PRIMARY KEY (username))", tbl))
		require.NoError(t, err)

		now := time.Now().UTC()
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (username, created_at) VALUES ($1, $2)", tbl),
			"u1", now)
		require.NoError(t, err, "parametrized TIMESTAMP INSERT must succeed")

		_, err = db.Exec(fmt.Sprintf("UPDATE %s SET created_at = $1 WHERE username = $2", tbl),
			now.Add(time.Hour), "u1")
		require.NoError(t, err, "parametrized TIMESTAMP UPDATE must succeed")
	})

	t.Run("DECIMAL column accepts integer literal", func(t *testing.T) {
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, price DECIMAL(10,2))", tbl))
		require.NoError(t, err)

		// User report: `INSERT … VALUES (1, 0)` fails on a DECIMAL
		// column; only `0.0` works. Implicit int→float conversion
		// should cover this (embedded/sql/implicit_conversion.go).
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, price) VALUES (1, 0)", tbl))
		require.NoError(t, err, "integer literal into DECIMAL column should be coerced")

		// Single-row non-zero int
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, price) VALUES (2, 42)", tbl))
		require.NoError(t, err, "single-row non-zero int into DECIMAL")

		// Multi-row, all ints
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, price) VALUES (3, 42), (4, -7)", tbl))
		require.NoError(t, err, "multi-row all-int into DECIMAL")

		// Multi-row mixed int + float — F1DB-realistic
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, price) VALUES (5, 42), (6, 3.14)", tbl))
		require.NoError(t, err, "multi-row mixed int+float into DECIMAL")

		// 3 rows, int then float then int — ordering stress test.
		// The regression that motivated this test had this shape.
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, price) VALUES (7, 42), (8, -7), (9, 3.14)", tbl))
		require.NoError(t, err, "3-row int+int+float into DECIMAL")

		// UPDATE path — same coercion must apply.
		_, err = db.Exec(fmt.Sprintf("UPDATE %s SET price = 10 WHERE id = 1", tbl))
		require.NoError(t, err, "UPDATE int-literal into DECIMAL")
	})

	t.Run("NUMERIC column accepts integer literal", func(t *testing.T) {
		tbl := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, amount NUMERIC)", tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (id, amount) VALUES (1, 0)", tbl))
		require.NoError(t, err)
	})

	t.Run("CREATE VIEW with PG column-list", func(t *testing.T) {
		// pg_dump emits `CREATE VIEW v("a","b") AS SELECT …` — the
		// outer (a,b) renames output columns. immudb's grammar rejects
		// the list, so the pgwire layer strips it; the SELECT's AS
		// aliases carry the names through.
		tbl := getRandomTableName()
		view := getRandomTableName()
		_, err := db.Exec(fmt.Sprintf(
			"CREATE TABLE %s (id INTEGER PRIMARY KEY, raw_time VARCHAR(32))", tbl))
		require.NoError(t, err)
		_, err = db.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, raw_time) VALUES (1, '1:23.456')", tbl))
		require.NoError(t, err)

		viewDDL := fmt.Sprintf(
			"CREATE VIEW %s(\"id\", \"time\") AS SELECT \"%s\".\"id\", \"%s\".\"raw_time\" AS \"time\" FROM \"%s\"",
			view, tbl, tbl, tbl)
		_, err = db.Exec(viewDDL)
		require.NoError(t, err, "PG-style CREATE VIEW with column-list should load")

		var id int64
		var tstr string
		err = db.QueryRow(fmt.Sprintf("SELECT id, time FROM %s", view)).Scan(&id, &tstr)
		require.NoError(t, err)
		require.Equal(t, int64(1), id)
		require.Equal(t, "1:23.456", tstr)
	})
}

func TestPgsqlServer_UseDatabaseSwitchesSession(t *testing.T) {
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

	_, err = db.Exec("CREATE DATABASE seconddb")
	require.NoError(t, err)

	// USE DATABASE used to be rejected with ErrUseDBStatementNotSupported.
	// It now succeeds and rebinds the session to the target database without
	// requiring the client to reconnect.
	_, err = db.Exec("USE DATABASE seconddb")
	require.NoError(t, err)

	// A table created after USE must land in seconddb. We prove this by
	// switching back to defaultdb and confirming the table is no longer
	// visible there.
	_, err = db.Exec("CREATE TABLE post_use (id INTEGER, val VARCHAR, PRIMARY KEY id)")
	require.NoError(t, err)
	_, err = db.Exec("UPSERT INTO post_use (id, val) VALUES (1, 'in-second-db')")
	require.NoError(t, err)

	_, err = db.Exec("USE DATABASE defaultdb")
	require.NoError(t, err)

	_, err = db.Exec("SELECT * FROM post_use")
	require.Error(t, err) // table doesn't exist in defaultdb

	// Switching to a database that does not exist must surface an error
	// without leaving the session in a half-bound state.
	_, err = db.Exec("USE DATABASE no_such_db")
	require.Error(t, err)

	// Sanity: defaultdb is still usable after the failed switch.
	_, err = db.Exec("CREATE TABLE in_defaultdb (id INTEGER, PRIMARY KEY id)")
	require.NoError(t, err)
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
