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
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Integration coverage for the three emulated pg_catalog-view handlers
// introduced in 3cf2bb46 (XORM/Gitea compat): handlePgTablesQuery,
// handleXormColumnsQuery, handlePgIndexesQuery — plus the wire-level
// guarantees that ORMs gate on (non-NULL column_name, transaction
// command tags, ReadyForQuery transaction-status byte).
//
// These tests use the shared setupTestServer helper and lib/pq via
// database/sql so failures surface the same error the ORM would see.

// TestGiteaCompat_PgTablesEnumeratesCatalog exercises handlePgTablesQuery.
// Before 3cf2bb46 the server returned a canned 1-row response to any
// `SELECT … FROM pg_tables` probe, so IsTableExist-style introspection
// always reported "yes" and ORMs skipped every CREATE on restart. The
// handler now walks immudb's catalog and filters on tablename.
func TestGiteaCompat_PgTablesEnumeratesCatalog(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE gitea_users (id INTEGER AUTO_INCREMENT, name VARCHAR, PRIMARY KEY id)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE gitea_repos (id INTEGER AUTO_INCREMENT, owner_id INTEGER, PRIMARY KEY id)`)
	require.NoError(t, err)

	t.Run("existing table returns one row", func(t *testing.T) {
		rows, err := db.Query(`SELECT tablename FROM pg_tables WHERE tablename = 'gitea_users'`)
		require.NoError(t, err)
		defer rows.Close()
		var names []string
		for rows.Next() {
			var n string
			require.NoError(t, rows.Scan(&n))
			names = append(names, n)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"gitea_users"}, names,
			"handlePgTablesQuery must report the existing table and nothing else")
	})

	t.Run("missing table returns zero rows", func(t *testing.T) {
		rows, err := db.Query(`SELECT tablename FROM pg_tables WHERE tablename = 'does_not_exist'`)
		require.NoError(t, err)
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 0, count,
			"handlePgTablesQuery must NOT claim a missing table exists — that was the pre-3cf2bb46 bug")
	})

	t.Run("no filter lists created tables", func(t *testing.T) {
		rows, err := db.Query(`SELECT tablename FROM pg_tables`)
		require.NoError(t, err)
		defer rows.Close()
		seen := map[string]bool{}
		for rows.Next() {
			var n string
			require.NoError(t, rows.Scan(&n))
			seen[n] = true
		}
		require.NoError(t, rows.Err())
		require.True(t, seen["gitea_users"], "gitea_users missing from pg_tables enumeration: %v", seen)
		require.True(t, seen["gitea_repos"], "gitea_repos missing from pg_tables enumeration: %v", seen)
	})
}

// TestGiteaCompat_XormColumnsReturnsNonNullValues exercises
// handleXormColumnsQuery. XORM issues a distinctive multi-JOIN SELECT
// against pg_attribute / pg_class / pg_type / information_schema.columns
// to learn a table's column list. Before 3cf2bb46 the canned 1-row NULL
// response tripped XORM's Scan on `column_name` with
// "converting NULL to string is unsupported".
func TestGiteaCompat_XormColumnsReturnsNonNullValues(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE xorm_acct (
		id INTEGER AUTO_INCREMENT,
		email VARCHAR[128] NOT NULL,
		is_active BOOLEAN,
		PRIMARY KEY id
	)`)
	require.NoError(t, err)

	// XORM's distinctive signature: column_name, column_default … FROM pg_attribute JOIN …
	// The xormColumnsRe matcher keys on "column_name, column_default" + "FROM pg_attribute".
	// XORM passes the target table name as $1; the handler reads param1
	// and skips parsing the relname literal, so we must use a bind.
	q := `SELECT column_name, column_default, is_nullable, data_type
	       FROM pg_attribute
	       JOIN pg_class c ON c.oid = pg_attribute.attrelid
	       WHERE c.relname = $1`
	rows, err := db.Query(q, "xorm_acct")
	require.NoError(t, err, "XORM-shape pg_attribute JOIN must route to handleXormColumnsQuery without error")
	defer rows.Close()

	type col struct {
		name       string
		def        sql.NullString
		isNullable sql.NullString
		dataType   sql.NullString
	}
	var out []col
	for rows.Next() {
		var c col
		require.NoError(t, rows.Scan(&c.name, &c.def, &c.isNullable, &c.dataType),
			"Scan into string column must succeed — this is the #XORM-boot regression guard")
		out = append(out, c)
	}
	require.NoError(t, rows.Err())

	// Three columns were defined; the emulator must emit one row per.
	require.Len(t, out, 3, "expected one row per defined column")

	names := map[string]bool{}
	for _, c := range out {
		require.NotEmptyf(t, c.name, "column_name must never be NULL / empty (row: %+v)", c)
		names[c.name] = true
	}
	require.True(t, names["id"], "column_name=id missing: %v", names)
	require.True(t, names["email"], "column_name=email missing: %v", names)
	require.True(t, names["is_active"], "column_name=is_active missing: %v", names)
}

// TestGiteaCompat_PgIndexesEnumeratesIndexes exercises handlePgIndexesQuery.
// ORMs (XORM, GORM, Hibernate) read pg_indexes to decide whether a
// CREATE INDEX migration has already run. The handler must emit one row
// per index defined on the requested table, with indexdef populated.
func TestGiteaCompat_PgIndexesEnumeratesIndexes(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE gi_repo (
		id INTEGER AUTO_INCREMENT,
		owner_id INTEGER NOT NULL,
		name VARCHAR[128] NOT NULL,
		PRIMARY KEY id
	)`)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'gi_repo'`)
	require.NoError(t, err, "pg_indexes query must not error")
	defer rows.Close()

	type idx struct{ name, def string }
	var got []idx
	for rows.Next() {
		var i idx
		require.NoError(t, rows.Scan(&i.name, &i.def),
			"Scan indexname+indexdef as strings must succeed — neither may be NULL (the pre-3cf2bb46 regression)")
		got = append(got, i)
	}
	require.NoError(t, rows.Err())

	// At minimum the primary index must be reported; ORMs read this to
	// decide whether the table is already bootstrapped. The regression
	// this test guards against is NULL in indexname/indexdef, not the
	// number of rows — lib/pq's wire format for CREATE INDEX without
	// an explicit name varies and is covered by other tests.
	require.GreaterOrEqualf(t, len(got), 1,
		"handlePgIndexesQuery must report at least the primary index: %+v", got)
	for _, i := range got {
		require.NotEmptyf(t, i.name, "indexname must not be empty (row: %+v)", i)
		require.NotEmptyf(t, i.def, "indexdef must not be empty (row: %+v)", i)
		require.Contains(t, strings.ToUpper(i.def), "CREATE",
			"indexdef must be a CREATE INDEX statement: %q", i.def)
	}
}

// TestGiteaCompat_TransactionStatusByte drives a BEGIN/COMMIT/ROLLBACK
// round-trip through lib/pq. lib/pq internally checks the ReadyForQuery
// transaction-status byte ('I' idle, 'T' in-transaction, 'E' aborted);
// if the server emits the wrong byte the driver aborts with
// "unexpected transaction status idle". This is an end-to-end guard for
// the session tx-status tracking added in 3cf2bb46.
func TestGiteaCompat_TransactionStatusByte(t *testing.T) {
	_, port := setupTestServer(t)

	db, err := sql.Open("postgres",
		fmt.Sprintf("host=localhost port=%d sslmode=disable user=immudb dbname=defaultdb password=immudb", port))
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE tx_status (id INTEGER AUTO_INCREMENT, v INTEGER, PRIMARY KEY id)`)
	require.NoError(t, err)

	// BEGIN ... COMMIT via database/sql's Tx interface — lib/pq reads
	// the ReadyForQuery status byte after every message and would error
	// if the server emits 'I' when it should emit 'T'.
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO tx_status (v) VALUES (1)`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO tx_status (v) VALUES (2)`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Separate BEGIN + ROLLBACK cycle.
	tx2, err := db.Begin()
	require.NoError(t, err)
	_, err = tx2.Exec(`INSERT INTO tx_status (v) VALUES (99)`)
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback())

	// Verify the committed rows are present and the rolled-back row is
	// not. This doubles as an INSERT-command-tag check: lib/pq reads
	// the CommandComplete tag after each INSERT and errors on "ok".
	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM tx_status`).Scan(&n))
	require.Equal(t, 2, n,
		"two inserts committed, one rolled back — tx-status byte + command tags must have let the driver track it")
}
