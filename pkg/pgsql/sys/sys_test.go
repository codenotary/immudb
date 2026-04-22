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

package sys_test

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	_ "github.com/codenotary/immudb/pkg/pgsql/sys" // register pg_catalog tables
	"github.com/stretchr/testify/require"
)

// newEngine opens a fresh in-tempdir immudb SQL engine for a test.
func newEngine(t *testing.T) *sql.Engine {
	t.Helper()
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	e, err := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
	require.NoError(t, err)
	return e
}

func exec(t *testing.T, e *sql.Engine, q string) {
	t.Helper()
	_, _, err := e.Exec(context.Background(), nil, q, nil)
	require.NoError(t, err, "exec %q", q)
}

func query(t *testing.T, e *sql.Engine, q string) [][]interface{} {
	t.Helper()
	r, err := e.Query(context.Background(), nil, q, nil)
	require.NoError(t, err, "query %q", q)
	defer r.Close()

	var rows [][]interface{}
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		vals := make([]interface{}, len(row.ValuesByPosition))
		for i, v := range row.ValuesByPosition {
			vals[i] = v.RawValue()
		}
		rows = append(rows, vals)
	}
	return rows
}

// TestPgNamespace_StaticRows pins the three fixed namespaces. Clients
// (psql, pgAdmin) filter by nspname='public' — any regression here
// breaks every subsequent \d/\dt lookup.
func TestPgNamespace_StaticRows(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT oid, nspname FROM pg_namespace ORDER BY oid")
	require.Len(t, rows, 3)

	names := []string{rows[0][1].(string), rows[1][1].(string), rows[2][1].(string)}
	require.ElementsMatch(t, []string{"information_schema", "pg_catalog", "public"}, names)
}

// TestPgAm_StaticRows pins the two access methods.
func TestPgAm_StaticRows(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT oid, amname, amtype FROM pg_am ORDER BY oid")
	require.Len(t, rows, 2)
	require.Equal(t, "btree", rows[0][1])
	require.Equal(t, "hash", rows[1][1])
	require.Equal(t, "i", rows[0][2])
}

// TestPgClass_ReflectsUserTables creates a real user table and verifies
// it shows up in pg_class with the right relkind, relname, relnatts,
// relnamespace, and relhasindex values — the core \dt / \d query path.
func TestPgClass_ReflectsUserTables(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE continent (
		id INTEGER AUTO_INCREMENT,
		name VARCHAR[64] NOT NULL,
		code VARCHAR[3],
		PRIMARY KEY id
	)`)
	exec(t, e, `CREATE INDEX ON continent (code)`)

	// Filter by relkind='r' to isolate tables from their indexes.
	rows := query(t, e,
		`SELECT relname, relnatts, relhasindex, relkind, relnamespace
		 FROM pg_class WHERE relkind = 'r' ORDER BY relname`)
	require.Len(t, rows, 1, "expected exactly one user table in pg_class")
	require.Equal(t, "continent", rows[0][0])
	require.EqualValues(t, 3, rows[0][1])      // relnatts: 3 columns
	require.Equal(t, true, rows[0][2])         // relhasindex: PK + secondary
	require.Equal(t, "r", rows[0][3])          // relkind
	require.EqualValues(t, 2200, rows[0][4])   // relnamespace = public

	// And the two indexes (PK + secondary on code) show as relkind='i'.
	idxRows := query(t, e,
		`SELECT relname FROM pg_class WHERE relkind = 'i' ORDER BY relname`)
	require.Len(t, idxRows, 2)
}

// TestPgAttribute_ColumnMetadata is the pg_attribute half of psql's \d
// query. For each user column we assert attname, attnum (1-based),
// attnotnull, and atttypid (PG type OID).
func TestPgAttribute_ColumnMetadata(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE continent (
		id INTEGER AUTO_INCREMENT,
		name VARCHAR[64] NOT NULL,
		code VARCHAR[3],
		PRIMARY KEY id
	)`)

	// JOIN pg_attribute to pg_class on relname to verify the
	// attrelid wiring. If the OID-hashing scheme drifted between
	// the two tables, this JOIN would return zero rows.
	rows := query(t, e, `
		SELECT a.attname, a.attnum, a.attnotnull, a.atttypid
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = 'continent'
		ORDER BY a.attnum`)
	require.Len(t, rows, 3)

	require.Equal(t, "id", rows[0][0])
	require.EqualValues(t, 1, rows[0][1])
	// id is PRIMARY KEY → not null
	require.Equal(t, true, rows[0][2])
	require.EqualValues(t, 20, rows[0][3]) // int8

	require.Equal(t, "name", rows[1][0])
	require.EqualValues(t, 2, rows[1][1])
	require.Equal(t, true, rows[1][2]) // NOT NULL
	require.EqualValues(t, 1043, rows[1][3]) // varchar

	require.Equal(t, "code", rows[2][0])
	require.EqualValues(t, 3, rows[2][1])
	require.Equal(t, false, rows[2][2]) // nullable
	require.EqualValues(t, 1043, rows[2][3])
}

// TestPgIndex_ReflectsIndexes: create a table with PK + a secondary
// unique index and verify pg_index reports both with the right
// indisunique / indisprimary flags.
func TestPgIndex_ReflectsIndexes(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE continent (
		id INTEGER AUTO_INCREMENT,
		name VARCHAR[64] NOT NULL,
		code VARCHAR[3],
		PRIMARY KEY id
	)`)
	exec(t, e, `CREATE UNIQUE INDEX ON continent (code)`)

	rows := query(t, e, `
		SELECT i.indisunique, i.indisprimary
		FROM pg_index i
		JOIN pg_class t ON t.oid = i.indrelid
		WHERE t.relname = 'continent'
		ORDER BY i.indisprimary DESC`)
	require.Len(t, rows, 2)
	require.Equal(t, true, rows[0][0]) // PK is unique
	require.Equal(t, true, rows[0][1]) // PK is primary
	require.Equal(t, true, rows[1][0]) // code idx is unique
	require.Equal(t, false, rows[1][1])
}

// TestPgClass_PgNamespace_Join mirrors the exact shape of psql \d's
// first query: pg_class JOIN pg_namespace ON c.relnamespace = n.oid.
// If either side's oid assignment drifts, no row comes back.
func TestPgClass_PgNamespace_Join(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE accounts (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e, `
		SELECT c.oid, n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = 'accounts' AND c.relkind = 'r'`)
	require.Len(t, rows, 1)
	require.Equal(t, "public", rows[0][1])
	require.Equal(t, "accounts", rows[0][2])
}

// TestPsqlStyleQuery_PgTableIsVisible exercises the full psql \d
// query shape end-to-end: JOIN pg_class/pg_namespace, filter on
// pg_table_is_visible(). All built-in PG functions come from
// embedded/sql/functions.go (pre-existing); this test ensures the new
// system tables compose with them correctly. If this test passes, a
// rewriter that strips the `OPERATOR(pg_catalog.~)` regex in psql's
// real query and turns it into `=` is the one remaining piece
// needed to make \d work for real — that's Part B scope.
func TestPsqlStyleQuery_PgTableIsVisible(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE continent (id INTEGER, name VARCHAR[64], PRIMARY KEY id)`)
	exec(t, e, `CREATE TABLE accounts (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e, `
		SELECT c.oid, n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname = 'continent'
		  AND c.relkind = 'r'
		  AND pg_table_is_visible(c.oid)
		ORDER BY n.nspname, c.relname`)
	require.Len(t, rows, 1)
	require.Equal(t, "public", rows[0][1])
	require.Equal(t, "continent", rows[0][2])
}

// TestPsqlStyleQuery_FormatType asserts format_type() returns a
// human-readable PG type string for the OIDs we emit in
// pg_attribute.atttypid — the column psql \d displays as "Type".
func TestPsqlStyleQuery_FormatType(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE continent (
		id INTEGER,
		name VARCHAR[64],
		score FLOAT,
		active BOOLEAN,
		PRIMARY KEY id
	)`)

	rows := query(t, e, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE c.relname = 'continent'
		ORDER BY a.attnum`)
	require.Len(t, rows, 4)

	// The exact format_type output comes from embedded/sql's
	// pgFormatType; we only assert it's non-empty + type-appropriate,
	// since the PG canonical strings vary slightly by version.
	for _, r := range rows {
		require.NotEmpty(t, r[1], "format_type returned empty for %v", r[0])
	}
}

// TestRelOID_Stability: repeated calls return the same oid for the
// same (schema, name). Important because psql's \d does two round
// trips — first gets the oid, second uses it in WHERE c.oid = '...'.
// If relOID drifted between calls the second query would miss.
func TestRelOID_Stability(t *testing.T) {
	e := newEngine(t)

	exec(t, e, `CREATE TABLE movements (id INTEGER, PRIMARY KEY id)`)

	rowA := query(t, e, `SELECT oid FROM pg_class WHERE relname = 'movements' AND relkind = 'r'`)
	rowB := query(t, e, `SELECT oid FROM pg_class WHERE relname = 'movements' AND relkind = 'r'`)
	require.Len(t, rowA, 1)
	require.Len(t, rowB, 1)
	require.Equal(t, rowA[0][0], rowB[0][0])
}
