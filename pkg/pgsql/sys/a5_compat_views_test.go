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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPgTables_ReflectsUserTables pins the A5 contract: pg_tables has
// one row per user table with schemaname='public', tableowner='immudb',
// hasindexes=true (PK creates an implicit index on every table).
func TestPgTables_ReflectsUserTables(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE t_one (id INTEGER, PRIMARY KEY id)`)
	exec(t, e, `CREATE TABLE t_two (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e, `SELECT schemaname, tablename, tableowner, hasindexes
		FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
	require.Len(t, rows, 2)

	require.Equal(t, "public", rows[0][0])
	require.Equal(t, "t_one", rows[0][1])
	require.Equal(t, "immudb", rows[0][2])
	require.Equal(t, true, rows[0][3])
}

// TestPgTables_NameFilter exercises the query ORMs issue for existence
// probes: `SELECT 1 FROM pg_tables WHERE tablename = 'X'`. Must return
// exactly one row when the table exists, zero otherwise.
func TestPgTables_NameFilter(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE exists_check (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e,
		`SELECT tablename FROM pg_tables WHERE tablename = 'exists_check'`)
	require.Len(t, rows, 1)

	rows = query(t, e,
		`SELECT tablename FROM pg_tables WHERE tablename = 'ghost'`)
	require.Empty(t, rows)
}

// TestPgIndexes_ReflectsIndexes asserts pg_indexes enumerates every
// index (PK + user-created). The indexdef column must contain a
// parseable CREATE INDEX string.
func TestPgIndexes_ReflectsIndexes(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE products (id INTEGER, sku VARCHAR[32], PRIMARY KEY id)`)
	exec(t, e, `CREATE UNIQUE INDEX ON products (sku)`)

	rows := query(t, e,
		`SELECT tablename, indexname, indexdef FROM pg_indexes
		 WHERE tablename = 'products' ORDER BY indexname`)
	require.GreaterOrEqual(t, len(rows), 2)

	// Every row should have a non-empty indexdef starting with CREATE …
	for _, r := range rows {
		def, ok := r[2].(string)
		require.True(t, ok, "indexdef should be a string")
		require.True(t, strings.HasPrefix(def, "CREATE"),
			"indexdef %q should start with CREATE", def)
	}
}

// TestPgViews_Empty documents the current zero-row contract. Changes
// to view enumeration need to update this test.
func TestPgViews_Empty(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE backing (id INTEGER, PRIMARY KEY id)`)
	exec(t, e, `CREATE VIEW v_backing AS SELECT id FROM backing`)

	rows := query(t, e, `SELECT viewname FROM pg_views`)
	require.Empty(t, rows, "view enumeration is not yet wired through pg_views")
}

// TestPgSequences_Empty pins the zero-row contract: immudb has no
// CREATE SEQUENCE. The table exists only so ORM probes joining it
// don't break dispatcher allowlisting.
func TestPgSequences_Empty(t *testing.T) {
	e := newEngine(t)
	rows := query(t, e, `SELECT sequencename FROM pg_sequences`)
	require.Empty(t, rows)
}

// TestPgTables_JoinNamespace exercises the canonical XORM / Hibernate
// probe that JOINs pg_tables with pg_namespace via schemaname. The
// pre-A5 canned handler could not serve JOINs at all.
func TestPgTables_JoinNamespace(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE widgets (id INTEGER, PRIMARY KEY id)`)

	rows := query(t, e,
		`SELECT t.tablename, n.nspname
		 FROM pg_tables t
		 JOIN pg_namespace n ON n.nspname = t.schemaname
		 WHERE t.tablename = 'widgets'`)
	require.Len(t, rows, 1)
	require.Equal(t, "widgets", rows[0][0])
	require.Equal(t, "public", rows[0][1])
}
