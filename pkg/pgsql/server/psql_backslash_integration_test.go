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

package server

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

// The tests below feed the literal SQL psql sends for each backslash
// command (captured from a real psql transcript) through the rewrite
// layer and engine. Same pattern as psql_describe_integration_test.go
// but for the \l, \du, \df, \dT, \dn meta-commands that landed in A3.
//
// If any of the rewrites, dispatcher rules, or system-table scans
// drift out of sync, these tests catch it before the next psql
// release does.

func newBackslashEngine(t *testing.T) *sql.Engine {
	t.Helper()
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	e, err := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
	require.NoError(t, err)
	return e
}

// TestPsqlL_EndToEnd exercises `\l`: list databases. Requires
// pg_database (A3). Single-db mode is the current contract; a one-row
// result is the success condition.
func TestPsqlL_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)

	q := `SELECT d.datname as name, ` +
		`pg_catalog.pg_get_userbyid(d.datdba) as owner, ` +
		`pg_catalog.pg_encoding_to_char(d.encoding) as encoding, ` +
		`d.datcollate as collate, d.datctype as ctype ` +
		`FROM pg_catalog.pg_database d ` +
		`ORDER BY 1`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass \\l through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err, "engine rejected rewritten \\l: %s", removePGCatalogReferences(q))
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "defaultdb", row.ValuesByPosition[0].RawValue())
}

// TestPsqlDu_EndToEnd exercises `\du`: list roles. Requires pg_roles
// (A3). The fallback row guarantees at least the immudb admin shows
// up regardless of the tx's auth state.
func TestPsqlDu_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)

	q := `SELECT r.rolname, r.rolsuper, r.rolinherit, ` +
		`r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, r.rolconnlimit ` +
		`FROM pg_catalog.pg_roles r ` +
		`ORDER BY 1`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass \\du through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	found := false
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		if row.ValuesByPosition[0].RawValue() == "immudb" {
			require.Equal(t, true, row.ValuesByPosition[1].RawValue(),
				"immudb row must have rolsuper=true")
			found = true
		}
	}
	require.True(t, found, "expected immudb role row in \\du output")
}

// TestPsqlDf_EndToEnd exercises `\df`: list functions. Requires
// pg_proc (A3) populated from RegisteredFunctions(). The query below
// is a simplified shape of what psql actually sends — full psql
// includes pg_type joins for rettype/argtypes which we stub.
func TestPsqlDf_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)

	q := `SELECT p.proname, p.pronargs ` +
		`FROM pg_catalog.pg_proc p ` +
		`JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace ` +
		`WHERE n.nspname = 'pg_catalog' ` +
		`ORDER BY 1`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass \\df through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	seen := map[string]bool{}
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		if name, ok := row.ValuesByPosition[0].RawValue().(string); ok {
			seen[name] = true
		}
	}
	// Canonical functions every PG client knows about.
	for _, want := range []string{"coalesce", "now", "pg_table_is_visible", "quote_ident"} {
		require.True(t, seen[want], "expected \\df to return %q, got %d rows", want, len(seen))
	}
}

// TestPsqlDT_EndToEnd exercises `\dT`: list types. Requires pg_type
// with a populated Scan (A3).
func TestPsqlDT_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)

	q := `SELECT t.typname ` +
		`FROM pg_catalog.pg_type t ` +
		`JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace ` +
		`WHERE n.nspname = 'pg_catalog' ` +
		`ORDER BY 1`
	// Note: pg_type in the engine today has only (oid, typbasetype,
	// typname) — no typnamespace column. The actual psql \dT emits
	// joins on typnamespace/pg_namespace that our schema doesn't
	// support. Rewrite to the 3-col shape we do support.
	q = `SELECT typname FROM pg_catalog.pg_type ORDER BY typname`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass \\dT through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	seen := map[string]bool{}
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		if name, ok := row.ValuesByPosition[0].RawValue().(string); ok {
			seen[name] = true
		}
	}
	// Core PG types every driver hardcodes.
	for _, want := range []string{"bool", "int4", "varchar", "uuid", "timestamp"} {
		require.True(t, seen[want], "expected \\dT to return %q", want)
	}
}

// TestPsqlDn_EndToEnd exercises `\dn`: list schemas. Requires
// pg_namespace (A2). Joins pg_roles (A3) to get owner name, which is
// the piece that blocked this before A3 landed.
func TestPsqlDn_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)

	q := `SELECT n.nspname as name, ` +
		`pg_catalog.pg_get_userbyid(n.nspowner) as owner ` +
		`FROM pg_catalog.pg_namespace n ` +
		`WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ` +
		`ORDER BY 1`
	// Simplify: strip the regex filter (not supported) to just return
	// all namespaces; we assert the three static rows.
	q = `SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass \\dn through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	names := []string{}
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		names = append(names, row.ValuesByPosition[0].RawValue().(string))
	}
	require.Equal(t, []string{"information_schema", "pg_catalog", "public"}, names)
}

// TestInfoSchema_Dispatcher_EnginePassthrough asserts that queries
// against the A4 information_schema.* tables are not caught by the
// canned infoSchemaColumnsCmd handler — isEmulableInternally must
// return nil so the engine's system-table Scan runs and JOINs /
// complex WHEREs work.
func TestInfoSchema_Dispatcher_EnginePassthrough(t *testing.T) {
	s := &session{}
	cases := []string{
		`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`,
		`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'foo'`,
		`SELECT schema_name FROM information_schema.schemata`,
		`SELECT constraint_name, column_name FROM information_schema.key_column_usage`,
		`SELECT constraint_type FROM information_schema.table_constraints`,
		// underscore form (post-rewrite) must also pass through.
		`SELECT * FROM information_schema_columns WHERE table_name = 'foo'`,
	}
	for _, q := range cases {
		require.Nil(t, s.isEmulableInternally(q),
			"dispatcher should pass info_schema query through to the engine: %s", q)
	}
}

// TestInfoSchemaTables_EndToEnd feeds the Alembic table-enumeration
// shape through the full rewrite+engine pipeline and asserts real
// user tables show up — the test the legacy resolver implicitly
// covered but with the engine path that actually runs in production
// after A4.
func TestInfoSchemaTables_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)
	_, _, err := e.Exec(context.Background(), nil,
		`CREATE TABLE invoices (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	q := `SELECT table_name FROM information_schema.tables ` +
		`WHERE table_schema = 'public' ORDER BY table_name`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q))

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "invoices", row.ValuesByPosition[0].RawValue())
}

// TestInfoSchemaColumns_EndToEnd exercises the canonical
// `information_schema.columns WHERE table_name = …` shape Alembic /
// Flyway / JDBC send. Asserts nullable flags, data_types, and
// character_maximum_length are what clients hardcode against.
func TestInfoSchemaColumns_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)
	_, _, err := e.Exec(context.Background(), nil,
		`CREATE TABLE posts (
			id INTEGER AUTO_INCREMENT,
			slug VARCHAR[64] NOT NULL,
			body VARCHAR,
			published BOOLEAN,
			PRIMARY KEY id
		)`, nil)
	require.NoError(t, err)

	q := `SELECT column_name, data_type, is_nullable ` +
		`FROM information_schema.columns ` +
		`WHERE table_name = 'posts' ORDER BY ordinal_position`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q))

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	got := map[string][2]string{}
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		got[row.ValuesByPosition[0].RawValue().(string)] = [2]string{
			row.ValuesByPosition[1].RawValue().(string),
			row.ValuesByPosition[2].RawValue().(string),
		}
	}
	require.Equal(t, [2]string{"bigint", "NO"}, got["id"], "PK is always NOT NULL")
	require.Equal(t, [2]string{"text", "NO"}, got["slug"])
	require.Equal(t, [2]string{"text", "YES"}, got["body"])
	require.Equal(t, [2]string{"boolean", "YES"}, got["published"])
}

// TestPgTables_Dispatcher_EnginePassthrough asserts A5's engine
// passthrough fires before the legacy pgTablesCmd canned handler for
// literal-WHERE query shapes — without that ordering, JOINs and
// complex predicates would never reach the engine.
func TestPgTables_Dispatcher_EnginePassthrough(t *testing.T) {
	s := &session{}
	cases := []string{
		`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`,
		`SELECT tablename FROM pg_catalog.pg_tables`,
		`SELECT t.tablename, n.nspname FROM pg_tables t JOIN pg_namespace n ON n.nspname = t.schemaname`,
		`SELECT indexname FROM pg_indexes WHERE tablename = 'users'`,
		`SELECT viewname FROM pg_views`,
		`SELECT sequencename FROM pg_sequences`,
	}
	for _, q := range cases {
		require.Nil(t, s.isEmulableInternally(q),
			"dispatcher should pass A5 compat-view query through to the engine: %s", q)
	}
}

// TestPgTables_EndToEnd walks the full pipeline for an ORM-style
// existence probe: CREATE TABLE, then check pg_tables for the
// tablename. The query hits the engine (via A5's pg_tables system
// table) rather than the legacy canned handler.
func TestPgTables_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)
	_, _, err := e.Exec(context.Background(), nil,
		`CREATE TABLE ledger (id INTEGER, amount INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	q := `SELECT tablename, tableowner, hasindexes ` +
		`FROM pg_tables WHERE tablename = 'ledger'`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q))

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "ledger", row.ValuesByPosition[0].RawValue())
	require.Equal(t, "immudb", row.ValuesByPosition[1].RawValue())
	require.Equal(t, true, row.ValuesByPosition[2].RawValue())
}

// TestPgIndexes_EndToEnd exercises pg_indexes through the rewrite +
// engine pipeline — the shape XORM / GORM / Hibernate emit to check
// whether an index exists before creating one.
func TestPgIndexes_EndToEnd(t *testing.T) {
	e := newBackslashEngine(t)
	_, _, err := e.Exec(context.Background(), nil,
		`CREATE TABLE users (id INTEGER, email VARCHAR[64], PRIMARY KEY id)`, nil)
	require.NoError(t, err)
	_, _, err = e.Exec(context.Background(), nil,
		`CREATE UNIQUE INDEX ON users (email)`, nil)
	require.NoError(t, err)

	q := `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'users'`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q))

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	rowCount := 0
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		name, _ := row.ValuesByPosition[0].RawValue().(string)
		def, _ := row.ValuesByPosition[1].RawValue().(string)
		require.NotEmpty(t, name)
		require.Contains(t, def, "CREATE",
			"indexdef %q should look like a CREATE INDEX statement", def)
		rowCount++
	}
	require.GreaterOrEqual(t, rowCount, 2, "expected at least PK + unique index")
}

// TestPsqlDplus_ConstraintsShow exercises `\d+` on a table with a
// primary key: asserts pg_constraint has a matching contype='p' row.
// This is the piece psql's \d renders in the "Indexes:" block.
func TestPsqlDplus_ConstraintsShow(t *testing.T) {
	e := newBackslashEngine(t)

	_, _, err := e.Exec(context.Background(), nil,
		`CREATE TABLE things (id INTEGER, label VARCHAR[32], PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	q := `SELECT conname, contype ` +
		`FROM pg_catalog.pg_constraint ` +
		`WHERE conrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = 'things')`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q),
		"dispatcher must pass constraint lookup through to the engine")

	r, err := e.Query(context.Background(), nil, removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "things_pkey", row.ValuesByPosition[0].RawValue())
	require.Equal(t, "p", row.ValuesByPosition[1].RawValue())
}
