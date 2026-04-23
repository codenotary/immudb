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

// TestPgDatabase_SingleRow asserts pg_database always returns a single
// row for the current database. That's what psql's `\l` needs to show
// *something*; multi-database enumeration would require plumbing the
// server-level database manager through the engine tx, deferred to
// A3+.
func TestPgDatabase_SingleRow(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT oid, datname, datallowconn FROM pg_database")
	require.Len(t, rows, 1)
	require.Equal(t, "defaultdb", rows[0][1])
	require.Equal(t, true, rows[0][2])
}

// TestPgRoles_FallbackOnEmpty asserts that when the tx doesn't surface
// any users (e.g. no auth context in an in-process engine test) we
// fall back to a synthetic `immudb` superuser row rather than an empty
// result. psql's `\du` treats an empty pg_roles as a configuration
// error and prints a confusing warning.
func TestPgRoles_FallbackOnEmpty(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT rolname, rolsuper, rolcanlogin FROM pg_roles")
	require.NotEmpty(t, rows)
	// Guaranteed fallback row — at least the admin exists.
	found := false
	for _, r := range rows {
		if r[0] == "immudb" {
			require.Equal(t, true, r[1], "immudb should be superuser")
			require.Equal(t, true, r[2], "immudb should be login-capable")
			found = true
		}
	}
	require.True(t, found, "expected immudb role in pg_roles fallback")
}

// TestPgSettings_HasServerVersion pins the one row every PG client
// reads on connect — a missing `server_version` setting makes psql
// fall back to a pre-8.0 protocol path and report scary warnings.
func TestPgSettings_HasServerVersion(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT name, setting FROM pg_settings WHERE name = 'server_version'")
	require.Len(t, rows, 1)
	require.Equal(t, "server_version", rows[0][0])
	// Whatever version we advertise, it must look like PG (N.M form).
	setting, ok := rows[0][1].(string)
	require.True(t, ok, "server_version must be a string")
	require.True(t, strings.ContainsRune(setting, '.'), "server_version %q must look version-y", setting)
}

// TestPgSettings_StandardConformingStrings guards the GUC that pq (Go
// driver) reads on connect to decide whether to escape backslashes in
// string literals. A missing row here would make pq fall back to its
// "ancient server" encoding path.
func TestPgSettings_StandardConformingStrings(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT setting FROM pg_settings WHERE name = 'standard_conforming_strings'")
	require.Len(t, rows, 1)
	require.Equal(t, "on", rows[0][0])
}

// TestPgConstraint_PrimaryKey verifies a table with a PRIMARY KEY
// shows up as a contype='p' row in pg_constraint with the expected
// naming convention (<table>_pkey) and conkey attnum vector.
func TestPgConstraint_PrimaryKey(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE accounts (id INTEGER, email VARCHAR[64], PRIMARY KEY id)`)

	rows := query(t, e,
		`SELECT conname, contype, conkey FROM pg_constraint
		 WHERE conname = 'accounts_pkey'`)
	require.Len(t, rows, 1)
	require.Equal(t, "accounts_pkey", rows[0][0])
	require.Equal(t, "p", rows[0][1])
	// conkey is a PG array literal; for a single-col PK it's `{1}`.
	require.Equal(t, "{1}", rows[0][2])
}

// TestPgConstraint_Unique verifies a CREATE UNIQUE INDEX generates a
// matching pg_constraint row with contype='u'. This is the row psql
// `\d` reads to render "Indexes:" and "Check constraints:" blocks.
func TestPgConstraint_Unique(t *testing.T) {
	e := newEngine(t)
	exec(t, e, `CREATE TABLE users (id INTEGER, email VARCHAR[64], PRIMARY KEY id)`)
	exec(t, e, `CREATE UNIQUE INDEX ON users (email)`)

	rows := query(t, e,
		`SELECT contype FROM pg_constraint WHERE contype = 'u'`)
	require.GreaterOrEqual(t, len(rows), 1, "expected at least one unique constraint row")
}

// TestPgDescription_Empty pins the current "no comments" behaviour.
// Registering pg_description (even empty) matters: without it the
// dispatcher's allPgRefsRegistered would reject any query that joins
// pg_description and fall through to the canned handler.
func TestPgDescription_Empty(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT description FROM pg_description")
	require.Empty(t, rows)
}

// TestPgProc_ContainsCoreFunctions asserts pg_proc enumerates the
// built-in functions registered in embedded/sql/functions.go. psql's
// `\df` lists rows from this table; a missing row means `\df` shows an
// incomplete inventory.
func TestPgProc_ContainsCoreFunctions(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT proname FROM pg_proc")
	require.NotEmpty(t, rows)

	got := map[string]bool{}
	for _, r := range rows {
		if name, ok := r[0].(string); ok {
			got[name] = true
		}
	}
	// Spot-check a few canonical functions.
	for _, want := range []string{
		"coalesce", "length", "now", "pg_table_is_visible",
		"current_database", "format_type", "quote_ident",
	} {
		require.True(t, got[want], "expected pg_proc row for %q, got %v", want, got)
	}
}

// TestPgType_HasCoreTypes exercises the expanded pg_type contract:
// clients (Rails, psql \dT) hardcode OIDs for the 10-or-so types
// every PG driver knows about. A regression here silently breaks
// Rails' type decoding.
func TestPgType_HasCoreTypes(t *testing.T) {
	e := newEngine(t)

	rows := query(t, e, "SELECT oid, typname FROM pg_type")
	require.GreaterOrEqual(t, len(rows), 20)

	got := map[string]string{}
	for _, r := range rows {
		oid, _ := r[0].(string)
		name, _ := r[1].(string)
		got[oid] = name
	}
	// Canonical OIDs used by Rails/psql/pq.
	require.Equal(t, "bool", got["16"])
	require.Equal(t, "bytea", got["17"])
	require.Equal(t, "int8", got["20"])
	require.Equal(t, "int4", got["23"])
	require.Equal(t, "text", got["25"])
	require.Equal(t, "varchar", got["1043"])
	require.Equal(t, "timestamp", got["1114"])
	require.Equal(t, "uuid", got["2950"])
	require.Equal(t, "jsonb", got["3802"])
}
