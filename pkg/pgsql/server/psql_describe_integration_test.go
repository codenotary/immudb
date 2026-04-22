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

// TestPsqlDescribe_EndToEnd feeds the two SQL strings psql's \d
// meta-command sends (verbatim from a real transcript) through the
// rewrite pipeline and asserts the resulting statements execute
// cleanly against the engine backed by the pkg/pgsql/sys system
// tables. If any of the rewrites, dispatcher rules, or system-table
// registrations drift out of sync, this test will catch it.
//
// The blank-import side-effect registrations happen because this file
// lives in package server which imports pkg/pgsql/sys in server.go.
func TestPsqlDescribe_EndToEnd(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	engine, err := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
	require.NoError(t, err)

	// Create a user table so pg_class / pg_attribute have something
	// to report.
	_, _, err = engine.Exec(context.Background(), nil, `CREATE TABLE continent (
		id INTEGER AUTO_INCREMENT,
		name VARCHAR[64] NOT NULL,
		code VARCHAR[3],
		PRIMARY KEY id
	)`, nil)
	require.NoError(t, err)

	// -- First round-trip ------------------------------------------------
	// This is the exact string psql sends for \d continent (whitespace
	// normalised to a single line for test readability; the rewrite
	// layer is whitespace-insensitive).
	firstSQL := `SELECT c.oid, n.nspname, c.relname ` +
		`FROM pg_catalog.pg_class c ` +
		`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
		`WHERE c.relname OPERATOR(pg_catalog.~) '^(continent)$' COLLATE pg_catalog.default ` +
		`AND pg_catalog.pg_table_is_visible(c.oid) ` +
		`ORDER BY 2, 3`

	// Dispatcher must allow passthrough — otherwise the query hits
	// the canned handler and this test is lying about "real data".
	s := &session{}
	require.Nil(t, s.isEmulableInternally(firstSQL),
		"dispatcher should pass the psql \\d first round-trip through to the engine")

	rewritten1 := removePGCatalogReferences(firstSQL)
	r1, err := engine.Query(context.Background(), nil, rewritten1, nil)
	require.NoError(t, err, "engine rejected rewritten \\d first round-trip: %s", rewritten1)
	defer r1.Close()

	row1, err := r1.Read(context.Background())
	require.NoError(t, err, "expected at least one row for pg_class lookup of 'continent'")
	require.Len(t, row1.ValuesByPosition, 3)
	require.Equal(t, "public", row1.ValuesByPosition[1].RawValue())
	require.Equal(t, "continent", row1.ValuesByPosition[2].RawValue())

	// Extract the oid to feed into the second round-trip.
	oid := row1.ValuesByPosition[0].RawValue()

	// -- Second round-trip ----------------------------------------------
	// psql now asks pg_class (and joins pg_am) for the detailed
	// relation metadata keyed by the oid returned above. The
	// '16384'-style string-quoted oid literal is what the oid-
	// coercion rewrite in normalizePsqlPatterns handles.
	secondSQL := `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers, ` +
		`c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, ` +
		`c.relispartition, '', c.reltablespace, ` +
		`CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype END, ` +
		`c.relpersistence, c.relreplident, am.amname ` +
		`FROM pg_catalog.pg_class c ` +
		`LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) ` +
		`LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) `
	// Build the WHERE with the literal oid we just got, exactly as
	// psql would — string-quoted.
	secondSQL += `WHERE c.oid = '` + oidToStr(t, oid) + `'`

	require.Nil(t, s.isEmulableInternally(secondSQL),
		"dispatcher should pass the psql \\d second round-trip through to the engine")

	rewritten2 := removePGCatalogReferences(secondSQL)
	r2, err := engine.Query(context.Background(), nil, rewritten2, nil)
	require.NoError(t, err, "engine rejected rewritten \\d second round-trip: %s", rewritten2)
	defer r2.Close()

	row2, err := r2.Read(context.Background())
	require.NoError(t, err, "expected one pg_class row for oid %v", oid)
	// relkind is column 1 — must be 'r' for a base table.
	require.Equal(t, "r", row2.ValuesByPosition[1].RawValue())
	// relhasindex is column 2 — must be true because PRIMARY KEY exists.
	require.Equal(t, true, row2.ValuesByPosition[2].RawValue())
}

// TestPsqlDt_EndToEnd exercises the rewrite+dispatch chain for a
// \dt-style query shape (enumerate all base tables). Same JOIN
// against pg_namespace, no oid filter. Asserts the engine returns
// one row per user table.
func TestPsqlDt_EndToEnd(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	engine, err := sql.NewEngine(st, sql.DefaultOptions().WithPrefix([]byte("sql")))
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), nil,
		`CREATE TABLE accounts (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)
	_, _, err = engine.Exec(context.Background(), nil,
		`CREATE TABLE movements (id INTEGER, PRIMARY KEY id)`, nil)
	require.NoError(t, err)

	// Approximate psql's \dt query shape — enumerate base tables in
	// public schema.
	q := `SELECT n.nspname, c.relname ` +
		`FROM pg_catalog.pg_class c ` +
		`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
		`WHERE c.relkind = 'r' AND n.nspname = 'public' ` +
		`ORDER BY 1, 2`

	s := &session{}
	require.Nil(t, s.isEmulableInternally(q))

	r, err := engine.Query(context.Background(), nil,
		removePGCatalogReferences(q), nil)
	require.NoError(t, err)
	defer r.Close()

	var names []string
	for {
		row, err := r.Read(context.Background())
		if err == sql.ErrNoMoreRows {
			break
		}
		require.NoError(t, err)
		names = append(names, row.ValuesByPosition[1].RawValue().(string))
	}
	require.ElementsMatch(t, []string{"accounts", "movements"}, names)
}

// oidToStr converts the oid TypedValue raw value (int64 or similar)
// to the string form psql would emit in its second round-trip.
func oidToStr(t *testing.T, v interface{}) string {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return intToStr(x)
	case int:
		return intToStr(int64(x))
	default:
		t.Fatalf("unexpected oid type %T: %v", v, v)
		return ""
	}
}

func intToStr(n int64) string {
	// Small alloc-free helper to avoid pulling strconv just here.
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
