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

package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPgFn_QuoteIdent pins quote_ident behaviour for identifiers psql
// uses heavily: simple unquoted, mixed-case, reserved words, embedded
// quotes.
func TestPgFn_QuoteIdent(t *testing.T) {
	engine := setupCommonTest(t)

	cases := []struct {
		in, want string
	}{
		{"users", "users"},        // simple — no quoting
		{"UserTable", `"UserTable"`}, // uppercase forces quoting
		{"order", `"order"`},      // reserved word
		{"a b", `"a b"`},          // embedded space
		{`with"quote`, `"with""quote"`}, // quote gets doubled
	}

	for _, c := range cases {
		r, err := engine.Query(context.Background(), nil,
			"SELECT quote_ident('"+c.in+"')", nil)
		require.NoError(t, err)

		row, err := r.Read(context.Background())
		require.NoError(t, err, "quote_ident(%q)", c.in)
		require.Equal(t, c.want, row.ValuesByPosition[0].RawValue(),
			"quote_ident(%q)", c.in)
		r.Close()
	}
}

// TestPgFn_CurrentSchemas returns the array-literal form clients
// round-trip through array_to_string.
func TestPgFn_CurrentSchemas(t *testing.T) {
	engine := setupCommonTest(t)

	r, err := engine.Query(context.Background(), nil,
		"SELECT current_schemas(true)", nil)
	require.NoError(t, err)
	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "{pg_catalog,public}", row.ValuesByPosition[0].RawValue())
	r.Close()

	r, err = engine.Query(context.Background(), nil,
		"SELECT current_schemas(false)", nil)
	require.NoError(t, err)
	row, err = r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, "{public}", row.ValuesByPosition[0].RawValue())
	r.Close()
}

// TestPgFn_ArrayToString exercises the curly-brace array literal
// parser. Non-array inputs pass through unchanged — matches what psql
// sends when a setting is a bare scalar.
func TestPgFn_ArrayToString(t *testing.T) {
	engine := setupCommonTest(t)

	cases := []struct {
		in, want string
	}{
		{"{a,b,c}", "a, b, c"},
		{"{single}", "single"},
		{"not_an_array", "not_an_array"},
		{"{}", ""},
	}

	for _, c := range cases {
		r, err := engine.Query(context.Background(), nil,
			"SELECT array_to_string('"+c.in+"', ', ')", nil)
		require.NoError(t, err)
		row, err := r.Read(context.Background())
		require.NoError(t, err, "array_to_string(%q)", c.in)
		require.Equal(t, c.want, row.ValuesByPosition[0].RawValue(),
			"array_to_string(%q)", c.in)
		r.Close()
	}
}

// TestPgFn_ZeroSize asserts the pg_*_size stubs return 0 rather than
// NULL — psql's `\d+` formatter crashes on NULL where it expects a
// byte count.
func TestPgFn_ZeroSize(t *testing.T) {
	engine := setupCommonTest(t)

	r, err := engine.Query(context.Background(), nil,
		"SELECT pg_total_relation_size(123), pg_relation_size(456)", nil)
	require.NoError(t, err)
	defer r.Close()

	row, err := r.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(0), row.ValuesByPosition[0].RawValue())
	require.Equal(t, int64(0), row.ValuesByPosition[1].RawValue())
}

// TestPgFn_PrivilegeChecks asserts the has_*_privilege stubs return
// true for any argument shape. psql emits these from \d to decide
// whether to render grant info; returning true is safe and matches
// what pgadmin_compat.go does today.
func TestPgFn_PrivilegeChecks(t *testing.T) {
	engine := setupCommonTest(t)

	for _, q := range []string{
		"SELECT has_table_privilege('some_table', 'SELECT')",
		"SELECT has_schema_privilege('public', 'USAGE')",
		"SELECT has_database_privilege('defaultdb', 'CONNECT')",
		"SELECT has_function_privilege('current_user', 'EXECUTE')",
	} {
		r, err := engine.Query(context.Background(), nil, q, nil)
		require.NoError(t, err, q)
		row, err := r.Read(context.Background())
		require.NoError(t, err, q)
		require.Equal(t, true, row.ValuesByPosition[0].RawValue(), q)
		r.Close()
	}
}

// TestPgFn_RegisteredFunctionsAccessor asserts the accessor returns
// a map populated with the built-in functions. Used by pg_proc's
// Scan; a nil/empty return would make `\df` return zero rows.
func TestPgFn_RegisteredFunctionsAccessor(t *testing.T) {
	fns := RegisteredFunctions()
	require.NotEmpty(t, fns)

	// Spot-check that both old and new registrations are present.
	for _, name := range []string{
		"LENGTH", "NOW", "COALESCE",
		"QUOTE_IDENT", "CURRENT_SCHEMAS", "ARRAY_TO_STRING",
		"PG_TOTAL_RELATION_SIZE", "HAS_DATABASE_PRIVILEGE",
	} {
		require.NotNil(t, fns[name], "expected function %q", name)
	}
}
