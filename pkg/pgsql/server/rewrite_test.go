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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNormalizePsqlPatterns_OperatorRegex pins the core rewrite that
// turns psql's anchored regex-match operator into equality. Without
// this the engine fails to parse OPERATOR(pg_catalog.~) and psql \d
// falls back to the canned-handler path with fabricated NULLs.
func TestNormalizePsqlPatterns_OperatorRegex(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "parenthesised_anchor_with_pg_catalog_prefix",
			in:   `WHERE c.relname OPERATOR(pg_catalog.~) '^(continent)$'`,
			want: `WHERE c.relname = 'continent'`,
		},
		{
			name: "parenthesised_anchor_bare",
			in:   `WHERE c.relname OPERATOR(~) '^(accounts)$'`,
			want: `WHERE c.relname = 'accounts'`,
		},
		{
			name: "no_parens_form",
			in:   `WHERE c.relname OPERATOR(pg_catalog.~) '^mytable$'`,
			want: `WHERE c.relname = 'mytable'`,
		},
		{
			// Alternations are out of scope for Path A. Rule should
			// leave them untouched so the canned-handler fallback
			// still catches them.
			name: "alternation_not_rewritten",
			in:   `WHERE c.relname OPERATOR(pg_catalog.~) '^(a|b|c)$'`,
			want: `WHERE c.relname OPERATOR(pg_catalog.~) '^(a|b|c)$'`,
		},
		{
			// Make sure we don't accidentally rewrite unanchored
			// patterns — those are real regex usage and should fall
			// through to the canned handler.
			name: "unanchored_not_rewritten",
			in:   `WHERE c.relname OPERATOR(pg_catalog.~) 'foo'`,
			want: `WHERE c.relname OPERATOR(pg_catalog.~) 'foo'`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizePsqlPatterns(tc.in))
		})
	}
}

// Note: an earlier regex rule (psqlAlwaysZeroOidCaseRe) used to
// collapse the psql `\d <table>` CASE for always-zero oid columns
// to the bare `''` literal. That workaround has been retired — the
// engine-level fix (coerceTypesForCase in embedded/sql/stmt.go)
// now widens mixed-type CASE arms at plan time and converts at
// reduce time. See embedded/sql/case_when_widening_test.go for the
// replacement coverage.

// TestNormalizePsqlPatterns_OidLiteralCoercion pins the second
// transform psql depends on: stripping single quotes around an
// integer compared to an oid column. PostgreSQL accepts
// `c.oid = '16384'` via implicit text→oid cast; immudb doesn't.
func TestNormalizePsqlPatterns_OidLiteralCoercion(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "pg_class_oid",
			in:   `WHERE c.oid = '16384'`,
			want: `WHERE c.oid = 16384`,
		},
		{
			name: "attrelid",
			in:   `WHERE a.attrelid = '99'`,
			want: `WHERE a.attrelid = 99`,
		},
		{
			name: "indrelid",
			in:   `WHERE i.indrelid = '20001'`,
			want: `WHERE i.indrelid = 20001`,
		},
		{
			name: "qualifier_with_spaces",
			in:   `WHERE c . oid = '42'`,
			want: `WHERE c . oid = 42`,
		},
		{
			// A string column with a numeric string must not be
			// touched — bounded allowlist in psqlOidStringLiteralRe
			// is what keeps this safe.
			name: "string_column_untouched",
			in:   `WHERE t.name = '42'`,
			want: `WHERE t.name = '42'`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, normalizePsqlPatterns(tc.in))
		})
	}
}

// TestAllPgRefsRegistered exercises the helper the dispatcher uses to
// decide whether a query's pg_* references are all satisfiable by the
// SQL engine. A miss here means the query falls through to the
// canned-handler path, which is the safe default for anything we
// haven't explicitly registered.
func TestAllPgRefsRegistered(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "psql_describe_first_roundtrip",
			sql: `SELECT c.oid, n.nspname, c.relname
			      FROM pg_catalog.pg_class c
			      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
			      WHERE c.relname = 'continent'
			        AND pg_catalog.pg_table_is_visible(c.oid)
			      ORDER BY 2, 3`,
			want: true,
		},
		{
			name: "psql_describe_second_roundtrip",
			sql: `SELECT c.relchecks, c.relkind, am.amname
			      FROM pg_catalog.pg_class c
			      LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
			      LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
			      WHERE c.oid = '16384'`,
			want: true,
		},
		{
			// pg_proc is served by the A3 sys/ system table — engine
			// passthrough is correct so psql \df can execute against
			// real function metadata.
			name: "references_registered_pg_proc",
			sql:  `SELECT * FROM pg_catalog.pg_proc WHERE proname = 'foo'`,
			want: true,
		},
		{
			// pg_roles is served by pkg/pgsql/sys/ system tables (A3) —
			// engine passthrough is correct.
			name: "references_registered_pg_roles",
			sql:  `SELECT rolname FROM pg_catalog.pg_roles`,
			want: true,
		},
		{
			// pg_database is served by the A3 sys/ system table —
			// engine passthrough is correct so psql \l can execute.
			name: "references_registered_pg_database",
			sql:  `SELECT datname FROM pg_catalog.pg_database`,
			want: true,
		},
		{
			name: "only_builtin_function",
			sql:  `SELECT pg_table_is_visible(42)`,
			want: true,
		},
		{
			// A user table that happens to have a pg_ prefix must
			// correctly block engine passthrough — registering the
			// engine for such a query would give wrong results
			// (trying to read pg_class when the user meant their own
			// table). Safer to fall to canned-handler.
			name: "user_pg_prefixed_table_blocks",
			sql:  `SELECT * FROM pg_my_app_state`,
			want: false,
		},
		{
			// psql `\d` (no args) filters out pg_toast schemas with
			// `n.nspname !~ '^pg_toast'`. The `pg_toast` token inside
			// the string literal must not disqualify the query —
			// stripSingleQuotedLiterals handles that.
			name: "psql_backslash_d_with_pg_toast_in_literal",
			sql: `SELECT n.nspname, c.relname ` +
				`FROM pg_catalog.pg_class c ` +
				`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
				`WHERE c.relkind IN ('r','p','v','m','S','f','') ` +
				`AND n.nspname <> 'pg_catalog' ` +
				`AND n.nspname !~ '^pg_toast' ` +
				`AND n.nspname <> 'information_schema' ` +
				`AND pg_catalog.pg_table_is_visible(c.oid)`,
			want: true,
		},
		{
			// Embedded pg_* names inside any literal must be stripped —
			// not just psql's toast filter.
			name: "pg_name_inside_arbitrary_literal",
			sql:  `SELECT 'pg_extension matched' FROM pg_catalog.pg_class`,
			want: true,
		},
		{
			// psql `\d <table>` column detail query. FROM pg_attribute
			// with subqueries against pg_attrdef (default expr) and
			// pg_collation/pg_type (collation). Both subquery tables
			// must be registered (A6 empty stubs) or the query falls
			// to the canned pgAdminProbe handler and the user sees
			// 23 NULL rows instead of their actual column metadata.
			name: "psql_describe_columns_with_subqueries",
			sql: `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), ` +
				`(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) ` +
				` FROM pg_catalog.pg_attrdef d ` +
				` WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), ` +
				`a.attnotnull, ` +
				`(SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t ` +
				` WHERE c.oid = a.attcollation AND t.oid = a.atttypid ` +
				`       AND a.attcollation <> t.typcollation) AS attcollation ` +
				`FROM pg_catalog.pg_attribute a ` +
				`WHERE a.attrelid = '16384' AND a.attnum > 0 AND NOT a.attisdropped ` +
				`ORDER BY a.attnum`,
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, allPgRefsRegistered(tc.sql))
		})
	}
}
