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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestASTRewrite_StressBattery exercises the full 14-rule AST
// pipeline through one call each of removePGCatalogReferences with
// the flag flipped to "ast". Each case names the rule it targets
// and asserts (a) the output is non-empty, (b) the expected rewrite
// took effect, and (c) nothing panicked. This is the "run tests
// with the new features" smoke — any regression in a rule surfaces
// here before reaching the integration suite.
func TestASTRewrite_StressBattery(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	cases := []struct {
		rule       string
		in         string
		wantAbsent []string // substrings that must NOT appear (rewrite effect)
		wantPresent []string // substrings that must appear (survivors)
	}{
		// B1 rules
		{
			rule:       "StripSchemaQualifier",
			in:         `SELECT * FROM pg_catalog.pg_class`,
			wantAbsent: []string{"pg_catalog."},
		},
		{
			rule:        "StripSchemaQualifier (info_schema)",
			in:          `SELECT * FROM information_schema.tables`,
			wantPresent: []string{"information_schema_tables"},
			wantAbsent:  []string{"information_schema.tables"},
		},
		{
			rule:       "StripPGCasts",
			in:         `SELECT c.oid::regclass FROM pg_class c`,
			wantAbsent: []string{"::REGCLASS", "::regclass"},
		},
		{
			rule:       "StripCollate",
			in:         `SELECT 'x' COLLATE "default"`,
			wantAbsent: []string{"COLLATE"},
		},
		// B2 rules
		{
			rule:        "NormalizeCountOne",
			in:          `SELECT COUNT(1) FROM t`,
			wantPresent: []string{"count(*)"},
			wantAbsent:  []string{"count(1)", "COUNT(1)"},
		},
		{
			rule:       "StripTableStarPrefix",
			in:         `SELECT users.* FROM users`,
			wantAbsent: []string{"users.*"},
		},
		{
			rule:        "StripOnConflictColumns",
			in:          `INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING`,
			wantPresent: []string{"ON CONFLICT DO NOTHING"},
			wantAbsent:  []string{"ON CONFLICT (a)"},
		},
		{
			rule:       "StripCheckConstraints",
			in:         `CREATE TABLE t (id INTEGER, age INTEGER CHECK (age > 0))`,
			wantAbsent: []string{"CHECK ("},
		},
		{
			rule:       "StripForeignKeys",
			in:         `CREATE TABLE orders (id INTEGER, uid INTEGER REFERENCES users(id))`,
			wantAbsent: []string{"REFERENCES"},
		},
		{
			rule:       "StripCreateIndexName",
			in:         `CREATE INDEX idx_foo ON t (x)`,
			wantAbsent: []string{"idx_foo"},
		},
		{
			rule:       "StripCreateViewColList",
			in:         `CREATE VIEW v(a, b) AS SELECT 1, 2`,
			wantAbsent: []string{"(a, b)", "(a,b)"},
		},
		{
			rule:        "EnsureCreateTableIfNotExists",
			in:          `CREATE TABLE users (id INTEGER)`,
			wantPresent: []string{"IF NOT EXISTS"},
		},
		{
			rule:       "StripTransactionModes",
			in:         `BEGIN READ WRITE`,
			wantAbsent: []string{"READ WRITE"},
		},
		{
			rule:       "CollapseStarCommaList",
			in:         `SELECT *, id FROM t`,
			wantAbsent: []string{", id"},
		},
		{
			rule:       "StripTrailingCommentOn",
			in:         `CREATE TABLE t (id INTEGER); COMMENT ON TABLE t IS 'foo'`,
			wantAbsent: []string{"COMMENT ON"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.rule, func(t *testing.T) {
			out := removePGCatalogReferences(tc.in)
			require.NotEmpty(t, out, "empty output for %q", tc.in)

			upper := strings.ToUpper(out)
			for _, want := range tc.wantPresent {
				require.Contains(t, strings.ToLower(out), strings.ToLower(want),
					"%s expected %q in output. in=%q out=%q",
					tc.rule, want, tc.in, out)
			}
			for _, absent := range tc.wantAbsent {
				require.NotContains(t, upper, strings.ToUpper(absent),
					"%s should not contain %q. in=%q out=%q",
					tc.rule, absent, tc.in, out)
			}
		})
	}
}

// TestASTRewrite_CompositeQueryAllRules fires a single complex
// query that triggers MULTIPLE rules in one pass. Validates rule
// ordering — if the pipeline's fixed order is wrong, individual
// rules can mutually interfere and this test catches it.
func TestASTRewrite_CompositeQueryAllRules(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	// Shape observed in a real Rails + XORM mixed environment:
	// psql-ish schema-qualified FROM, AR-style table.* projection,
	// a PG cast, a bare-star collapse, and a count-one aggregate.
	in := `SELECT users.*, COUNT(1), id::regclass FROM pg_catalog.users`

	out := removePGCatalogReferences(in)
	require.NotEmpty(t, out)

	// Every rule's target must be gone.
	upper := strings.ToUpper(out)
	require.NotContains(t, upper, "PG_CATALOG.")
	require.NotContains(t, upper, "::REGCLASS")
	require.NotContains(t, upper, "USERS.*")
	require.NotContains(t, upper, "COUNT(1)")
}

// TestASTRewrite_IdempotentOverRegex asserts a critical invariant:
// running removePGCatalogReferences twice produces the same output
// the second time. Without idempotency we'd see drift on prepared-
// statement caches (query_machine.go:393 uses the rewritten SQL as
// the cache key).
func TestASTRewrite_IdempotentOverRegex(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	queries := []string{
		`SELECT * FROM pg_catalog.pg_class`,
		`SELECT c.oid::regclass FROM pg_class c`,
		`INSERT INTO t VALUES (1) ON CONFLICT (a) DO NOTHING`,
		`CREATE TABLE users (id INTEGER)`,
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			once := removePGCatalogReferences(q)
			twice := removePGCatalogReferences(once)
			require.Equal(t, once, twice,
				"rewrite must be idempotent: in=%q once=%q twice=%q",
				q, once, twice)
		})
	}
}
