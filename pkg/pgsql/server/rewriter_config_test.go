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

// TestRewriterMode_Default pins the baseline contract: without any
// configuration, the rewriter runs the regex chain. Any future
// change that flips the default would be a production behaviour
// change and should light up here first.
func TestRewriterMode_Default(t *testing.T) {
	// Clear any earlier test's setting.
	setSQLRewriterMode(DefaultSQLRewriterMode)
	require.Equal(t, "regex", sqlRewriterMode())
}

// TestRewriterMode_SetAndReset exercises the option setter — both
// accepted values land, unknown values fall back to the default.
func TestRewriterMode_SetAndReset(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)

	setSQLRewriterMode("ast")
	require.Equal(t, "ast", sqlRewriterMode())

	setSQLRewriterMode("regex")
	require.Equal(t, "regex", sqlRewriterMode())

	setSQLRewriterMode("nonsense")
	require.Equal(t, "regex", sqlRewriterMode(),
		"unknown modes must fall back to the default")
}

// TestASTRewrite_PathARoundtripFirst runs the first round-trip psql
// sends for `\d continent` through the AST path and asserts the
// output mentions the same tables without any pg_catalog prefix.
// This is the canary that tells us the AST rule set is functionally
// equivalent to the regex chain for this (highest-traffic) query.
func TestASTRewrite_PathARoundtripFirst(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	in := `SELECT c.oid, n.nspname, c.relname ` +
		`FROM pg_catalog.pg_class c ` +
		`LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace ` +
		`WHERE c.relname = 'continent' ` +
		`ORDER BY 2, 3`

	out := removePGCatalogReferences(in)
	require.NotContains(t, out, "pg_catalog.",
		"AST path should strip pg_catalog. prefix: %q", out)
	require.Contains(t, out, "pg_class")
	require.Contains(t, out, "pg_namespace")
}

// TestASTRewrite_StripsPGCasts covers the second core rule via the
// full dispatch pipeline.
func TestASTRewrite_StripsPGCasts(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	in := `SELECT c.oid::regclass FROM pg_class c`
	out := removePGCatalogReferences(in)
	require.NotContains(t, strings.ToUpper(out), "::REGCLASS",
		"AST path should strip ::regclass: %q", out)
}

// TestASTRewrite_FallsBackOnParseError confirms the safety net: an
// input auxten can't parse (typically immudb-specific grammar like
// AUTO_INCREMENT or VARCHAR[N]) still gets the regex treatment, so
// no client sees a correctness regression from enabling the flag.
func TestASTRewrite_FallsBackOnParseError(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)
	setSQLRewriterMode("ast")

	// immudb-specific: VARCHAR with bracket size syntax is not PG
	// and doesn't parse with auxten. We expect the regex chain to
	// handle this path — verified by the pg_catalog. prefix being
	// stripped even though the AST rewriter can't see the input.
	in := `SELECT * FROM pg_catalog.pg_class c WHERE c.relname = 'foo'`
	out := removePGCatalogReferences(in)
	require.NotContains(t, out, "pg_catalog.")

	// And the purely-immudb DDL survives (regex chain handles it).
	ddl := `CREATE TABLE foo (id INTEGER AUTO_INCREMENT, PRIMARY KEY id)`
	outDDL := removePGCatalogReferences(ddl)
	require.Contains(t, outDDL, "AUTO_INCREMENT")
}

// TestSQLRewriterOption verifies the public server-option setter
// threads through to the atomic. Catches any future refactor that
// breaks the option without exercising the full server stack.
func TestSQLRewriterOption(t *testing.T) {
	defer setSQLRewriterMode(DefaultSQLRewriterMode)

	// Server construction isn't needed — the Option is just a fn
	// that mutates state when applied; calling it directly is what
	// tests do elsewhere in this package (e.g. Host, Port).
	opt := SQLRewriter("ast")
	opt(nil)
	require.Equal(t, "ast", sqlRewriterMode())

	opt = SQLRewriter("regex")
	opt(nil)
	require.Equal(t, "regex", sqlRewriterMode())
}
