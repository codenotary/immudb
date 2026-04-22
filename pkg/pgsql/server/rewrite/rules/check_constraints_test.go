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

package rules_test

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite"
	"github.com/codenotary/immudb/pkg/pgsql/server/rewrite/rules"
	"github.com/stretchr/testify/require"
)

// TestStripCheckConstraints_Rewrites covers the three shapes the
// parser emits: column-inline, table-level unnamed, and table-level
// named. All must vanish; the rest of the CREATE TABLE survives.
func TestStripCheckConstraints_Rewrites(t *testing.T) {
	cases := []string{
		`CREATE TABLE t (id INTEGER, age INTEGER CHECK (age > 0))`,
		`CREATE TABLE t (id INTEGER, CHECK (id > 0))`,
		`CREATE TABLE t (id INTEGER, CONSTRAINT positive_id CHECK (id > 0))`,
	}

	r := rewrite.New().WithRule(rules.StripCheckConstraints{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.NotContains(t, strings.ToUpper(out), "CHECK (",
				"CHECK constraint must be stripped: %q", out)
			// The columns themselves must survive.
			require.Contains(t, out, "id")
		})
	}
}

// TestStripCheckConstraints_PreservesOtherTables guards
// non-CreateTable statements and CREATE TABLEs without any CHECKs.
func TestStripCheckConstraints_PreservesOtherTables(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripCheckConstraints{})

	for _, sql := range []string{
		`CREATE TABLE t (id INTEGER, name TEXT)`,
		`SELECT 1`,
	} {
		out, err := r.Rewrite(sql)
		require.NoError(t, err)
		require.NotEmpty(t, out, "in=%q", sql)
	}
}

// TestStripCheckConstraints_NestedParenExpr demonstrates the AST
// advantage over the regex: a nested-paren CHECK expression (which
// the regex at query_machine.go:666 would mangle) is handled
// correctly because the parser has already resolved grouping.
func TestStripCheckConstraints_NestedParenExpr(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripCheckConstraints{})
	out, err := r.Rewrite(`CREATE TABLE t (id INTEGER, CHECK ((id > 0) AND (id < 100)))`)
	require.NoError(t, err)
	require.NotContains(t, strings.ToUpper(out), "CHECK",
		"nested-paren CHECK must be stripped cleanly: %q", out)
}
