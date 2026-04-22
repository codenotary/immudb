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

// TestStripCollate_RemovesCollateClause pins the contract: any
// COLLATE qualifier is dropped, regardless of the collation name.
// psql / pgAdmin emit COLLATE "default" routinely; every one must
// unwrap to the bare inner expression.
func TestStripCollate_RemovesCollateClause(t *testing.T) {
	cases := []string{
		`SELECT 'x' COLLATE "default"`,
		`SELECT 'x' COLLATE "C"`,
		`SELECT 'x' COLLATE default`,
		`SELECT * FROM t WHERE name COLLATE "C" = 'foo'`,
	}

	r := rewrite.New().WithRule(rules.StripCollate{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.NotContains(t, strings.ToUpper(out), "COLLATE",
				"StripCollate(%q) should not contain COLLATE, got %q", sql, out)
		})
	}
}

// TestStripCollate_ParseFailFallback documents the current
// limitation: the `COLLATE pg_catalog.default` form (dotted
// collation name) doesn't parse with auxten, so Rewriter.Rewrite
// returns an error and the caller falls back to the regex chain.
// This test locks in that behaviour so we don't silently stop
// emitting a parse error.
func TestStripCollate_ParseFailFallback(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripCollate{})
	in := `SELECT 'x' COLLATE pg_catalog.default`
	out, err := r.Rewrite(in)
	require.Error(t, err, "dotted COLLATE name must fail to parse")
	require.Equal(t, in, out, "fallback path must return the input unchanged")
}
