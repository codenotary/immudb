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

// TestNormalizeCountOne_Rewrites pins the positive cases: any
// COUNT(1) aggregate — in SELECT, WHERE, HAVING, or ORDER BY —
// normalises to COUNT(*). This is the XORM / Gitea shape the
// regex at query_machine.go:625 handles.
func TestNormalizeCountOne_Rewrites(t *testing.T) {
	cases := []string{
		`SELECT COUNT(1) FROM t`,
		`SELECT comment_id, COUNT(1) AS n FROM t GROUP BY comment_id`,
		`SELECT * FROM t HAVING COUNT(1) > 1`,
		`SELECT * FROM t WHERE COUNT(1) > 0`,
		// Case-insensitive match — Gitea mixes cases.
		`SELECT count(1) FROM t`,
	}

	r := rewrite.New().WithRule(rules.NormalizeCountOne{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			upper := strings.ToUpper(out)
			require.NotContains(t, upper, "COUNT(1)",
				"NormalizeCountOne(%q) should strip COUNT(1), got %q", sql, out)
			require.Contains(t, upper, "COUNT(*)",
				"expected COUNT(*) in output: %q", out)
		})
	}
}

// TestNormalizeCountOne_PreservesOtherAggregates guards against
// over-reach. Only COUNT(1) rewrites; other COUNT forms and
// unrelated aggregates must survive untouched.
func TestNormalizeCountOne_PreservesOtherAggregates(t *testing.T) {
	cases := []string{
		`SELECT COUNT(*) FROM t`,
		`SELECT COUNT(col) FROM t`,
		`SELECT COUNT(DISTINCT col) FROM t`,
		`SELECT COUNT(2) FROM t`,   // different literal
		`SELECT SUM(1) FROM t`,     // different function
		`SELECT LENGTH('1') FROM t`, // non-aggregate with literal
	}

	r := rewrite.New().WithRule(rules.NormalizeCountOne{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			// A roundtrip through auxten changes casing and spacing;
			// just confirm the core pieces survive.
			require.Contains(t, strings.ToLower(out), strings.ToLower(
				strings.SplitN(sql, " FROM", 2)[0][7:]),
				"aggregate must survive untouched: in=%q out=%q", sql, out)
		})
	}
}

// TestNormalizeCountOne_ParseFailFallback confirms the parse-error
// path is not broken by the rule. An unparseable input reaches the
// fallback path via Rewriter.Rewrite's error return — the rule
// itself never runs on it.
func TestNormalizeCountOne_ParseFailFallback(t *testing.T) {
	r := rewrite.New().WithRule(rules.NormalizeCountOne{})
	_, err := r.Rewrite(`SELECT COUNT(1 FROM t`)
	require.Error(t, err, "malformed SQL must fail to parse")
}
