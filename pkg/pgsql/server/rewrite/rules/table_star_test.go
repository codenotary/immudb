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

// TestStripTableStarPrefix_Rewrites pins the core Rails AR shape:
// `SELECT "users".* FROM "users"` → `SELECT * FROM users`. The
// quoted identifier survives or is unquoted — auxten's deparser
// drops the quotes for bare lowercase names and keeps them for
// mixed-case; tolerant check below.
func TestStripTableStarPrefix_Rewrites(t *testing.T) {
	cases := []string{
		`SELECT "users".* FROM "users"`,
		`SELECT users.* FROM users`,
		`SELECT users.* FROM users WHERE id = 1`,
		`SELECT users.* FROM users ORDER BY id`,
	}

	r := rewrite.New().WithRule(rules.StripTableStarPrefix{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.NotContains(t, out, ".*",
				"StripTableStarPrefix(%q) should strip the .*, got %q", sql, out)
			require.Contains(t, out, "*",
				"bare * must survive: %q", out)
		})
	}
}

// TestStripTableStarPrefix_PreservesBareStar guards against
// over-eager matching: plain `SELECT *` must survive untouched and
// not get stripped to empty.
func TestStripTableStarPrefix_PreservesBareStar(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTableStarPrefix{})
	cases := []string{
		`SELECT * FROM users`,
		`SELECT *, id FROM users`,
	}
	for _, sql := range cases {
		out, err := r.Rewrite(sql)
		require.NoError(t, err, "sql=%q", sql)
		require.Contains(t, out, "*",
			"bare * must survive: in=%q out=%q", sql, out)
	}
}

// TestStripTableStarPrefix_PreservesColumnRefs guards qualified
// column references (which are NOT star-qualified) — they must
// survive the rule.
func TestStripTableStarPrefix_PreservesColumnRefs(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTableStarPrefix{})
	out, err := r.Rewrite(`SELECT u.id, u.name FROM users u`)
	require.NoError(t, err)
	require.Contains(t, strings.ToLower(out), "u.id")
	require.Contains(t, strings.ToLower(out), "u.name")
}

// TestStripTableStarPrefix_JoinWithQualifiedStars covers the
// multi-qualified case (Rails rarely emits it but pg_dump does).
// Both prefixes strip to bare `*`; the regex rule's "*, *"
// side-effect is documented in the rule's godoc.
func TestStripTableStarPrefix_JoinWithQualifiedStars(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTableStarPrefix{})
	out, err := r.Rewrite(`SELECT t1.*, t2.* FROM t1 JOIN t2 ON t1.id = t2.tid`)
	require.NoError(t, err)
	require.NotContains(t, out, "t1.*")
	require.NotContains(t, out, "t2.*")
}
