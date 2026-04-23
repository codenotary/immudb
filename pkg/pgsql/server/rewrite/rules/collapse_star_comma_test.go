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

// TestCollapseStarCommaList pins the XORM-shape reductions: any
// SELECT target list containing a bare `*` alongside other items
// collapses to just `*`.
func TestCollapseStarCommaList(t *testing.T) {
	cases := []string{
		`SELECT *, id FROM t`,
		`SELECT *, id, name FROM t`,
		`SELECT id, *, name FROM t`,
		`SELECT *, project_issue.issue_id FROM project`,
	}

	r := rewrite.New().WithRule(rules.CollapseStarCommaList{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			// Post-rewrite the target list should be a bare `*` —
			// no comma between SELECT and FROM.
			upper := strings.ToUpper(out)
			selectIdx := strings.Index(upper, "SELECT ")
			fromIdx := strings.Index(upper, " FROM ")
			require.GreaterOrEqual(t, selectIdx, 0)
			require.Greater(t, fromIdx, selectIdx)
			targetList := strings.TrimSpace(out[selectIdx+len("SELECT "):fromIdx])
			require.Equal(t, "*", targetList,
				"target list should collapse to bare *: in=%q out=%q",
				sql, out)
		})
	}
}

// TestCollapseStarCommaList_PreservesWithoutStar guards against
// touching target lists that don't contain a bare `*`.
func TestCollapseStarCommaList_PreservesWithoutStar(t *testing.T) {
	r := rewrite.New().WithRule(rules.CollapseStarCommaList{})
	cases := []string{
		`SELECT id, name FROM t`,
		`SELECT * FROM t`, // bare * alone — don't over-strip.
	}
	for _, sql := range cases {
		out, err := r.Rewrite(sql)
		require.NoError(t, err, "sql=%q", sql)
		require.NotEmpty(t, out)
	}
}
