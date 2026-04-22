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

// TestStripTrailingCommentOn_DropsCommentStatements covers the
// pg_dump shape: DDL followed by `; COMMENT ON …`. The DDL must
// survive; the COMMENT ON must vanish.
func TestStripTrailingCommentOn_DropsCommentStatements(t *testing.T) {
	cases := []string{
		`CREATE TABLE t (id INTEGER); COMMENT ON TABLE t IS 'foo'`,
		`CREATE TABLE t (id INTEGER); COMMENT ON COLUMN t.id IS 'pk'`,
	}

	r := rewrite.New().WithRule(rules.StripTrailingCommentOn{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.NotContains(t, strings.ToUpper(out), "COMMENT ON",
				"COMMENT ON must be dropped: in=%q out=%q", sql, out)
			require.Contains(t, strings.ToUpper(out), "CREATE TABLE",
				"preceding DDL must survive: %q", out)
		})
	}
}

// TestStripTrailingCommentOn_PreservesNonCommentSQL guards against
// dropping unrelated statements.
func TestStripTrailingCommentOn_PreservesNonCommentSQL(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTrailingCommentOn{})
	out, err := r.Rewrite(`SELECT 1; SELECT 2`)
	require.NoError(t, err)
	require.Contains(t, out, "SELECT 1")
	require.Contains(t, out, "SELECT 2")
}

// TestStripTrailingCommentOn_StandaloneDrop covers the standalone
// COMMENT ON shape — whole statement disappears, leaving empty.
func TestStripTrailingCommentOn_StandaloneDrop(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTrailingCommentOn{})
	out, err := r.Rewrite(`COMMENT ON TABLE t IS 'foo'`)
	require.NoError(t, err)
	require.NotContains(t, strings.ToUpper(out), "COMMENT ON",
		"standalone COMMENT ON must drop too: %q", out)
}
