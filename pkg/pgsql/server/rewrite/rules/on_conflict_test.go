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

// TestStripOnConflictColumns_Rewrites covers both Rails shapes:
// DO NOTHING and DO UPDATE SET. The column list must vanish; the
// DO clause must survive unchanged.
func TestStripOnConflictColumns_Rewrites(t *testing.T) {
	cases := []struct {
		in    string
		wantContain []string
		wantAbsent  []string
	}{
		{
			in:          `INSERT INTO t(a,b) VALUES (1,2) ON CONFLICT (a) DO NOTHING`,
			wantContain: []string{"ON CONFLICT DO NOTHING"},
			wantAbsent:  []string{"ON CONFLICT (a)", "ON CONFLICT(a)"},
		},
		{
			in: `INSERT INTO t(a,b) VALUES (1,2) ON CONFLICT (a,b) DO UPDATE SET b = 3`,
			wantContain: []string{"ON CONFLICT DO UPDATE"},
			wantAbsent:  []string{"ON CONFLICT (a", "ON CONFLICT(a"},
		},
	}

	r := rewrite.New().WithRule(rules.StripOnConflictColumns{})
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := r.Rewrite(tc.in)
			require.NoError(t, err)
			upper := strings.ToUpper(out)
			for _, want := range tc.wantContain {
				require.Contains(t, upper, want,
					"expected %q in output: %q", want, out)
			}
			for _, absent := range tc.wantAbsent {
				require.NotContains(t, upper, absent,
					"did not expect %q in output: %q", absent, out)
			}
		})
	}
}

// TestStripOnConflictColumns_PreservesPlainInsert guards against
// touching INSERTs without an ON CONFLICT clause.
func TestStripOnConflictColumns_PreservesPlainInsert(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripOnConflictColumns{})
	out, err := r.Rewrite(`INSERT INTO t(a,b) VALUES (1,2)`)
	require.NoError(t, err)
	require.Contains(t, strings.ToUpper(out), "INSERT INTO T")
}

// TestStripOnConflictColumns_PreservesOnConflictDoNothingBare pins
// the already-supported case: `ON CONFLICT DO NOTHING` (no column
// list) must survive untouched.
func TestStripOnConflictColumns_PreservesOnConflictDoNothingBare(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripOnConflictColumns{})
	out, err := r.Rewrite(`INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING`)
	require.NoError(t, err)
	require.Contains(t, strings.ToUpper(out), "ON CONFLICT DO NOTHING")
}
