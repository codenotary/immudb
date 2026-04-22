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

// TestStripCreateViewColList covers pg_dump-style CREATE VIEW with
// and without the rename column list.
func TestStripCreateViewColList(t *testing.T) {
	cases := []struct {
		in         string
		wantAbsent []string // substrings that must NOT appear
	}{
		{
			in:         `CREATE VIEW v(a,b) AS SELECT 1, 2`,
			wantAbsent: []string{"(a, b)", "(a,b)"},
		},
		{
			in:         `CREATE VIEW v (a, b, c) AS SELECT 1, 2, 3`,
			wantAbsent: []string{"(a, b, c)"},
		},
		// No column list — must pass through unchanged.
		{
			in: `CREATE VIEW v AS SELECT 1, 2`,
		},
	}

	r := rewrite.New().WithRule(rules.StripCreateViewColList{})
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := r.Rewrite(tc.in)
			require.NoError(t, err)
			require.Contains(t, strings.ToUpper(out), "CREATE VIEW V")
			require.Contains(t, strings.ToUpper(out), "AS SELECT")
			for _, absent := range tc.wantAbsent {
				require.NotContains(t, out, absent,
					"column list must be stripped: in=%q out=%q", tc.in, out)
			}
		})
	}
}
