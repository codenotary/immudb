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

// TestStripCreateIndexName covers the pg_dump shapes that need the
// name stripped so immudb's grammar accepts them.
func TestStripCreateIndexName(t *testing.T) {
	cases := []struct {
		in            string
		wantContain   []string
		wantAbsent    []string
	}{
		{
			in:          `CREATE INDEX idx_foo ON t (x)`,
			wantContain: []string{"CREATE INDEX", "ON t", "(x)"},
			wantAbsent:  []string{"idx_foo"},
		},
		{
			in:          `CREATE UNIQUE INDEX idx_foo ON t (x)`,
			wantContain: []string{"CREATE UNIQUE INDEX", "ON t", "(x)"},
			wantAbsent:  []string{"idx_foo"},
		},
		{
			in:          `CREATE INDEX IF NOT EXISTS "My Index" ON t (x)`,
			wantContain: []string{"IF NOT EXISTS", "ON t"},
			wantAbsent:  []string{"My Index", `"My Index"`},
		},
		// No name — must pass through untouched.
		{
			in:          `CREATE INDEX ON t (x)`,
			wantContain: []string{"CREATE INDEX", "ON t", "(x)"},
		},
	}

	r := rewrite.New().WithRule(rules.StripCreateIndexName{})
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			out, err := r.Rewrite(tc.in)
			require.NoError(t, err)
			upper := strings.ToUpper(out)
			for _, want := range tc.wantContain {
				require.Contains(t, upper, strings.ToUpper(want),
					"expected %q in output: %q", want, out)
			}
			for _, absent := range tc.wantAbsent {
				require.NotContains(t, out, absent,
					"did not expect %q in output: %q", absent, out)
			}
		})
	}
}
