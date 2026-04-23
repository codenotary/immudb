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

// TestStripPGCasts_RemovesPseudoTypes pins the core contract: any
// `::type` cast whose target is a PG pseudo-type is stripped, while
// honest type conversions survive. These are the exact casts the
// regex at query_machine.go:477 targets.
func TestStripPGCasts_RemovesPseudoTypes(t *testing.T) {
	cases := []struct {
		name   string
		in     string
		absent []string // substrings that must NOT appear after rewrite
	}{
		{
			name:   "regclass_stripped",
			in:     `SELECT c.oid::regclass FROM pg_class c`,
			absent: []string{"::REGCLASS", "::regclass"},
		},
		{
			name:   "text_cast_stripped",
			in:     `SELECT 'bar'::text FROM t`,
			absent: []string{"::TEXT", "::text", "::STRING", "::string"},
		},
		{
			name:   "oid_cast_stripped",
			in:     `SELECT t.attrelid::oid FROM t`,
			absent: []string{"::OID", "::oid"},
		},
		{
			name:   "regtype_stripped",
			in:     `SELECT typ::regtype FROM t`,
			absent: []string{"::REGTYPE", "::regtype"},
		},
		{
			name:   "cast_in_where",
			in:     `SELECT * FROM t WHERE oid::regclass = 'foo'`,
			absent: []string{"::REGCLASS", "::regclass"},
		},
	}

	r := rewrite.New().WithRule(rules.StripPGCasts{})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := r.Rewrite(tc.in)
			require.NoError(t, err)
			for _, bad := range tc.absent {
				require.NotContains(t, out, bad,
					"StripPGCasts(%q) should not contain %q, got %q", tc.in, bad, out)
			}
		})
	}
}

// TestStripPGCasts_PreservesRealCasts guards against over-eager
// stripping: `x::INT8` and `CAST(x AS INTEGER)` are real type
// conversions the user asked for and must survive untouched.
func TestStripPGCasts_PreservesRealCasts(t *testing.T) {
	cases := []string{
		`SELECT x::INT8 FROM t`,
		`SELECT x::FLOAT8 FROM t`,
	}

	r := rewrite.New().WithRule(rules.StripPGCasts{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.True(t,
				strings.Contains(out, "::"),
				"real cast must survive StripPGCasts: in=%q out=%q", sql, out)
		})
	}
}
