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

// TestStripTransactionModes covers the PG-mode suffixes that
// immudb's BEGIN grammar rejects.
func TestStripTransactionModes(t *testing.T) {
	cases := []string{
		`BEGIN READ WRITE`,
		`BEGIN READ ONLY`,
		`BEGIN ISOLATION LEVEL SERIALIZABLE`,
		`BEGIN ISOLATION LEVEL READ COMMITTED`,
		`START TRANSACTION READ WRITE`,
		`START TRANSACTION READ ONLY`,
		// DEFERRABLE / NOT DEFERRABLE: auxten rejects these forms
		// even though PG accepts them. The regex chain handles the
		// fallback for now. When auxten gains DEFERRABLE support (or
		// we swap to pg_query_go in B3), add them here.
	}

	r := rewrite.New().WithRule(rules.StripTransactionModes{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			upper := strings.ToUpper(out)
			for _, mode := range []string{
				"READ WRITE", "READ ONLY",
				"ISOLATION LEVEL", "DEFERRABLE",
			} {
				require.NotContains(t, upper, mode,
					"transaction mode %q must be stripped: in=%q out=%q",
					mode, sql, out)
			}
		})
	}
}

// TestStripTransactionModes_PreservesBareBegin guards against
// touching plain BEGIN / COMMIT / ROLLBACK.
func TestStripTransactionModes_PreservesBareBegin(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripTransactionModes{})
	for _, sql := range []string{`BEGIN`, `COMMIT`, `ROLLBACK`} {
		out, err := r.Rewrite(sql)
		require.NoError(t, err, "sql=%q", sql)
		require.NotEmpty(t, out, "in=%q", sql)
	}
}
