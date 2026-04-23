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

// TestStripForeignKeys_Rewrites covers the canonical Rails and
// pg_dump shapes.
func TestStripForeignKeys_Rewrites(t *testing.T) {
	cases := []string{
		// Column-level REFERENCES
		`CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id))`,
		// Column-level with ON DELETE
		`CREATE TABLE orders (id INTEGER, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)`,
		// Table-level FK
		`CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))`,
		// Table-level FK with ON DELETE
		`CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)`,
	}

	r := rewrite.New().WithRule(rules.StripForeignKeys{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			upper := strings.ToUpper(out)
			require.NotContains(t, upper, "REFERENCES",
				"REFERENCES must be stripped: %q", out)
			require.NotContains(t, upper, "FOREIGN KEY",
				"FOREIGN KEY must be stripped: %q", out)
			require.NotContains(t, upper, "ON DELETE",
				"ON DELETE action must be stripped: %q", out)
			// The columns themselves survive.
			require.Contains(t, out, "user_id")
		})
	}
}

// TestStripForeignKeys_PreservesOtherDDL guards non-FK CREATE TABLE
// (PK, UNIQUE, regular columns) — nothing should be disturbed.
func TestStripForeignKeys_PreservesOtherDDL(t *testing.T) {
	r := rewrite.New().WithRule(rules.StripForeignKeys{})
	out, err := r.Rewrite(`CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL)`)
	require.NoError(t, err)
	upper := strings.ToUpper(out)
	require.Contains(t, upper, "PRIMARY KEY")
	require.Contains(t, upper, "UNIQUE")
}
