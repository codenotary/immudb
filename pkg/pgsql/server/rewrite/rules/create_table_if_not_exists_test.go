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

// TestEnsureCreateTableIfNotExists pins the Rails db:prepare
// idempotency: every CREATE TABLE gains an IF NOT EXISTS clause
// so a second run doesn't blow up on "table already exists".
func TestEnsureCreateTableIfNotExists(t *testing.T) {
	cases := []string{
		`CREATE TABLE users (id INTEGER)`,
		`CREATE TABLE "User" (id INTEGER)`,
		// Already has IF NOT EXISTS — must remain idempotent.
		`CREATE TABLE IF NOT EXISTS users (id INTEGER)`,
	}

	r := rewrite.New().WithRule(rules.EnsureCreateTableIfNotExists{})
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			out, err := r.Rewrite(sql)
			require.NoError(t, err)
			require.Contains(t, strings.ToUpper(out), "IF NOT EXISTS",
				"IF NOT EXISTS must be present after rewrite: in=%q out=%q", sql, out)
		})
	}
}

// TestEnsureCreateTableIfNotExists_PreservesNonCreate guards against
// touching non-CREATE-TABLE statements.
func TestEnsureCreateTableIfNotExists_PreservesNonCreate(t *testing.T) {
	r := rewrite.New().WithRule(rules.EnsureCreateTableIfNotExists{})
	for _, sql := range []string{
		`SELECT * FROM users`,
		`INSERT INTO users (id) VALUES (1)`,
		`CREATE INDEX idx ON users(id)`,
	} {
		out, err := r.Rewrite(sql)
		require.NoError(t, err, "sql=%q", sql)
		require.NotContains(t, strings.ToUpper(out), "IF NOT EXISTS",
			"rule should only affect CREATE TABLE: %q", out)
	}
}
