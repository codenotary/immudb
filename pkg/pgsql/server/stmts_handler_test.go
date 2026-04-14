/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package server

import (
	"testing"
)

// TestSetBlacklistOnlyMatchesTopLevelSet guards the 14f64047 fix: the
// `set` blacklist regex previously matched any statement containing
// `SET\s+...`, which silently swallowed every UPDATE … SET … WHERE …
// — UPDATE was fundamentally broken for all clients.
func TestSetBlacklistOnlyMatchesTopLevelSet(t *testing.T) {
	mustNotMatch := []string{
		`UPDATE users SET first_name = 'X' WHERE id = '1'`,
		`UPDATE t SET a=1, b=2 WHERE id IN (SELECT id FROM s)`,
		`update users set first_name='X' where id='1'`,
		`  UPDATE foo SET v = 1`,
		// INSERT/DELETE/SELECT shouldn't match either even though they
		// contain SET in column names or values.
		`INSERT INTO settings (key, val) VALUES ('a', 'b')`,
		`SELECT key FROM settings WHERE key = 'mindset'`,
		`DELETE FROM settings WHERE val = 'reset'`,
	}
	for _, sql := range mustNotMatch {
		if set.MatchString(sql) {
			t.Errorf("set regex unexpectedly matched: %q", sql)
		}
	}

	mustMatch := []string{
		`SET timezone = 'UTC'`,
		`set client_encoding = 'UTF8'`,
		`SET search_path TO public`,
		`  SET datestyle = ISO`,
	}
	for _, sql := range mustMatch {
		if !set.MatchString(sql) {
			t.Errorf("set regex failed to match top-level SET: %q", sql)
		}
	}
}

// TestBlacklistDoesNotEatUpdate is a higher-level guard tying the regex
// fix to its consumer: isInBlackList must return false for UPDATE.
func TestBlacklistDoesNotEatUpdate(t *testing.T) {
	s := &session{}
	if s.isInBlackList(`UPDATE users SET first_name = 'X' WHERE id = '1'`) {
		t.Fatal("UPDATE was blacklisted — silent UPDATE-eating bug returned")
	}
	if !s.isInBlackList(`SET timezone = 'UTC'`) {
		t.Fatal("top-level SET should still be blacklisted")
	}
}
