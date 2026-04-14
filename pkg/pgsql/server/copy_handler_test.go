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

package server

import (
	"strings"
	"testing"
)

func TestStripTimestampTz(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		// Bare DATE values must be untouched. Earlier the regex matched the
		// trailing `-DD` of the day component as if it were a `-NN` TZ offset,
		// turning `2021-09-25` into `2021-09` and breaking COPY of any DATE
		// column (regression in commit 434bf1a5).
		{"2021-09-25", "2021-09-25"},
		{"1999-12-31", "1999-12-31"},
		{"2006-02-09", "2006-02-09"},

		// timestamptz with offsets gets the offset stripped.
		{"2021-09-25 10:30:00-05", "2021-09-25 10:30:00"},
		{"2021-09-25 10:30:00+00", "2021-09-25 10:30:00"},
		{"2021-09-25 10:30:00+05:30", "2021-09-25 10:30:00"},
		{"2021-09-25 10:30:00.123456-05:00", "2021-09-25 10:30:00.123456"},
		{"2021-09-25T10:30+00:00", "2021-09-25T10:30"},

		// Plain timestamps without an offset are returned unchanged.
		{"2021-09-25 10:30:00", "2021-09-25 10:30:00"},
		{"2021-09-25 10:30:00.123", "2021-09-25 10:30:00.123"},

		// Non-timestamp strings are untouched.
		{"", ""},
		{"hello", "hello"},
	}
	for _, c := range cases {
		got := stripTimestampTz(c.in)
		if got != c.want {
			t.Errorf("stripTimestampTz(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

// TestSanitizeColumnNameAcceptsCommonIdentifiers pins the trimmed
// sqlReservedWords map. Words that are *not* reserved by immudb's SQL
// parser (verified live against `CREATE TABLE t(id INT NOT NULL, <word>
// VARCHAR(8), PRIMARY KEY(id))`) must pass through unchanged so that
// dumps using common columns like `type`, `date`, `year` load without
// being silently renamed to `_type` / `_date` / `_year` etc. The earlier
// over-eager list broke the netflix sample dump.
func TestSanitizeColumnNameAcceptsCommonIdentifiers(t *testing.T) {
	pass := []string{
		"id", "type", "year", "date", "time", "key", "index",
		"role", "offset", "values", "between", "begin", "commit", "rollback",
		"group", "order", "set", "TYPE", "Year", // case-insensitive check
	}
	for _, name := range pass {
		got := sanitizeColumnName(name)
		if got != name {
			t.Errorf("sanitizeColumnName(%q) = %q; want %q (must NOT be prefixed)", name, got, name)
		}
	}

	// Tokens that immudb's grammar still reserves must continue to be
	// prefixed so the rewritten INSERT stays parseable.
	prefixed := []string{
		"check", "default", "limit", "primary", "having", "like", "in",
		"is", "not", "null", "and", "or", "cast", "user", "select", "from",
		"where", "join",
	}
	for _, name := range prefixed {
		got := sanitizeColumnName(name)
		if !strings.HasPrefix(got, "_") {
			t.Errorf("sanitizeColumnName(%q) = %q; want a `_`-prefixed rewrite", name, got)
		}
	}
}
