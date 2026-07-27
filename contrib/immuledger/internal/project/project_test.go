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

package project

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// Clean names are the common case (a git top-level folder name) and must pass
// through untouched, so existing ledgers keep their keys.
func TestSanitizeLeavesOrdinaryNamesAlone(t *testing.T) {
	for _, name := range []string{"immudb", "my-project", "Team_Alpha", "a.b.c", "проект"} {
		if got := Sanitize(name); got != name {
			t.Errorf("Sanitize(%q) = %q, want it unchanged", name, got)
		}
	}
	if got := Sanitize("   "); got != "default" {
		t.Errorf("Sanitize(blank) = %q, want %q", got, "default")
	}
	if got := Sanitize("  spaced  "); got != "spaced" {
		t.Errorf("Sanitize trims: got %q", got)
	}
}

// Distinct project names must never collapse into one key segment: every
// project's records AND its id sequence live under immuledger/<project>/, so a
// collision merges two separate ledgers.
func TestSanitizeDoesNotMergeDistinctProjects(t *testing.T) {
	cases := [][2]string{
		{"team-alpha/billing", "team-alpha_billing"},
		{"a/b", "a\\b"},
		{"x\ny", "x_y"},
	}
	for _, c := range cases {
		if Sanitize(c[0]) == Sanitize(c[1]) {
			t.Errorf("Sanitize(%q) and Sanitize(%q) both produced %q", c[0], c[1], Sanitize(c[0]))
		}
	}

	// Two names agreeing on their first maxProjectLen bytes must stay distinct.
	long1 := strings.Repeat("z", maxProjectLen) + "-one"
	long2 := strings.Repeat("z", maxProjectLen) + "-two"
	if Sanitize(long1) == Sanitize(long2) {
		t.Errorf("long names sharing a prefix collided: %q", Sanitize(long1))
	}
}

func TestSanitizeIsBoundedAndValidUTF8(t *testing.T) {
	// A multi-byte rune straddling the cap must not be cut in half.
	long := strings.Repeat("é", maxProjectLen)
	got := Sanitize(long)
	if len(got) > maxProjectLen {
		t.Errorf("Sanitize produced %d bytes, over the %d cap", len(got), maxProjectLen)
	}
	if !utf8.ValidString(got) {
		t.Errorf("Sanitize produced invalid UTF-8: %q", got)
	}
	if strings.ContainsAny(got, "/\\") {
		t.Errorf("Sanitize left a key separator in %q", got)
	}
}

func TestSanitizeIsDeterministic(t *testing.T) {
	const name = "team-alpha/billing"
	first := Sanitize(name)
	for range 5 {
		if got := Sanitize(name); got != first {
			t.Fatalf("Sanitize is not stable: %q then %q", first, got)
		}
	}
}
