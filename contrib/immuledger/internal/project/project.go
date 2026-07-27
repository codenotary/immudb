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

// Package project derives a stable, human-friendly identifier for the repo the
// user is working in, so one immuledger data directory can serve many projects.
package project

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

// Name returns a stable project identifier: the git repository's top-level
// folder name, or the basename of startDir when it is not inside a repo.
//
// startDir may be empty, in which case PROJECT_DIR, then CLAUDE_PROJECT_DIR,
// then the current working directory are tried in order.
func Name(startDir string) string {
	if strings.Contains(startDir, "${") { // ignore an unexpanded placeholder
		startDir = ""
	}
	dir := firstNonEmpty(
		strings.TrimSpace(startDir),
		cleanEnv("PROJECT_DIR"),
		cleanEnv("CLAUDE_PROJECT_DIR"),
	)
	if dir == "" {
		if wd, err := os.Getwd(); err == nil {
			dir = wd
		}
	}

	if top := gitToplevel(dir); top != "" {
		return Sanitize(filepath.Base(top))
	}
	if dir != "" {
		if abs, err := filepath.Abs(dir); err == nil {
			return Sanitize(filepath.Base(abs))
		}
	}
	return "default"
}

// cleanEnv returns a trimmed env value, treating an unexpanded "${VAR}"
// placeholder (which the plugin runtime passes when the variable is unset) as
// empty so it is never used as a literal path.
func cleanEnv(name string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" || strings.Contains(v, "${") {
		return ""
	}
	return v
}

func gitToplevel(dir string) string {
	if dir == "" {
		return ""
	}
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// maxProjectLen bounds the project segment so a caller-supplied override cannot
// produce unbounded keys.
const maxProjectLen = 128

// disambiguatorLen is the length of the "-xxxxxxxx" suffix appended to any name
// that could not be represented losslessly.
const disambiguatorLen = 9

// Sanitize keeps a project name usable and safe as a key segment: it strips the
// '/' separator used to build ledger keys, removes control characters, trims,
// and length-limits. Applied to both auto-detected names and caller overrides.
//
// Every project's records and its id sequence live under the same
// "immuledger/<project>/..." prefix, so two distinct names must never collapse
// into the same segment — that would merge separate ledgers. Whenever the
// mapping is lossy (a character was replaced, or the name had to be truncated)
// a short digest of the original is appended, so "team/billing" and
// "team_billing" stay distinct and any two long names that share a prefix do
// too. Clean, short names — the overwhelmingly common auto-detected case — are
// returned unchanged.
func Sanitize(name string) string {
	original := strings.TrimSpace(name)
	if original == "" {
		return "default"
	}

	safe := strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r < 0x20 {
			return '_'
		}
		return r
	}, original)

	if safe == original && len(safe) <= maxProjectLen {
		return safe
	}

	// Lossy: truncate on a rune boundary (a byte-slice can cut a multi-byte
	// rune and yield invalid UTF-8) and tag with a digest of the original.
	sum := sha256.Sum256([]byte(original))
	return truncateRunes(safe, maxProjectLen-disambiguatorLen) + "-" + hex.EncodeToString(sum[:])[:8]
}

// truncateRunes shortens s to at most max bytes without splitting a rune.
func truncateRunes(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for len(s) > 0 && !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
