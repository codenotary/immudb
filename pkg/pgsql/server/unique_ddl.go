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
	"regexp"
	"strings"
)

// PostgreSQL accepts `UNIQUE` as both an inline column constraint
// (`email VARCHAR(255) UNIQUE`) and a table-level constraint
// (`UNIQUE (a, b)`). immudb's grammar has neither — uniqueness is
// expressed only via `CREATE UNIQUE INDEX ON tbl(cols)`
// (sql_grammar.y:400-408). Historically the pgwire layer "solved"
// this by regex-stripping the `UNIQUE` keyword, which silently
// dropped the constraint. That is wrong: constraints are a
// semantic promise, not decoration.
//
// extractUniqueConstraints rewrites each inline/table-level UNIQUE
// into a trailing `CREATE UNIQUE INDEX IF NOT EXISTS ON tbl(cols)`
// statement, preserving the uniqueness guarantee against the engine.
// Called from removePGCatalogReferences BEFORE the pgTypeReplacements
// regex pipeline so the downstream regexes see a clean column list.
//
// Input assumptions:
//   - String literals have already been masked by maskStringLiterals,
//     so every ' in the string delimits a placeholder token, not
//     user content.
//   - The input may contain multiple statements separated by `;`.

// createTableHeader matches `CREATE TABLE [IF NOT EXISTS] <name>` up
// to (but not including) the opening `(` of the column list.
// `<name>` can be a bare identifier, a double-quoted identifier, or
// a schema-qualified form `a.b` (the pg_catalog / public strip pass
// runs before this, so the qualifier is usually already gone, but we
// stay permissive).
var createTableHeaderRe = regexp.MustCompile(`(?is)\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?("[^"]+"|[A-Za-z_][\w.]*)\s*\(`)

// unquoteIdent strips the surrounding double quotes from a PG-style
// quoted identifier, leaving bare identifiers untouched. CREATE INDEX
// cannot reference a quoted name (immudb's grammar), so we emit the
// bare form.
func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// splitTopLevelCommas splits s by commas that are not nested inside
// parentheses. Needed to distinguish `UNIQUE (a, b)` (single element,
// comma inside parens) from two column definitions.
func splitTopLevelCommas(s string) []string {
	var parts []string
	depth := 0
	last := 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[last:i])
				last = i + 1
			}
		}
	}
	parts = append(parts, s[last:])
	return parts
}

// stripUniqueFromColumn removes a standalone `UNIQUE` keyword from a
// column definition like `email VARCHAR(255) UNIQUE NOT NULL`,
// returning the rewritten definition and whether anything was
// stripped. Case-insensitive, word-bounded, does not strip inside
// parens (covers `DEFAULT unique_fn()` or similar).
var uniqueTokenRe = regexp.MustCompile(`(?i)\s+UNIQUE\b`)

func stripUniqueFromColumn(col string) (string, bool) {
	// Only strip at depth 0 (outside any inner parens).
	var out strings.Builder
	depth := 0
	matches := uniqueTokenRe.FindAllStringIndex(col, -1)
	if len(matches) == 0 {
		return col, false
	}
	// Walk the string; track paren depth; skip matches that fall inside
	// inner parens.
	stripped := false
	i := 0
	for i < len(col) {
		// Check if any match starts at i AND depth == 0.
		var hit *[]int
		for mi := range matches {
			if matches[mi][0] == i {
				hit = &matches[mi]
				break
			}
		}
		if hit != nil && depth == 0 {
			i = (*hit)[1]
			stripped = true
			continue
		}
		switch col[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		out.WriteByte(col[i])
		i++
	}
	return out.String(), stripped
}

// firstIdentifier returns the first identifier-looking token in s
// (after skipping leading whitespace). Handles double-quoted and bare
// forms. Used to pull the column name off a column definition.
func firstIdentifier(s string) string {
	s = strings.TrimLeft(s, " \t\n\r")
	if s == "" {
		return ""
	}
	if s[0] == '"' {
		end := strings.IndexByte(s[1:], '"')
		if end < 0 {
			return s
		}
		return s[:end+2]
	}
	end := 0
	for end < len(s) {
		c := s[end]
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			end++
			continue
		}
		break
	}
	return s[:end]
}

// tableLevelUniqueRe matches a table-level `UNIQUE (col1, col2, …)`
// constraint element. Used as a quick check; the parenthesised column
// list is then extracted from the tail of the element.
var tableLevelUniqueRe = regexp.MustCompile(`(?is)^\s*UNIQUE\s*\(([^)]*)\)\s*$`)

// extractUniqueConstraints walks every CREATE TABLE statement in sqlStr,
// rewriting inline and table-level UNIQUE constraints into trailing
// `CREATE UNIQUE INDEX IF NOT EXISTS ON tbl(cols)` statements. Returns
// the rewritten SQL. If no CREATE TABLE contains a UNIQUE, returns
// sqlStr unchanged (pointer-equal for cheap downstream checks).
func extractUniqueConstraints(sqlStr string) string {
	headers := createTableHeaderRe.FindAllStringSubmatchIndex(sqlStr, -1)
	if len(headers) == 0 {
		return sqlStr
	}

	// Walk headers back-to-front so index positions in the suffix stay
	// valid as we splice the prefix.
	type insertion struct {
		pos  int    // where to splice (after the matching ')')
		stmt string // "; CREATE UNIQUE INDEX IF NOT EXISTS ON tbl(cols)"
	}
	var insertions []insertion

	// Also accumulate replacements (newColSection) keyed by position.
	type replacement struct {
		from, to int
		with     string
	}
	var replacements []replacement

	for _, h := range headers {
		// h = [match_start, match_end, name_start, name_end]
		openParen := h[1] - 1 // '(' is the last char matched
		if openParen < 0 || openParen >= len(sqlStr) || sqlStr[openParen] != '(' {
			continue
		}
		name := unquoteIdent(sqlStr[h[2]:h[3]])

		// Find matching close paren.
		depth := 1
		i := openParen + 1
		for i < len(sqlStr) && depth > 0 {
			switch sqlStr[i] {
			case '(':
				depth++
			case ')':
				depth--
			}
			i++
		}
		if depth != 0 {
			// Unbalanced — leave this CREATE TABLE alone.
			continue
		}
		closeParen := i - 1
		body := sqlStr[openParen+1 : closeParen]

		parts := splitTopLevelCommas(body)

		var keptParts []string
		var newIndexes []string // parenthesised col lists, e.g. "email" or "a, b"

		for _, p := range parts {
			trimmed := strings.TrimSpace(p)

			// Table-level UNIQUE (…)
			if m := tableLevelUniqueRe.FindStringSubmatch(trimmed); m != nil {
				newIndexes = append(newIndexes, strings.TrimSpace(m[1]))
				continue
			}

			// Inline UNIQUE on a column definition.
			stripped, didStrip := stripUniqueFromColumn(p)
			if didStrip {
				col := unquoteIdent(firstIdentifier(stripped))
				if col != "" {
					newIndexes = append(newIndexes, col)
				}
				keptParts = append(keptParts, stripped)
				continue
			}

			keptParts = append(keptParts, p)
		}

		if len(newIndexes) == 0 {
			continue
		}

		// Rebuild body. Use a leading newline before each element only
		// if the original column list looks multi-line; otherwise keep
		// a simple comma-space join. We don't try to preserve exact
		// original formatting — the downstream parser doesn't care.
		newBody := strings.Join(keptParts, ",")

		replacements = append(replacements, replacement{
			from: openParen + 1,
			to:   closeParen,
			with: newBody,
		})

		for _, cols := range newIndexes {
			insertions = append(insertions, insertion{
				pos:  closeParen + 1,
				stmt: "; CREATE UNIQUE INDEX IF NOT EXISTS ON " + name + "(" + cols + ")",
			})
		}
	}

	if len(replacements) == 0 && len(insertions) == 0 {
		return sqlStr
	}

	// Apply replacements first (back-to-front), then insertions
	// (back-to-front). Insertions go at closeParen+1 of the ORIGINAL
	// string; since replacements preserve string length only
	// coincidentally, do insertions against the string as it stands
	// after each step, walking right-to-left.
	//
	// Simpler: build a list of (position, op) events in descending
	// order of position and apply in that order.
	type edit struct {
		from, to int
		with     string
	}
	var edits []edit
	for _, r := range replacements {
		edits = append(edits, edit{r.from, r.to, r.with})
	}
	for _, ins := range insertions {
		edits = append(edits, edit{ins.pos, ins.pos, ins.stmt})
	}
	// Sort descending by `from`.
	for i := 1; i < len(edits); i++ {
		for j := i; j > 0 && edits[j-1].from < edits[j].from; j-- {
			edits[j-1], edits[j] = edits[j], edits[j-1]
		}
	}

	out := sqlStr
	for _, e := range edits {
		out = out[:e.from] + e.with + out[e.to:]
	}
	return out
}
