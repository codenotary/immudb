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
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	fm "github.com/codenotary/immudb/pkg/pgsql/server/fmessages"
)

var copyFromStdinRe = regexp.MustCompile(`(?i)^\s*COPY\s+(\S+)\s*\(([^)]+)\)\s+FROM\s+stdin\s*;?\s*$`)

// parseCopyStatement extracts table name and columns from a COPY ... FROM stdin statement.
// Returns table, columns, ok.
func parseCopyStatement(sql string) (string, []string, bool) {
	matches := copyFromStdinRe.FindStringSubmatch(sql)
	if len(matches) != 3 {
		return "", nil, false
	}

	table := strings.TrimSpace(matches[1])
	// Remove schema prefix (e.g. "public.actor" -> "actor")
	if idx := strings.LastIndex(table, "."); idx >= 0 {
		table = table[idx+1:]
	}

	colStr := matches[2]
	parts := strings.Split(colStr, ",")
	cols := make([]string, 0, len(parts))
	for _, p := range parts {
		col := strings.TrimSpace(p)
		// Strip double quotes from column names and handle reserved words
		col = strings.Trim(col, "\"")
		if col != "" {
			col = sanitizeColumnName(col)
			cols = append(cols, col)
		}
	}

	return table, cols, true
}

// handleCopyFromStdin implements the COPY sub-protocol.
// It sends CopyInResponse, reads CopyData messages, converts to INSERTs,
// and executes them.
func (s *session) handleCopyFromStdin(table string, cols []string) error {
	numCols := len(cols)

	s.log.Infof("pgcompat: COPY sending CopyInResponse for %d cols", numCols)

	// Send CopyInResponse to tell client to start sending data
	if _, err := s.writeMessage(bm.CopyInResponse(numCols)); err != nil {
		return err
	}

	s.log.Infof("pgcompat: COPY waiting for CopyData messages")

	// Collect all rows from CopyData messages
	var rows [][]string
	var currentData []byte

	for {
		msg, _, err := s.nextMessage()
		if err != nil {
			s.log.Warningf("COPY %s: error reading message: %v", table, err)
			return err
		}
		s.log.Infof("COPY %s: received message type %T", table, msg)

		switch v := msg.(type) {
		case fm.CopyDataMsg:
			// Accumulate data — may contain multiple lines or partial lines
			currentData = append(currentData, v.Data...)

			// Process complete lines
			for {
				idx := indexOf(currentData, '\n')
				if idx < 0 {
					break
				}

				line := string(currentData[:idx])
				currentData = currentData[idx+1:]

				// Skip empty lines and the COPY terminator "\."
				line = strings.TrimRight(line, "\r")
				if line == "" || line == "\\." {
					continue
				}

				// Parse tab-separated values
				fields := strings.Split(line, "\t")
				if len(fields) > 0 {
					row := make([]string, len(fields))
					for i, f := range fields {
						row[i] = unescapeCopyValue(f)
					}
					rows = append(rows, row)
				}
			}

		case fm.CopyDoneMsg:
			// Process any remaining data
			if len(currentData) > 0 {
				line := strings.TrimRight(string(currentData), "\r\n")
				if line != "" && line != "\\." {
					fields := strings.Split(line, "\t")
					row := make([]string, len(fields))
					for i, f := range fields {
						row[i] = unescapeCopyValue(f)
					}
					rows = append(rows, row)
				}
			}

			// Execute INSERTs in batches
			rowCount, err := s.executeCopyInserts(table, cols, rows)
			if err != nil {
				return err
			}

			// Send CommandComplete with row count
			_, err = s.writeMessage(bm.CommandComplete([]byte(fmt.Sprintf("COPY %d", rowCount))))
			return err

		case fm.CopyFailMsg:
			s.log.Warningf("COPY failed: %s", v.Error)
			return fmt.Errorf("COPY failed: %s", v.Error)

		default:
			return fmt.Errorf("unexpected message during COPY: %T", v)
		}
	}
}

// executeCopyInserts converts COPY rows to INSERT statements and executes them.
func (s *session) executeCopyInserts(table string, cols []string, rows [][]string) (int, error) {
	s.log.Infof("COPY %s: executing %d rows into %d columns", table, len(rows), len(cols))
	if len(rows) == 0 {
		return 0, nil
	}

	colList := strings.Join(cols, ", ")
	batchSize := 100
	totalInserted := 0
	lastKeepAlive := time.Now()

	for i := 0; i < len(rows); i += batchSize {
		// Refresh session activity to prevent session timeout during bulk inserts
		if time.Since(lastKeepAlive) > 10*time.Second {
			s.refreshSessionActivity()
			lastKeepAlive = time.Now()
		}
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		for _, row := range batch {
			// Build VALUES clause
			vals := make([]string, len(row))
			for j, v := range row {
				if v == "NULL" {
					vals[j] = "NULL"
				} else if isTimestampValue(v) {
					// immudb requires CAST for timestamp string literals
					// Strip timezone offset (+00, -05:00) that immudb can't parse
					tsVal := stripTimestampTz(v)
					vals[j] = "CAST('" + strings.ReplaceAll(tsVal, "'", "''") + "' AS TIMESTAMP)"
				} else if isBoolValue(v) {
					vals[j] = normalizeBool(v)
				} else {
					vals[j] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
				}
			}

			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				table, colList, strings.Join(vals, ", "))

			stmts, err := removePGCatalogReferencesAndParse(insertSQL)
			if err != nil {
				s.log.Warningf("COPY INSERT parse error (skipping row): %v — SQL: %.200s", err, insertSQL)
				continue
			}

			for _, stmt := range stmts {
				if err := s.exec(stmt, nil, nil, false); err != nil {
					s.log.Warningf("COPY INSERT exec error (skipping row): %v", err)
					continue
				}
			}

			totalInserted++
		}
	}

	if totalInserted < len(rows) && len(rows) > 0 {
		s.log.Warningf("COPY %s: only %d/%d rows inserted (some failed)", table, totalInserted, len(rows))
	} else {
		s.log.Infof("COPY %s: inserted %d/%d rows", table, totalInserted, len(rows))
	}
	return totalInserted, nil
}

// removePGCatalogReferencesAndParse is a helper that strips pg_catalog references
// and parses the SQL.
func removePGCatalogReferencesAndParse(sqlStr string) ([]sql.SQLStmt, error) {
	cleaned := removePGCatalogReferences(sqlStr)
	return sql.ParseSQL(strings.NewReader(cleaned))
}

// unescapeCopyValue converts COPY text format escapes.
// \N = NULL, \\ = backslash, \t = tab, \n = newline
func unescapeCopyValue(s string) string {
	if s == "\\N" {
		return "NULL"
	}

	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case '\\':
				b.WriteByte('\\')
				i++
			case 'n':
				b.WriteByte('\n')
				i++
			case 't':
				b.WriteByte('\t')
				i++
			case 'r':
				b.WriteByte('\r')
				i++
			default:
				b.WriteByte(s[i])
			}
		} else {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

var timestampRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}`)
var timestampTzStripRe = regexp.MustCompile(`[+-]\d{2}(:\d{2})?$`)

func isTimestampValue(v string) bool {
	return timestampRe.MatchString(v)
}

// stripTimestampTz removes timezone offset (+00, +00:00, -05:00) from timestamp strings
func stripTimestampTz(v string) string {
	return timestampTzStripRe.ReplaceAllString(v, "")
}

// SQL reserved words that need _ prefix when used as column names
var sqlReservedWords = map[string]bool{
	"group": true, "order": true, "key": true, "index": true, "table": true,
	"column": true, "type": true, "year": true, "date": true, "time": true,
	"check": true, "default": true, "desc": true, "asc": true, "select": true,
	"from": true, "where": true, "set": true, "grant": true, "user": true,
	"role": true, "limit": true, "offset": true, "values": true, "primary": true,
	"foreign": true, "create": true, "drop": true, "alter": true, "insert": true,
	"update": true, "delete": true, "begin": true, "commit": true, "rollback": true,
	"having": true, "between": true, "like": true, "in": true, "is": true,
	"not": true, "null": true, "and": true, "or": true, "cast": true,
	"case": true, "when": true, "then": true, "else": true, "end": true,
	"join": true, "on": true, "as": true, "distinct": true, "all": true,
	"any": true, "exists": true, "union": true, "except": true, "intersect": true,
	"natural": true, "cross": true, "full": true, "outer": true, "inner": true,
	"left": true, "right": true, "using": true, "returning": true, "with": true,
	"recursive": true, "password": true, "database": true, "transaction": true,
}

func sanitizeColumnName(col string) string {
	if sqlReservedWords[strings.ToLower(col)] {
		return "_" + col
	}
	return col
}

func isBoolValue(v string) bool {
	return v == "t" || v == "f" || v == "true" || v == "false" || v == "TRUE" || v == "FALSE"
}

func normalizeBool(v string) string {
	if v == "t" {
		return "true"
	}
	if v == "f" {
		return "false"
	}
	return v
}

func indexOf(data []byte, b byte) int {
	for i, c := range data {
		if c == b {
			return i
		}
	}
	return -1
}
