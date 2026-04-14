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
	"bytes"
	"encoding/binary"
	"regexp"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// Known column values for pg_catalog canned responses.
// When pgAdmin asks for these columns, we return sensible defaults.
var knownColumnValues = map[string]sql.TypedValue{
	// pg_database
	"did":            sql.NewInteger(16384),
	"oid":            sql.NewInteger(16384),
	"datname":        nil, // filled dynamically with current db name
	"datallowconn":   sql.NewBool(true),
	"datistemplate":  sql.NewBool(false),
	"is_template":    sql.NewBool(false),
	"datlastsysoid":  sql.NewInteger(12000),
	"datconnlimit":   sql.NewInteger(-1),
	"encoding":       sql.NewInteger(6), // UTF8
	"serverencoding": sql.NewVarchar("UTF8"),
	"datacl":         sql.NewNull(sql.VarcharType),
	"owner":          sql.NewInteger(10),
	"datdba":         sql.NewInteger(10),

	// pg_roles / user info
	"id":                 sql.NewInteger(10),
	"rolname":            sql.NewVarchar("immudb"),
	"name":               nil, // filled dynamically based on query context
	"rolsuper":           sql.NewBool(true),
	"is_superuser":       sql.NewBool(true),
	"rolinherit":         sql.NewBool(true),
	"rolcreaterole":      sql.NewBool(true),
	"can_create_role":    sql.NewBool(true),
	"rolcreatedb":        sql.NewBool(true),
	"can_create_db":      sql.NewBool(true),
	"rolcanlogin":        sql.NewBool(true),
	"rolreplication":     sql.NewBool(false),
	"rolbypassrls":       sql.NewBool(false),
	"can_signal_backend": sql.NewBool(true),
	"rolvaliduntil":      sql.NewNull(sql.VarcharType),
	"rolconfig":          sql.NewNull(sql.VarcharType),

	// pg_settings
	"setting":    sql.NewVarchar(""),
	"unit":       sql.NewNull(sql.VarcharType),
	"category":   sql.NewVarchar(""),
	"short_desc": sql.NewVarchar(""),
	"vartype":    sql.NewVarchar("string"),
	"context":    sql.NewVarchar("internal"),
	"source":     sql.NewVarchar("default"),

	// pg_tablespace
	"spcname": sql.NewVarchar("pg_default"),
	"spcacl":  sql.NewNull(sql.VarcharType),

	// general boolean columns pgAdmin probes
	"cancreate":        sql.NewBool(true),
	"connected":        sql.NewBool(true),
	"gss_authenticated": sql.NewBool(false),
	"encrypted":        sql.NewBool(false),

	// replication type
	"type": sql.NewNull(sql.VarcharType),

	// general
	"description": sql.NewNull(sql.VarcharType),
	"comment":     sql.NewNull(sql.VarcharType),
}

// Regex to extract "AS alias" patterns from anywhere in the query
var asAliasRe = regexp.MustCompile(`(?i)\bAS\s+(\w+)`)

// handlePgSystemQuery returns canned responses for pg_catalog queries.
// It extracts column aliases from the SQL and returns a single row with
// known default values for each column.
func (s *session) handlePgSystemQuery(query string) error {
	cols := extractColumnNames(query)
	if len(cols) == 0 {
		// Can't determine columns — return empty result with single "result" column
		cols = []string{"result"}
	}

	// Debug logging
	s.log.Infof("pgcompat: intercepted query: %q", query)
	s.log.Infof("pgcompat: extracted columns: %v", cols)
	s.log.Infof("pgcompat: query length: %d bytes", len(query))

	// pg_type and pg_range lookups (Rails load_additional_types, etc.) must
	// return zero rows rather than a single row of bogus defaults. Returning
	// a fake row causes Rails to register a broken type handler (e.g. jsonb
	// with OID 16384 and nil typname), which then blows up with
	// "can't quote Hash" the first time a jsonb default like `{}` is
	// serialised. Empty result = Rails falls back to its built-in OID map
	// which knows real PG OIDs (jsonb=3802, numeric=1700, …).
	lq := strings.ToLower(query)
	if strings.Contains(lq, "pg_type") || strings.Contains(lq, "pg_range") {
		colDescs := make([]sql.ColDescriptor, len(cols))
		for i, name := range cols {
			colDescs[i] = sql.ColDescriptor{Column: name, Type: sql.VarcharType}
		}
		if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
			return err
		}
		s.log.Infof("pgcompat: pg_type/pg_range query — returning 0 rows so Rails uses its built-in OID map")
		return nil
	}
	for i, name := range cols {
		if val, ok := knownColumnValues[name]; ok && val != nil {
			s.log.Infof("pgcompat:   col[%d] %s -> type=%s val=%v", i, name, val.Type(), val.RawValue())
		} else {
			s.log.Infof("pgcompat:   col[%d] %s -> dynamic/null", i, name)
		}
	}

	// Build column descriptors
	colDescs := make([]sql.ColDescriptor, len(cols))
	for i, name := range cols {
		colType := sql.VarcharType
		if val, ok := knownColumnValues[name]; ok && val != nil {
			colType = val.Type()
		}
		colDescs[i] = sql.ColDescriptor{Column: name, Type: colType}
	}

	// Write row description with plain column names
	if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
		return err
	}

	// Build row values
	dbName := "defaultdb"
	if s.db != nil {
		dbName = s.db.GetName()
	}

	values := make([]sql.TypedValue, len(cols))
	for i, name := range cols {
		if val, ok := knownColumnValues[name]; ok {
			if val == nil {
				// dynamic value
				switch name {
				case "datname":
					values[i] = sql.NewVarchar(dbName)
				case "name":
					if strings.Contains(strings.ToLower(query), "pg_database") {
						values[i] = sql.NewVarchar(dbName)
					} else {
						values[i] = sql.NewVarchar("immudb")
					}
				default:
					values[i] = sql.NewNull(sql.VarcharType)
				}
			} else {
				values[i] = val
			}
		} else {
			// Unknown column — return NULL
			values[i] = sql.NewNull(sql.VarcharType)
		}
	}

	row := &sql.Row{ValuesByPosition: values}
	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// extractColumnNames extracts column names from a SQL query.
// Strategy: find all "AS alias" patterns, plus bare column names from
// simple "SELECT col1, col2 FROM" patterns.
func extractColumnNames(query string) []string {
	// Strategy: parse the SELECT list between SELECT and FROM (at depth 0),
	// split by commas at depth 0, then for each part extract AS alias or
	// the bare trailing column name.

	upper := strings.ToUpper(query)
	selIdx := strings.Index(upper, "SELECT")
	if selIdx < 0 {
		return nil
	}

	rest := query[selIdx+6:]

	// Find FROM at depth 0
	depth := 0
	fromIdx := -1
	for i := 0; i < len(rest)-4; i++ {
		switch rest[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && i+5 <= len(rest) && strings.EqualFold(rest[i:i+4], "FROM") {
				before := byte(' ')
				if i > 0 {
					before = rest[i-1]
				}
				after := byte(' ')
				if i+4 < len(rest) {
					after = rest[i+4]
				}
				if (before == ' ' || before == '\n' || before == '\t') &&
					(after == ' ' || after == '\n' || after == '\t') {
					fromIdx = i
				}
			}
		}
		if fromIdx >= 0 {
			break
		}
	}

	var selectList string
	if fromIdx < 0 {
		// No FROM clause (e.g. "SELECT CASE ... END as type;")
		// Use everything after SELECT, stripping trailing semicolons
		selectList = strings.TrimSpace(strings.TrimRight(rest, "; \n\t"))
	} else {
		selectList = strings.TrimSpace(rest[:fromIdx])
	}
	if selectList == "*" {
		return nil
	}

	parts := splitAtDepthZero(selectList, ',')

	var cols []string
	seen := make(map[string]bool)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var colName string

		// Check for "... AS alias"
		asMatch := regexp.MustCompile(`(?i)\bAS\s+(\w+)\s*$`).FindStringSubmatch(part)
		if len(asMatch) == 2 {
			colName = strings.ToLower(asMatch[1])
		} else {
			// No AS — get the last token as bare column name
			// Handle "table.column" or just "column"
			words := strings.Fields(part)
			if len(words) > 0 {
				last := words[len(words)-1]
				// Strip table prefix
				if idx := strings.LastIndex(last, "."); idx >= 0 {
					last = last[idx+1:]
				}
				last = strings.Trim(last, "\"'`()")
				colName = strings.ToLower(last)
			}
		}

		if colName != "" && !isReserved(colName) && !seen[colName] {
			cols = append(cols, colName)
			seen[colName] = true
		}
	}

	return cols
}

func splitAtDepthZero(s string, sep rune) []string {
	var parts []string
	depth := 0
	start := 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		default:
			if depth == 0 && c == sep {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func isReserved(s string) bool {
	switch s {
	case "select", "from", "where", "and", "or", "not", "in", "is",
		"null", "true", "false", "case", "when", "then", "else", "end",
		"as", "on", "join", "left", "right", "inner", "outer", "cross",
		"order", "by", "group", "having", "limit", "offset", "union",
		"all", "distinct", "exists", "between", "like", "ilike":
		return true
	}
	return false
}

// handlePgSystemQueryDataOnly is like handlePgSystemQuery but skips
// RowDescription (for extended query protocol where Describe already sent it).
func (s *session) handlePgSystemQueryDataOnly(query string) error {
	cols := extractColumnNames(query)
	if len(cols) == 0 {
		return nil
	}

	// Same zero-row short-circuit as handlePgSystemQuery — see that comment
	// for why pg_type / pg_range must not emit a bogus canned row.
	lq := strings.ToLower(query)
	if strings.Contains(lq, "pg_type") || strings.Contains(lq, "pg_range") {
		s.log.Infof("pgcompat: pg_type/pg_range query (ext mode) — returning 0 rows")
		return nil
	}

	dbName := "defaultdb"
	if s.db != nil {
		dbName = s.db.GetName()
	}

	values := make([]sql.TypedValue, len(cols))
	for i, name := range cols {
		if val, ok := knownColumnValues[name]; ok {
			if val == nil {
				switch name {
				case "datname":
					values[i] = sql.NewVarchar(dbName)
				case "name":
					if strings.Contains(strings.ToLower(query), "pg_database") {
						values[i] = sql.NewVarchar(dbName)
					} else {
						values[i] = sql.NewVarchar("immudb")
					}
				default:
					values[i] = sql.NewNull(sql.VarcharType)
				}
			} else {
				values[i] = val
			}
		} else {
			values[i] = sql.NewNull(sql.VarcharType)
		}
	}

	row := &sql.Row{ValuesByPosition: values}

	// Log the actual values being sent
	for i, v := range values {
		if v != nil {
			s.log.Infof("pgcompat: DataRow col[%d]=%s rawValue=%v type=%s", i, cols[i], v.RawValue(), v.Type())
		}
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// extractResultCols returns ColDescriptor slice for emulable queries,
// used by the Parse handler to populate statement.Results for Describe.
func extractResultCols(query string) []sql.ColDescriptor {
	names := extractColumnNames(query)
	if len(names) == 0 {
		return nil
	}

	cols := make([]sql.ColDescriptor, len(names))
	for i, name := range names {
		colType := sql.VarcharType
		if val, ok := knownColumnValues[name]; ok && val != nil {
			colType = val.Type()
		}
		cols[i] = sql.ColDescriptor{Column: name, Type: colType}
	}
	return cols
}

// buildMultiColRowDescription creates a RowDescription message with plain
// column names from ColDescriptor.Column, bypassing the Selector() encoding.
func buildMultiColRowDescription(cols []sql.ColDescriptor) []byte {
	messageType := []byte(`T`)
	fieldNumb := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldNumb, uint16(len(cols)))

	fieldData := make([]byte, 0)
	for n, col := range cols {
		fieldName := append([]byte(col.Column), 0)
		tableOID := make([]byte, 4)
		attrNum := make([]byte, 2)
		binary.BigEndian.PutUint16(attrNum, uint16(n+1))
		oid := make([]byte, 4)
		binary.BigEndian.PutUint32(oid, uint32(pgmeta.PgTypeMap[col.Type][pgmeta.PgTypeMapOid]))
		typeLen := make([]byte, 2)
		binary.BigEndian.PutUint16(typeLen, uint16(pgmeta.PgTypeMap[col.Type][pgmeta.PgTypeMapLength]))
		typeMod := make([]byte, 4)
		binary.BigEndian.PutUint32(typeMod, 0xFFFFFFFF)
		formatCode := make([]byte, 2)

		fieldData = append(fieldData, bytes.Join([][]byte{fieldName, tableOID, attrNum, oid, typeLen, typeMod, formatCode}, nil)...)
	}

	msgLen := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLen, uint32(4+2+len(fieldData)))

	return bytes.Join([][]byte{messageType, msgLen, fieldNumb, fieldData}, nil)
}
