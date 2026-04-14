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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
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

	// pg_type lookups: emit a real row per standard PG type so Rails's
	// load_additional_types populates its OID-to-Ruby-Type map. Without
	// this, every column value comes back with a "unknown OID NNNN"
	// warning and the cast type becomes Type::Value (the default
	// fallback), which then breaks `enum :col, ...` at model-load time.
	// pg_range stays as a 0-row response.
	lq := strings.ToLower(query)
	if strings.Contains(lq, "pg_type") {
		return s.handlePgTypeRows(query, cols)
	}
	if strings.Contains(lq, "pg_range") {
		colDescs := make([]sql.ColDescriptor, len(cols))
		for i, name := range cols {
			colDescs[i] = sql.ColDescriptor{Column: name, Type: sql.VarcharType}
		}
		if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
			return err
		}
		s.log.Infof("pgcompat: pg_range query — returning 0 rows")
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

// stdPgTypes is the minimal set of Postgres types that Rails's
// load_additional_types pass needs to populate its OID type map.
// Without these rows Rails treats every value as Type::Value (default),
// which breaks model-side `enum :col, ...` declarations.
var stdPgTypes = []struct {
	oid     int64
	typname string
	typtype string
}{
	{16, "bool", "b"},
	{17, "bytea", "b"},
	{18, "char", "b"},
	{19, "name", "b"},
	{20, "int8", "b"},
	{21, "int2", "b"},
	{23, "int4", "b"},
	{25, "text", "b"},
	{26, "oid", "b"},
	{114, "json", "b"},
	{142, "xml", "b"},
	{700, "float4", "b"},
	{701, "float8", "b"},
	{1042, "bpchar", "b"},
	{1043, "varchar", "b"},
	{1082, "date", "b"},
	{1083, "time", "b"},
	{1114, "timestamp", "b"},
	{1184, "timestamptz", "b"},
	{1186, "interval", "b"},
	{1700, "numeric", "b"},
	{2950, "uuid", "b"},
	{3802, "jsonb", "b"},
}

// handlePgTypeRows answers Rails's pg_type catalog query with one row per
// standard Postgres type. The query column list varies (Rails 7 selects
// oid, typname, typelem, typdelim, typinput, rngsubtype, typtype,
// typbasetype) — emit values for the columns the query asks for, NULL
// for any unrecognised name.
func (s *session) handlePgTypeRows(query string, cols []string) error {
	colDescs := make([]sql.ColDescriptor, len(cols))
	for i, name := range cols {
		switch strings.ToLower(name) {
		case "oid":
			colDescs[i] = sql.ColDescriptor{Column: name, Type: sql.IntegerType}
		default:
			colDescs[i] = sql.ColDescriptor{Column: name, Type: sql.VarcharType}
		}
	}
	if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
		return err
	}

	rows := make([]*sql.Row, 0, len(stdPgTypes))
	for _, t := range stdPgTypes {
		vals := make([]sql.TypedValue, len(cols))
		for i, name := range cols {
			switch strings.ToLower(name) {
			case "oid":
				vals[i] = sql.NewInteger(t.oid)
			case "typname":
				vals[i] = sql.NewVarchar(t.typname)
			case "typtype":
				vals[i] = sql.NewVarchar(t.typtype)
			case "typelem":
				vals[i] = sql.NewVarchar("0")
			case "typdelim":
				vals[i] = sql.NewVarchar(",")
			case "typinput":
				vals[i] = sql.NewVarchar(t.typname + "in")
			case "rngsubtype":
				vals[i] = sql.NewNull(sql.VarcharType)
			case "typbasetype":
				vals[i] = sql.NewVarchar("0")
			case "typcategory":
				vals[i] = sql.NewVarchar("U")
			case "typnotnull":
				vals[i] = sql.NewVarchar("false")
			case "typdefault":
				vals[i] = sql.NewNull(sql.VarcharType)
			default:
				vals[i] = sql.NewNull(sql.VarcharType)
			}
		}
		rows = append(rows, &sql.Row{ValuesByPosition: vals})
	}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}
	s.log.Infof("pgcompat: pg_type query — returned %d standard type rows", len(rows))
	return nil
}

// handlePgSystemQueryDataOnly is like handlePgSystemQuery but skips
// RowDescription (for extended query protocol where Describe already sent it).
func (s *session) handlePgSystemQueryDataOnly(query string) error {
	cols := extractColumnNames(query)
	if len(cols) == 0 {
		return nil
	}

	// pg_type rows are needed by Rails' type registry. In Extended Query
	// mode the Describe has already sent RowDescription, so emit only the
	// DataRow set.
	lq := strings.ToLower(query)
	if strings.Contains(lq, "pg_type") {
		rows := make([]*sql.Row, 0, len(stdPgTypes))
		for _, t := range stdPgTypes {
			vals := make([]sql.TypedValue, len(cols))
			for i, name := range cols {
				switch strings.ToLower(name) {
				case "oid":
					vals[i] = sql.NewInteger(t.oid)
				case "typname":
					vals[i] = sql.NewVarchar(t.typname)
				case "typtype":
					vals[i] = sql.NewVarchar(t.typtype)
				case "typelem":
					vals[i] = sql.NewVarchar("0")
				case "typdelim":
					vals[i] = sql.NewVarchar(",")
				case "typinput":
					vals[i] = sql.NewVarchar(t.typname + "in")
				case "rngsubtype":
					vals[i] = sql.NewNull(sql.VarcharType)
				case "typbasetype":
					vals[i] = sql.NewVarchar("0")
				default:
					vals[i] = sql.NewNull(sql.VarcharType)
				}
			}
			rows = append(rows, &sql.Row{ValuesByPosition: vals})
		}
		if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
			return err
		}
		s.log.Infof("pgcompat: pg_type query (ext mode) — emitted %d type rows", len(rows))
		return nil
	}
	if strings.Contains(lq, "pg_range") {
		s.log.Infof("pgcompat: pg_range query (ext mode) — returning 0 rows")
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

// pgTablesNameFilterLitRe extracts a literal `tablename = '<name>'` from
// a pg_tables WHERE clause for the simple-query path (no binds).
var pgTablesNameFilterLitRe = regexp.MustCompile(`(?i)\btablename\s*=\s*'([^']+)'`)

// pgTablesNameFilterParamRe extracts a `tablename = $N` reference from
// the extended-query path so the bound value can be looked up.
var pgTablesNameFilterParamRe = regexp.MustCompile(`(?i)\btablename\s*=\s*\$(\d+)`)

// pgTablesSchemaFilterLitRe extracts a literal `schemaname = '<name>'`
// filter — XORM and others typically scope to the public schema.
var pgTablesSchemaFilterLitRe = regexp.MustCompile(`(?i)\bschemaname\s*=\s*'([^']+)'`)

// handlePgTablesQuery emulates the standard `pg_tables` view by
// enumerating immudb's actual catalog and applying a WHERE filter on
// `tablename` (literal or bound) when present. ORM IsTableExist probes
// rely on this view, so a generic 1-row response would either say
// "every table exists" (skip every CREATE) or "no table exists" (CREATE
// a table that already exists). Real catalog lookup avoids both.
//
// extMode controls whether RowDescription is also emitted (simple-query
// path) or skipped (extended-query path, where Describe already sent it).
func (s *session) handlePgTablesQuery(query string, params []*schema.NamedParam, extMode bool) error {
	cols := extractColumnNames(query)
	if len(cols) == 0 || (len(cols) == 1 && cols[0] == "*") {
		// Default to PG's pg_tables column set.
		cols = []string{
			"schemaname", "tablename", "tableowner", "tablespace",
			"hasindexes", "hasrules", "hastriggers", "rowsecurity",
		}
	}

	colDescs := make([]sql.ColDescriptor, len(cols))
	for i, name := range cols {
		colType := sql.VarcharType
		switch strings.ToLower(name) {
		case "hasindexes", "hasrules", "hastriggers", "rowsecurity":
			colType = sql.BooleanType
		}
		colDescs[i] = sql.ColDescriptor{Column: name, Type: colType}
	}

	if !extMode {
		if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
			return err
		}
	}

	// Extract WHERE filters.
	var nameFilter string
	if m := pgTablesNameFilterLitRe.FindStringSubmatch(query); m != nil {
		nameFilter = m[1]
	} else if m := pgTablesNameFilterParamRe.FindStringSubmatch(query); m != nil {
		idx, _ := strconv.Atoi(m[1])
		key := fmt.Sprintf("param%d", idx)
		for _, p := range params {
			if p.Name == key {
				if v, ok := schema.RawValue(p.Value).(string); ok {
					nameFilter = strings.Trim(v, `"`)
				}
				break
			}
		}
	}

	schemaFilter := "public"
	if m := pgTablesSchemaFilterLitRe.FindStringSubmatch(query); m != nil {
		schemaFilter = m[1]
	}

	tx, err := s.db.NewSQLTx(s.ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		s.log.Infof("pgcompat: pg_tables: begin tx: %v — emitting 0 rows", err)
		return nil
	}
	defer tx.Cancel()

	allTables := tx.Catalog().GetTables()
	rows := make([]*sql.Row, 0, len(allTables))
	for _, t := range allTables {
		if nameFilter != "" && t.Name() != nameFilter {
			continue
		}
		vals := make([]sql.TypedValue, len(cols))
		for i, name := range cols {
			switch strings.ToLower(name) {
			case "schemaname":
				vals[i] = sql.NewVarchar(schemaFilter)
			case "tablename":
				vals[i] = sql.NewVarchar(t.Name())
			case "tableowner":
				vals[i] = sql.NewVarchar("immudb")
			case "tablespace":
				vals[i] = sql.NewVarchar("pg_default")
			case "hasindexes":
				vals[i] = sql.NewBool(len(t.GetIndexes()) > 0)
			case "hasrules", "hastriggers", "rowsecurity":
				vals[i] = sql.NewBool(false)
			default:
				vals[i] = sql.NewNull(sql.VarcharType)
			}
		}
		rows = append(rows, &sql.Row{ValuesByPosition: vals})
	}

	s.log.Infof("pgcompat: pg_tables: %d table(s) match filter (nameFilter=%q schema=%q catalogTotal=%d)",
		len(rows), nameFilter, schemaFilter, len(allTables))

	if len(rows) > 0 {
		if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
			return err
		}
	}
	return nil
}

// handleXormColumnsQuery emulates XORM's column-introspection query
// (see xormColumnsRe in stmts_handler.go). XORM walks pg_attribute /
// pg_class / pg_type / information_schema.columns and reads:
//
//   column_name, column_default, is_nullable, data_type,
//   character_maximum_length, description, primarykey, uniquekey
//
// Bind values: $1 = c.relname (table name), $2 = s.table_schema (== public).
//
// We synthesise one row per column from immudb's catalog. Without this,
// the query fell to the generic 1-row pgAdminProbe handler, which filled
// `column_name` with NULL, and XORM's subsequent
//   sql: Scan error on column index 0, name "column_name":
//        converting NULL to string is unsupported
// stopped initialisation cold.
func (s *session) handleXormColumnsQuery(query string, params []*schema.NamedParam, extMode bool) error {
	cols := extractColumnNames(query)
	// Build column descriptors. XORM expects strings for the textual
	// columns and bools for primarykey/uniquekey.
	colDescs := make([]sql.ColDescriptor, len(cols))
	for i, name := range cols {
		colType := sql.VarcharType
		switch strings.ToLower(name) {
		case "primarykey", "uniquekey":
			colType = sql.BooleanType
		case "character_maximum_length":
			colType = sql.IntegerType
		}
		colDescs[i] = sql.ColDescriptor{Column: name, Type: colType}
	}
	if !extMode {
		if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
			return err
		}
	}

	// Resolve $1 (table name). paramAsString tolerates both string- and
	// bytes-shaped parameter values, so it doesn't matter whether the
	// client encoded the bind as text or binary format.
	tableName := ""
	if len(params) >= 1 {
		tableName = paramAsString(params[0])
	}
	for _, p := range params {
		if p.Name == "param1" {
			tableName = paramAsString(p)
		}
	}

	tx, err := s.db.NewSQLTx(s.ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		s.log.Infof("pgcompat: xormColumns: begin tx: %v", err)
		return nil
	}
	defer tx.Cancel()

	catalog := tx.Catalog()
	table, err := catalog.GetTableByName(tableName)
	if err != nil {
		s.log.Infof("pgcompat: xormColumns: table %q absent — emitting 0 rows", tableName)
		return nil
	}

	// Identify primary-key columns (single-column PK assumed; immudb's
	// PK is a single composite index rooted at table.PrimaryIndex()).
	pkCols := map[string]bool{}
	if pk := table.PrimaryIndex(); pk != nil {
		for _, c := range pk.Cols() {
			pkCols[c.Name()] = true
		}
	}
	uniqueCols := map[string]bool{}
	for _, idx := range table.GetIndexes() {
		if !idx.IsUnique() {
			continue
		}
		for _, c := range idx.Cols() {
			uniqueCols[c.Name()] = true
		}
	}

	rows := make([]*sql.Row, 0, len(table.Cols()))
	for _, c := range table.Cols() {
		pgTypeName, _ := immudbToPGType(c.Type())
		var maxLen sql.TypedValue = sql.NewNull(sql.IntegerType)
		if c.Type() == sql.VarcharType && c.MaxLen() > 0 {
			maxLen = sql.NewInteger(int64(c.MaxLen()))
		}
		isNullable := "YES"
		if c.IsNullable() == false {
			isNullable = "NO"
		}
		var defaultExpr sql.TypedValue = sql.NewNull(sql.VarcharType)
		if c.HasDefault() {
			defaultExpr = sql.NewVarchar(c.DefaultValue().String())
		}

		vals := make([]sql.TypedValue, len(cols))
		for i, name := range cols {
			switch strings.ToLower(name) {
			case "column_name":
				vals[i] = sql.NewVarchar(c.Name())
			case "column_default":
				vals[i] = defaultExpr
			case "is_nullable":
				vals[i] = sql.NewVarchar(isNullable)
			case "data_type":
				vals[i] = sql.NewVarchar(pgTypeName)
			case "character_maximum_length":
				vals[i] = maxLen
			case "description":
				vals[i] = sql.NewNull(sql.VarcharType)
			case "primarykey":
				vals[i] = sql.NewBool(pkCols[c.Name()])
			case "uniquekey":
				vals[i] = sql.NewBool(uniqueCols[c.Name()])
			default:
				vals[i] = sql.NewNull(sql.VarcharType)
			}
		}
		rows = append(rows, &sql.Row{ValuesByPosition: vals})
	}

	s.log.Infof("pgcompat: xormColumns: table=%q cols=%d", tableName, len(rows))
	if len(rows) > 0 {
		if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
			return err
		}
	}
	return nil
}

// handlePgIndexesQuery emulates the standard `pg_indexes` view by
// enumerating immudb's catalog. ORM index introspection (XORM, GORM,
// JDBC) reads `indexname` and `indexdef` and crashes on the canned NULL
// row. We synthesise one row per (table, index) and apply optional
// `tablename = '…'` / `= $1` and `schemaname = …` filters.
func (s *session) handlePgIndexesQuery(query string, params []*schema.NamedParam, extMode bool) error {
	cols := extractColumnNames(query)
	if len(cols) == 0 {
		cols = []string{"schemaname", "tablename", "indexname", "tablespace", "indexdef"}
	}

	colDescs := make([]sql.ColDescriptor, len(cols))
	for i, name := range cols {
		colDescs[i] = sql.ColDescriptor{Column: name, Type: sql.VarcharType}
	}
	if !extMode {
		if _, err := s.writeMessage(buildMultiColRowDescription(colDescs)); err != nil {
			return err
		}
	}

	// Reuse pg_tables filter regex shapes for tablename / schemaname.
	var nameFilter string
	if m := pgTablesNameFilterLitRe.FindStringSubmatch(query); m != nil {
		nameFilter = m[1]
	} else if m := pgTablesNameFilterParamRe.FindStringSubmatch(query); m != nil {
		idx, _ := strconv.Atoi(m[1])
		key := fmt.Sprintf("param%d", idx)
		for _, p := range params {
			if p.Name == key {
				nameFilter = paramAsString(p)
				break
			}
		}
	}
	schemaFilter := "public"
	if m := pgTablesSchemaFilterLitRe.FindStringSubmatch(query); m != nil {
		schemaFilter = m[1]
	}

	tx, err := s.db.NewSQLTx(s.ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil
	}
	defer tx.Cancel()

	allTables := tx.Catalog().GetTables()
	rows := make([]*sql.Row, 0)
	for _, t := range allTables {
		if nameFilter != "" && t.Name() != nameFilter {
			continue
		}
		for _, idx := range t.GetIndexes() {
			indexName := fmt.Sprintf("%s_idx_%d", t.Name(), idx.ID())
			indexDef := buildIndexDef(t.Name(), idx)
			vals := make([]sql.TypedValue, len(cols))
			for i, name := range cols {
				switch strings.ToLower(name) {
				case "schemaname":
					vals[i] = sql.NewVarchar(schemaFilter)
				case "tablename":
					vals[i] = sql.NewVarchar(t.Name())
				case "indexname":
					vals[i] = sql.NewVarchar(indexName)
				case "tablespace":
					vals[i] = sql.NewVarchar("pg_default")
				case "indexdef":
					vals[i] = sql.NewVarchar(indexDef)
				default:
					vals[i] = sql.NewNull(sql.VarcharType)
				}
			}
			rows = append(rows, &sql.Row{ValuesByPosition: vals})
		}
	}

	s.log.Infof("pgcompat: pg_indexes: %d index(es) match (nameFilter=%q schema=%q)",
		len(rows), nameFilter, schemaFilter)
	if len(rows) > 0 {
		if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
			return err
		}
	}
	return nil
}

// buildIndexDef synthesises a canonical CREATE INDEX statement for an
// index. Real Postgres returns the exact original CREATE INDEX SQL via
// pg_get_indexdef(); we don't store the source, so reconstruct from the
// catalog (good enough for ORM "does this index exist + cover X" checks).
func buildIndexDef(tableName string, idx *sql.Index) string {
	colNames := make([]string, 0, len(idx.Cols()))
	for _, c := range idx.Cols() {
		colNames = append(colNames, c.Name())
	}
	verb := "CREATE INDEX"
	if idx.IsUnique() {
		verb = "CREATE UNIQUE INDEX"
	}
	return fmt.Sprintf("%s ON %s (%s)", verb, tableName, strings.Join(colNames, ", "))
}

// paramAsString unwraps a NamedParam into a Go string regardless of
// whether the wire delivered it as text (SQLValue_S) or as raw bytes
// (SQLValue_Bs — happens when the parameter type was reported as
// AnyType/bytea and the client encoded a string as raw UTF-8 bytes).
func paramAsString(p *schema.NamedParam) string {
	switch v := schema.RawValue(p.Value).(type) {
	case string:
		return strings.Trim(v, `"`)
	case []byte:
		return strings.Trim(string(v), `"`)
	}
	return ""
}
