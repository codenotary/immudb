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
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
)

// immudbState handles SELECT immudb_state() — returns the current immutable database state
// including transaction ID, hash, and database name.
func (s *session) immudbState() error {
	state, err := s.db.CurrentState()
	if err != nil {
		return err
	}

	cols := []sql.ColDescriptor{
		{Column: "db", Type: sql.VarcharType},
		{Column: "tx_id", Type: sql.IntegerType},
		{Column: "tx_hash", Type: sql.VarcharType},
		{Column: "precommitted_tx_id", Type: sql.IntegerType},
		{Column: "precommitted_tx_hash", Type: sql.VarcharType},
		{Column: "signature", Type: sql.VarcharType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	txHash := hex.EncodeToString(state.TxHash)
	precommittedTxHash := hex.EncodeToString(state.PrecommittedTxHash)

	sig := ""
	if state.Signature != nil {
		sig = hex.EncodeToString(state.Signature.Signature)
	}

	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewVarchar(state.Db),
			sql.NewInteger(int64(state.TxId)),
			sql.NewVarchar(txHash),
			sql.NewInteger(int64(state.PrecommittedTxId)),
			sql.NewVarchar(precommittedTxHash),
			sql.NewVarchar(sig),
		},
		ValuesBySelector: map[string]sql.TypedValue{
			"db":                   sql.NewVarchar(state.Db),
			"tx_id":               sql.NewInteger(int64(state.TxId)),
			"tx_hash":             sql.NewVarchar(txHash),
			"precommitted_tx_id":  sql.NewInteger(int64(state.PrecommittedTxId)),
			"precommitted_tx_hash": sql.NewVarchar(precommittedTxHash),
			"signature":           sql.NewVarchar(sig),
		},
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// immudbVerifyRow handles SELECT immudb_verify_row(table, pk_value) — performs a verifiable
// SQL get and returns verification status, transaction ID, and proof details.
func (s *session) immudbVerifyRow(args string) error {
	tableName, pkValues, err := parseVerifyArgs(args)
	if err != nil {
		return err
	}

	req := &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{
			Table:    tableName,
			PkValues: pkValues,
		},
		ProveSinceTx: 0,
	}

	entry, err := s.db.VerifiableSQLGet(s.ctx, req)
	if err != nil {
		return s.writeVerifyError(err)
	}

	cols := []sql.ColDescriptor{
		{Column: "verified", Type: sql.VarcharType},
		{Column: "table_name", Type: sql.VarcharType},
		{Column: "tx_id", Type: sql.IntegerType},
		{Column: "revision", Type: sql.IntegerType},
		{Column: "entry_key", Type: sql.VarcharType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	txID := int64(0)
	if entry.SqlEntry != nil {
		txID = int64(entry.SqlEntry.Tx)
	}

	revision := int64(0)
	if entry.SqlEntry != nil && entry.SqlEntry.Metadata != nil && !entry.SqlEntry.Metadata.GetDeleted() {
		revision = 1
	}

	entryKey := ""
	if entry.SqlEntry != nil {
		entryKey = hex.EncodeToString(entry.SqlEntry.Key)
	}

	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewVarchar("true"),
			sql.NewVarchar(tableName),
			sql.NewInteger(txID),
			sql.NewInteger(revision),
			sql.NewVarchar(entryKey),
		},
		ValuesBySelector: map[string]sql.TypedValue{
			"verified":   sql.NewVarchar("true"),
			"table_name": sql.NewVarchar(tableName),
			"tx_id":      sql.NewInteger(txID),
			"revision":   sql.NewInteger(revision),
			"entry_key":  sql.NewVarchar(entryKey),
		},
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// immudbVerifyTx handles SELECT immudb_verify_tx(tx_id) — performs a verifiable
// transaction lookup and returns verification status with proof details.
func (s *session) immudbVerifyTx(args string) error {
	txID, err := strconv.ParseUint(strings.TrimSpace(args), 10, 64)
	if err != nil {
		return fmt.Errorf("immudb_verify_tx: invalid transaction ID: %s", args)
	}

	req := &schema.VerifiableTxRequest{
		Tx:           txID,
		ProveSinceTx: 0,
	}

	vtx, err := s.db.VerifiableTxByID(s.ctx, req)
	if err != nil {
		return s.writeVerifyError(err)
	}

	cols := []sql.ColDescriptor{
		{Column: "verified", Type: sql.VarcharType},
		{Column: "tx_id", Type: sql.IntegerType},
		{Column: "timestamp", Type: sql.IntegerType},
		{Column: "nentries", Type: sql.IntegerType},
		{Column: "eh", Type: sql.VarcharType},
		{Column: "bl_tx_id", Type: sql.IntegerType},
		{Column: "bl_root", Type: sql.VarcharType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	hdr := vtx.Tx.Header
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewVarchar("true"),
			sql.NewInteger(int64(hdr.Id)),
			sql.NewInteger(hdr.Ts),
			sql.NewInteger(int64(hdr.Nentries)),
			sql.NewVarchar(hex.EncodeToString(hdr.EH)),
			sql.NewInteger(int64(hdr.BlTxId)),
			sql.NewVarchar(hex.EncodeToString(hdr.BlRoot)),
		},
		ValuesBySelector: map[string]sql.TypedValue{
			"verified":  sql.NewVarchar("true"),
			"tx_id":     sql.NewInteger(int64(hdr.Id)),
			"timestamp": sql.NewInteger(hdr.Ts),
			"nentries":  sql.NewInteger(int64(hdr.Nentries)),
			"eh":        sql.NewVarchar(hex.EncodeToString(hdr.EH)),
			"bl_tx_id":  sql.NewInteger(int64(hdr.BlTxId)),
			"bl_root":   sql.NewVarchar(hex.EncodeToString(hdr.BlRoot)),
		},
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// immudbHistory handles SELECT immudb_history(key) — retrieves all historical
// versions of a key with transaction metadata.
func (s *session) immudbHistory(args string) error {
	key := strings.TrimSpace(args)
	key = trimQuotes(key)

	req := &schema.HistoryRequest{
		Key:   []byte(key),
		Limit: 100,
		Desc:  true,
	}

	entries, err := s.db.History(s.ctx, req)
	if err != nil {
		return s.writeVerifyError(err)
	}

	cols := []sql.ColDescriptor{
		{Column: "tx_id", Type: sql.IntegerType},
		{Column: "key", Type: sql.VarcharType},
		{Column: "value", Type: sql.VarcharType},
		{Column: "revision", Type: sql.IntegerType},
		{Column: "expired", Type: sql.VarcharType},
		{Column: "deleted", Type: sql.VarcharType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	rows := make([]*sql.Row, 0, len(entries.Entries))
	for _, e := range entries.Entries {
		deleted := "false"
		if e.Metadata != nil && e.Metadata.GetDeleted() {
			deleted = "true"
		}

		expired := "false"
		if e.Expired {
			expired = "true"
		}

		row := &sql.Row{
			ValuesByPosition: []sql.TypedValue{
				sql.NewInteger(int64(e.Tx)),
				sql.NewVarchar(string(e.Key)),
				sql.NewVarchar(hex.EncodeToString(e.Value)),
				sql.NewInteger(int64(e.Revision)),
				sql.NewVarchar(expired),
				sql.NewVarchar(deleted),
			},
			ValuesBySelector: map[string]sql.TypedValue{
				"tx_id":    sql.NewInteger(int64(e.Tx)),
				"key":      sql.NewVarchar(string(e.Key)),
				"value":    sql.NewVarchar(hex.EncodeToString(e.Value)),
				"revision": sql.NewInteger(int64(e.Revision)),
				"expired":  sql.NewVarchar(expired),
				"deleted":  sql.NewVarchar(deleted),
			},
		}
		rows = append(rows, row)
	}

	if len(rows) > 0 {
		if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
			return err
		}
	}

	return nil
}

// immudbTxByID handles SELECT immudb_tx(tx_id) — retrieves transaction details
// including header metadata and entry count.
func (s *session) immudbTxByID(args string) error {
	txID, err := strconv.ParseUint(strings.TrimSpace(args), 10, 64)
	if err != nil {
		return fmt.Errorf("immudb_tx: invalid transaction ID: %s", args)
	}

	req := &schema.TxRequest{
		Tx: txID,
	}

	tx, err := s.db.TxByID(s.ctx, req)
	if err != nil {
		return s.writeVerifyError(err)
	}

	cols := []sql.ColDescriptor{
		{Column: "tx_id", Type: sql.IntegerType},
		{Column: "timestamp", Type: sql.IntegerType},
		{Column: "nentries", Type: sql.IntegerType},
		{Column: "prev_alh", Type: sql.VarcharType},
		{Column: "eh", Type: sql.VarcharType},
		{Column: "bl_tx_id", Type: sql.IntegerType},
		{Column: "bl_root", Type: sql.VarcharType},
		{Column: "version", Type: sql.IntegerType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	hdr := tx.Header
	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewInteger(int64(hdr.Id)),
			sql.NewInteger(hdr.Ts),
			sql.NewInteger(int64(hdr.Nentries)),
			sql.NewVarchar(hex.EncodeToString(hdr.PrevAlh)),
			sql.NewVarchar(hex.EncodeToString(hdr.EH)),
			sql.NewInteger(int64(hdr.BlTxId)),
			sql.NewVarchar(hex.EncodeToString(hdr.BlRoot)),
			sql.NewInteger(int64(hdr.Version)),
		},
		ValuesBySelector: map[string]sql.TypedValue{
			"tx_id":     sql.NewInteger(int64(hdr.Id)),
			"timestamp": sql.NewInteger(hdr.Ts),
			"nentries":  sql.NewInteger(int64(hdr.Nentries)),
			"prev_alh":  sql.NewVarchar(hex.EncodeToString(hdr.PrevAlh)),
			"eh":        sql.NewVarchar(hex.EncodeToString(hdr.EH)),
			"bl_tx_id":  sql.NewInteger(int64(hdr.BlTxId)),
			"bl_root":   sql.NewVarchar(hex.EncodeToString(hdr.BlRoot)),
			"version":   sql.NewInteger(int64(hdr.Version)),
		},
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// writeVerifyError writes a verification error as a single-row result with the error message,
// so that clients can handle failures as data rather than connection errors.
func (s *session) writeVerifyError(origErr error) error {
	cols := []sql.ColDescriptor{
		{Column: "verified", Type: sql.VarcharType},
		{Column: "error", Type: sql.VarcharType},
	}

	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	row := &sql.Row{
		ValuesByPosition: []sql.TypedValue{
			sql.NewVarchar("false"),
			sql.NewVarchar(origErr.Error()),
		},
		ValuesBySelector: map[string]sql.TypedValue{
			"verified": sql.NewVarchar("false"),
			"error":    sql.NewVarchar(origErr.Error()),
		},
	}

	if _, err := s.writeMessage(bm.DataRow([]*sql.Row{row}, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// parseVerifyArgs parses "table_name, pk_value1, pk_value2, ..." from the arguments
// of immudb_verify_row(). Returns the table name and a list of protobuf-encoded primary key values.
func parseVerifyArgs(args string) (string, []*schema.SQLValue, error) {
	parts := strings.Split(args, ",")
	if len(parts) < 2 {
		return "", nil, fmt.Errorf("immudb_verify_row requires at least 2 arguments: table_name, pk_value")
	}

	tableName := trimQuotes(strings.TrimSpace(parts[0]))

	pkValues := make([]*schema.SQLValue, 0, len(parts)-1)
	for _, p := range parts[1:] {
		p = strings.TrimSpace(p)
		p = trimQuotes(p)

		// Try integer first
		if intVal, err := strconv.ParseInt(p, 10, 64); err == nil {
			pkValues = append(pkValues, &schema.SQLValue{Value: &schema.SQLValue_N{N: intVal}})
			continue
		}

		// Default to string
		pkValues = append(pkValues, &schema.SQLValue{Value: &schema.SQLValue_S{S: p}})
	}

	return tableName, pkValues, nil
}

// showSettings maps SHOW parameter names to their values for ORM compatibility.
var showSettings = map[string]string{
	"server_version":              "14.0",
	"server_version_num":          "140000",
	"standard_conforming_strings": "on",
	"client_encoding":             "UTF8",
	"server_encoding":             "UTF8",
	"search_path":                 "\"$user\", public",
	"transaction_isolation":       "serializable",
	"timezone":                    "UTC",
	"datestyle":                   "ISO, MDY",
	"integer_datetimes":           "on",
	"intervalstyle":               "postgres",
	"max_identifier_length":       "63",
	"default_transaction_isolation": "serializable",
	"lc_collate":                  "en_US.UTF-8",
	"lc_ctype":                    "en_US.UTF-8",
}

// handleShow handles SHOW <param> statements by returning a single-row result.
func (s *session) handleShow(param string) error {
	paramLower := strings.ToLower(param)
	value, ok := showSettings[paramLower]
	if !ok {
		value = ""
	}

	cols := []sql.ColDescriptor{{Column: param, Type: sql.VarcharType}}
	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	v := sql.NewVarchar(value)
	rows := []*sql.Row{{
		ValuesByPosition: []sql.TypedValue{v},
		ValuesBySelector: map[string]sql.TypedValue{param: v},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}

	return nil
}

// pgTypeOIDByName maps common PostgreSQL type names to their canonical OIDs.
// Used by handleRegtypeOid to answer `SELECT 'foo'::regtype::oid` queries so
// Rails / ActiveRecord get usable integer OIDs instead of the literal type
// name (which breaks the pg gem's type map and leads to "can't quote Hash").
var pgTypeOIDByName = map[string]int64{
	"bool":             16,
	"boolean":          16,
	"bytea":            17,
	"char":             18,
	"name":             19,
	"int8":             20,
	"bigint":           20,
	"int2":             21,
	"smallint":         21,
	"int4":             23,
	"integer":          23,
	"int":              23,
	"text":             25,
	"oid":              26,
	"json":             114,
	"xml":              142,
	"point":            600,
	"float4":           700,
	"real":             700,
	"float8":           701,
	"double precision": 701,
	"money":            790,
	"bpchar":           1042,
	"character":        1042,
	"varchar":          1043,
	"date":             1082,
	"time":             1083,
	"timestamp":        1114,
	"timestamptz":      1184,
	"interval":         1186,
	"timetz":           1266,
	"bit":              1560,
	"varbit":           1562,
	"numeric":          1700,
	"decimal":          1700,
	"uuid":             2950,
	"jsonb":            3802,
}

// handleRegtypeOid emulates `SELECT 'typename'::regtype::oid` by returning the
// canonical PostgreSQL OID for the requested type. Strips any size/precision
// qualifier (e.g. "decimal(19,4)" -> "decimal", "varchar(255)" -> "varchar").
// Unknown type names return a zero row set so the client falls back to its
// built-in defaults rather than crashing on a bad value.
func (s *session) handleRegtypeOid(typeName string) error {
	cols := []sql.ColDescriptor{{Column: "oid", Type: sql.IntegerType}}
	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	normalized := strings.ToLower(strings.TrimSpace(typeName))
	if idx := strings.IndexByte(normalized, '('); idx > 0 {
		normalized = strings.TrimSpace(normalized[:idx])
	}

	oid, ok := pgTypeOIDByName[normalized]
	if !ok {
		// Empty result: Rails treats this as "type unknown" and skips rather
		// than caching a bogus OID. This is the correct Postgres behaviour
		// when regtype cast fails for an unknown name, though real Postgres
		// would error rather than return empty. Empty is safer here because
		// it lets schema load continue past one unrecognised type.
		return nil
	}

	v := sql.NewInteger(oid)
	rows := []*sql.Row{{
		ValuesByPosition: []sql.TypedValue{v},
		ValuesBySelector: map[string]sql.TypedValue{"oid": v},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}
	return nil
}

// immudbToPGType maps an immudb SQLValueType to a Postgres-style type name
// and OID. Rails uses both to build its internal column descriptor; the OID
// decides which OID::Type subclass handles (de)serialisation for the column.
func immudbToPGType(t sql.SQLValueType) (string, int64) {
	switch t {
	case sql.IntegerType:
		return "bigint", 20
	case sql.Float64Type:
		return "double precision", 701
	case sql.BooleanType:
		return "boolean", 16
	case sql.VarcharType:
		return "character varying", 1043
	case sql.UUIDType:
		return "uuid", 2950
	case sql.BLOBType:
		return "bytea", 17
	case sql.JSONType:
		return "jsonb", 3802
	case sql.TimestampType:
		return "timestamp without time zone", 1114
	}
	return "text", 25
}

// pgAttributeResultCols returns the column descriptor list that handles
// Rails's pg_attribute introspection query. Exported as its own helper so
// ParseMsg can precompute it (via st.Results) — necessary for the
// Extended Query Describe message to send a correct RowDescription before
// Execute runs and emits DataRow.
func pgAttributeResultCols() []sql.ColDescriptor {
	return []sql.ColDescriptor{
		{Column: "attname", Type: sql.VarcharType},
		{Column: "format_type", Type: sql.VarcharType},
		{Column: "pg_get_expr", Type: sql.VarcharType},
		{Column: "attnotnull", Type: sql.BooleanType},
		{Column: "atttypid", Type: sql.IntegerType},
		{Column: "atttypmod", Type: sql.IntegerType},
		{Column: "collname", Type: sql.VarcharType},
		{Column: "comment", Type: sql.VarcharType},
		{Column: "identity", Type: sql.VarcharType},
		{Column: "attgenerated", Type: sql.VarcharType},
	}
}

// handlePgAttributeForTable answers Rails's pg_attribute introspection query
// with real column data sourced from immudb's catalog. Without this, Rails
// cannot resolve column types for ActiveRecord `enum :col, ...` declarations
// (which require a backing database column) and every model with an enum
// raises "Undeclared attribute type for enum 'col'".
//
// Rails expects exactly these columns in order:
//
//	attname, format_type, pg_get_expr, attnotnull,
//	atttypid, atttypmod, collname, comment, identity, attgenerated
func (s *session) handlePgAttributeForTable(tableName string, extQueryMode bool) error {
	cols := pgAttributeResultCols()
	// In Extended Query mode the preceding Describe has already sent
	// RowDescription; emitting it again confuses the client (Rails' pg
	// gem skips the "real" DataRow thinking the second RowDescription
	// is the start of a new result set).
	if !extQueryMode {
		if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
			return err
		}
	}

	tx, err := s.db.NewSQLTx(s.ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		s.log.Infof("pgcompat: pg_attribute intercept: begin tx: %v", err)
		return nil // empty rows — Rails falls back, not strictly an error
	}
	defer tx.Cancel()

	catalog := tx.Catalog()
	table, err := catalog.GetTableByName(tableName)
	if err != nil {
		s.log.Infof("pgcompat: pg_attribute intercept: table %q absent (%v); returning 0 rows", tableName, err)
		return nil
	}

	rows := make([]*sql.Row, 0, len(table.Cols()))
	for _, c := range table.Cols() {
		pgName, pgOID := immudbToPGType(c.Type())

		// format_type mimics Postgres' pretty-printed column type. For
		// VARCHAR we include the length; others stay bare.
		formatted := pgName
		typmod := int64(-1)
		if c.Type() == sql.VarcharType && c.MaxLen() > 0 {
			formatted = fmt.Sprintf("character varying(%d)", c.MaxLen())
			typmod = int64(c.MaxLen() + 4) // PG stores varchar typmod as len + 4
		}

		var defaultExpr sql.TypedValue = sql.NewNull(sql.VarcharType)
		if c.HasDefault() {
			defaultExpr = sql.NewVarchar(c.DefaultValue().String())
		}

		rowVals := []sql.TypedValue{
			sql.NewVarchar(c.Name()),
			sql.NewVarchar(formatted),
			defaultExpr,
			sql.NewBool(!c.IsNullable()),
			sql.NewInteger(pgOID),
			sql.NewInteger(typmod),
			sql.NewNull(sql.VarcharType), // collname
			sql.NewNull(sql.VarcharType), // comment
			sql.NewVarchar(""),           // identity
			sql.NewVarchar(""),           // attgenerated
		}
		rows = append(rows, &sql.Row{ValuesByPosition: rowVals})
	}

	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}
	return nil
}

// handlePgAdvisoryLock answers Rails's migration-lock queries
// (SELECT pg_try_advisory_lock / pg_advisory_unlock) with a single TRUE row.
// immudb has no advisory-lock subsystem, and a single-Rails-container
// deployment does not need one. Returning true is the documented Postgres
// response for "lock acquired", which lets Rails's migration logic proceed.
func (s *session) handlePgAdvisoryLock() error {
	cols := []sql.ColDescriptor{{Column: "result", Type: sql.BooleanType}}
	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}
	v := sql.NewBool(true)
	rows := []*sql.Row{{ValuesByPosition: []sql.TypedValue{v}}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}
	return nil
}

// trimQuotes removes surrounding single or double quotes from a string.
func trimQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
