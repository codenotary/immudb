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

// trimQuotes removes surrounding single or double quotes from a string.
func trimQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
