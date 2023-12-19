/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/codenotary/immudb/pkg/client/errors"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/emptypb"
)

const SQLPrefix byte = 2

// SQLExec performs a modifying SQL query within the transaction.
// Such query does not return SQL result.
func (c *immuClient) SQLExec(ctx context.Context, sql string, params map[string]interface{}) (*schema.SQLExecResult, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLExec(ctx, &schema.SQLExecRequest{Sql: sql, Params: namedParams})
}

// SQLQuery performs a query (read-only) operation.
//
// The renewSnapshot parameter is deprecated and  is ignored by the server.
func (c *immuClient) SQLQuery(ctx context.Context, sql string, params map[string]interface{}, renewSnapshot bool) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}

	namedParams, err := schema.EncodeParams(params)
	if err != nil {
		return nil, err
	}

	return c.ServiceClient.SQLQuery(ctx, &schema.SQLQueryRequest{Sql: sql, Params: namedParams, ReuseSnapshot: !renewSnapshot})
}

// ListTables returns a list of SQL tables.
func (c *immuClient) ListTables(ctx context.Context) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.ListTables(ctx, &emptypb.Empty{})
}

// Describe table returns a description of a table structure.
func (c *immuClient) DescribeTable(ctx context.Context, tableName string) (*schema.SQLQueryResult, error) {
	if !c.IsConnected() {
		return nil, errors.FromError(ErrNotConnected)
	}
	return c.ServiceClient.DescribeTable(ctx, &schema.Table{TableName: tableName})
}

// VerifyRow reads a single row from the database with additional validation of server-provided proof.
//
// The row parameter should contain row from a single table, either returned from
// query or manually assembled. The table parameter contains the name of the table
// where the row comes from. The pkVals argument is an array containing values for
// the primary key of the row. The row parameter does not have to contain all
// columns of the table. Once the row itself is verified, only those columns that
// are in the row will be compared against the verified row retrieved from the database.
func (c *immuClient) VerifyRow(ctx context.Context, row *schema.Row, table string, pkVals []*schema.SQLValue) error {
	if row == nil || len(table) == 0 || len(pkVals) == 0 {
		return ErrIllegalArguments
	}

	if len(row.Columns) == 0 || len(row.Columns) != len(row.Values) {
		return sql.ErrCorruptedData
	}

	if !c.IsConnected() {
		return ErrNotConnected
	}

	err := c.StateService.CacheLock()
	if err != nil {
		return err
	}
	defer c.StateService.CacheUnlock()

	state, err := c.StateService.GetState(ctx, c.currentDatabase())
	if err != nil {
		return err
	}

	vEntry, err := c.ServiceClient.VerifiableSQLGet(ctx, &schema.VerifiableSQLGetRequest{
		SqlGetRequest: &schema.SQLGetRequest{Table: table, PkValues: pkVals},
		ProveSinceTx:  state.TxId,
	})
	if err != nil {
		return err
	}

	if len(vEntry.PKIDs) < len(pkVals) {
		return ErrIllegalArguments
	}

	entrySpecDigest, err := store.EntrySpecDigestFor(int(vEntry.VerifiableTx.Tx.Header.Version))
	if err != nil {
		return err
	}

	inclusionProof := schema.InclusionProofFromProto(vEntry.InclusionProof)
	dualProof := schema.DualProofFromProto(vEntry.VerifiableTx.DualProof)

	var eh [sha256.Size]byte

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	vTx := vEntry.SqlEntry.Tx

	dbID := vEntry.DatabaseId
	tableID := vEntry.TableId

	valbuf := bytes.Buffer{}

	for i, pkVal := range pkVals {
		pkID := vEntry.PKIDs[i]

		pkType, ok := vEntry.ColTypesById[pkID]
		if !ok {
			return sql.ErrCorruptedData
		}

		pkLen, ok := vEntry.ColLenById[pkID]
		if !ok {
			return sql.ErrCorruptedData
		}

		pkEncVal, _, err := sql.EncodeRawValueAsKey(schema.RawValue(pkVal), pkType, int(pkLen))
		if err != nil {
			return err
		}

		_, err = valbuf.Write(pkEncVal)
		if err != nil {
			return err
		}
	}

	pkKey := sql.MapKey(
		[]byte{SQLPrefix},
		sql.RowPrefix,
		sql.EncodeID(dbID),
		sql.EncodeID(tableID),
		sql.EncodeID(sql.PKIndexID),
		valbuf.Bytes())

	decodedRow, err := decodeRow(vEntry.SqlEntry.Value, vEntry.ColTypesById, vEntry.MaxColId)
	if err != nil {
		return err
	}

	err = verifyRowAgainst(row, decodedRow, vEntry.ColIdsByName)
	if err != nil {
		return err
	}

	e := &store.EntrySpec{Key: pkKey, Value: vEntry.SqlEntry.Value}

	if state.TxId <= vTx {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.TargetTxHeader.EH)

		sourceID = state.TxId
		sourceAlh = schema.DigestFromProto(state.TxHash)
		targetID = vTx
		targetAlh = dualProof.TargetTxHeader.Alh()
	} else {
		eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.SourceTxHeader.EH)

		sourceID = vTx
		sourceAlh = dualProof.SourceTxHeader.Alh()
		targetID = state.TxId
		targetAlh = schema.DigestFromProto(state.TxHash)
	}

	verifies := store.VerifyInclusion(
		inclusionProof,
		entrySpecDigest(e),
		eh)
	if !verifies {
		return store.ErrCorruptedData
	}

	if state.TxId > 0 {
		err := c.verifyDualProof(
			ctx,
			dualProof,
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		)
		if err != nil {
			return err
		}
	}

	newState := &schema.ImmutableState{
		Db:        c.currentDatabase(),
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: vEntry.VerifiableTx.Signature,
	}

	if c.serverSigningPubKey != nil {
		err := newState.CheckSignature(c.serverSigningPubKey)
		if err != nil {
			return err
		}
	}

	err = c.StateService.SetState(c.currentDatabase(), newState)
	if err != nil {
		return err
	}

	return nil
}

func verifyRowAgainst(row *schema.Row, decodedRow map[uint32]*schema.SQLValue, colIdsByName map[string]uint32) error {
	for i, colName := range row.Columns {
		colID, ok := colIdsByName[colName]
		if !ok {
			return sql.ErrColumnDoesNotExist
		}

		val := row.Values[i]

		if val == nil || val.Value == nil {
			return sql.ErrCorruptedData
		}

		decodedVal, ok := decodedRow[colID]
		if !ok {
			_, isNull := val.Value.(*schema.SQLValue_Null)
			if isNull {
				continue
			}
			return sql.ErrCorruptedData
		}

		if decodedVal == nil || decodedVal.Value == nil {
			return sql.ErrCorruptedData
		}

		equals, err := val.Value.(schema.SqlValue).Equal(decodedVal.Value.(schema.SqlValue))
		if err != nil {
			return err
		}
		if !equals {
			return sql.ErrCorruptedData
		}
	}

	return nil
}

func decodeRow(encodedRow []byte, colTypes map[uint32]sql.SQLValueType, maxColID uint32) (map[uint32]*schema.SQLValue, error) {
	off := 0

	if len(encodedRow) < off+sql.EncLenLen {
		return nil, sql.ErrCorruptedData
	}

	colsCount := binary.BigEndian.Uint32(encodedRow[off:])
	off += sql.EncLenLen

	values := make(map[uint32]*schema.SQLValue, colsCount)

	for i := 0; i < int(colsCount); i++ {
		if len(encodedRow) < off+sql.EncIDLen {
			return nil, sql.ErrCorruptedData
		}

		colID := binary.BigEndian.Uint32(encodedRow[off:])
		off += sql.EncIDLen

		colType, ok := colTypes[colID]
		if !ok {
			// Support for dropped columns
			if colID > maxColID {
				return nil, sql.ErrCorruptedData
			}

			vlen, voff, err := sql.DecodeValueLength(encodedRow[off:])
			if err != nil {
				return nil, err
			}

			off += vlen
			off += voff
			continue
		}

		val, n, err := sql.DecodeValue(encodedRow[off:], colType)
		if err != nil {
			return nil, err
		}

		values[colID] = typedValueToRowValue(val)
		off += n
	}

	return values, nil
}

func typedValueToRowValue(tv sql.TypedValue) *schema.SQLValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: tv.RawValue().(int64)}}
		}
	case sql.VarcharType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv.RawValue().(string)}}
		}
	case sql.UUIDType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv.RawValue().(string)}}
		}
	case sql.BooleanType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv.RawValue().(bool)}}
		}
	case sql.BLOBType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv.RawValue().([]byte)}}
		}
	case sql.TimestampType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Ts{Ts: sql.TimeToInt64(tv.RawValue().(time.Time))}}
		}
	case sql.Float64Type:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_F{F: tv.RawValue().(float64)}}
		}
	}
	return nil
}
