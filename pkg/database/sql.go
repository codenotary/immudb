/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func (d *db) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	if req == nil || req.SqlGetRequest == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	sqlTx, err := d.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer sqlTx.Cancel()

	table, err := sqlTx.Catalog().GetTableByName(req.SqlGetRequest.Table)
	if err != nil {
		return nil, err
	}

	valbuf := bytes.Buffer{}

	if len(req.SqlGetRequest.PkValues) != len(table.PrimaryIndex().Cols()) {
		return nil, fmt.Errorf(
			"%w: incorrect number of primary key values, expected %d, got %d",
			ErrIllegalArguments,
			len(table.PrimaryIndex().Cols()),
			len(req.SqlGetRequest.PkValues),
		)
	}

	for i, pkCol := range table.PrimaryIndex().Cols() {
		pkEncVal, _, err := sql.EncodeRawValueAsKey(schema.RawValue(req.SqlGetRequest.PkValues[i]), pkCol.Type(), pkCol.MaxLen())
		if err != nil {
			return nil, err
		}

		_, err = valbuf.Write(pkEncVal)
		if err != nil {
			return nil, err
		}
	}

	// build the encoded key for the pk
	pkKey := sql.MapKey(
		[]byte{SQLPrefix},
		sql.PIndexPrefix,
		sql.EncodeID(1),
		sql.EncodeID(table.ID()),
		sql.EncodeID(sql.PKIndexID),
		valbuf.Bytes())

	e, err := d.sqlGetAt(pkKey, req.SqlGetRequest.AtTx, d.st, true)
	if err != nil {
		return nil, err
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	// key-value inclusion proof
	err = d.st.ReadTx(e.Tx, false, tx)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := tx.Proof(e.Key)
	if err != nil {
		return nil, err
	}

	var rootTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		rootTxHdr = tx.Header()
	} else {
		rootTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false, false)
		if err != nil {
			return nil, err
		}
	}

	var sourceTxHdr, targetTxHdr *store.TxHeader

	if req.ProveSinceTx <= e.Tx {
		sourceTxHdr = rootTxHdr
		targetTxHdr = tx.Header()
	} else {
		sourceTxHdr = tx.Header()
		targetTxHdr = rootTxHdr
	}

	dualProof, err := d.st.DualProof(sourceTxHdr, targetTxHdr)
	if err != nil {
		return nil, err
	}

	verifiableTx := &schema.VerifiableTx{
		Tx:        schema.TxToProto(tx),
		DualProof: schema.DualProofToProto(dualProof),
	}

	colNamesByID := make(map[uint32]string, len(table.Cols()))
	colIdsByName := make(map[string]uint32, len(table.ColsByName()))
	colTypesByID := make(map[uint32]string, len(table.Cols()))
	colLenByID := make(map[uint32]int32, len(table.Cols()))

	for _, col := range table.Cols() {
		colNamesByID[col.ID()] = col.Name()
		colIdsByName[sql.EncodeSelector("", table.Name(), col.Name())] = col.ID()
		colTypesByID[col.ID()] = col.Type()
		colLenByID[col.ID()] = int32(col.MaxLen())
	}

	pkIDs := make([]uint32, len(table.PrimaryIndex().Cols()))

	for i, col := range table.PrimaryIndex().Cols() {
		pkIDs[i] = col.ID()
	}

	return &schema.VerifiableSQLEntry{
		SqlEntry:       e,
		VerifiableTx:   verifiableTx,
		InclusionProof: schema.InclusionProofToProto(inclusionProof),
		DatabaseId:     1,
		TableId:        table.ID(),
		PKIDs:          pkIDs,
		ColNamesById:   colNamesByID,
		ColIdsByName:   colIdsByName,
		ColTypesById:   colTypesByID,
		ColLenById:     colLenByID,
	}, nil
}

func (d *db) sqlGetAt(key []byte, atTx uint64, index store.KeyIndex, skipIntegrityCheck bool) (entry *schema.SQLEntry, err error) {
	var txID uint64
	var md *store.KVMetadata
	var val []byte

	if atTx == 0 {
		valRef, err := index.Get(key)
		if err != nil {
			return nil, err
		}

		txID = valRef.Tx()

		md = valRef.KVMetadata()

		val, err = valRef.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		txID = atTx

		md, val, err = d.readMetadataAndValue(key, atTx, skipIntegrityCheck)
		if err != nil {
			return nil, err
		}
	}

	return &schema.SQLEntry{
		Tx:       txID,
		Key:      key,
		Metadata: schema.KVMetadataToProto(md),
		Value:    val,
	}, err
}

func (d *db) ListTables(ctx context.Context, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	catalog, err := d.sqlEngine.Catalog(ctx, tx)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{{Name: "TABLE", Type: sql.VarcharType}}}

	for _, t := range catalog.GetTables() {
		res.Rows = append(res.Rows, &schema.Row{Values: []*schema.SQLValue{{Value: &schema.SQLValue_S{S: t.Name()}}}})
	}

	return res, nil
}

func (d *db) DescribeTable(ctx context.Context, tx *sql.SQLTx, tableName string) (*schema.SQLQueryResult, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	catalog, err := d.sqlEngine.Catalog(ctx, tx)
	if err != nil {
		return nil, err
	}

	table, err := catalog.GetTableByName(tableName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{
		{Name: "COLUMN", Type: sql.VarcharType},
		{Name: "TYPE", Type: sql.VarcharType},
		{Name: "NULLABLE", Type: sql.BooleanType},
		{Name: "INDEX", Type: sql.VarcharType},
		{Name: "AUTO_INCREMENT", Type: sql.BooleanType},
		{Name: "UNIQUE", Type: sql.BooleanType},
	}}

	for _, c := range table.Cols() {
		index := "NO"

		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}
		if indexed {
			index = "YES"
		}

		if table.PrimaryIndex().IncludesCol(c.ID()) {
			index = "PRIMARY KEY"
		}

		var unique bool
		for _, index := range table.IndexesByColID(c.ID()) {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		var maxLen string

		if c.MaxLen() > 0 && (c.Type() == sql.VarcharType || c.Type() == sql.BLOBType) {
			maxLen = fmt.Sprintf("[%d]", c.MaxLen())
		}

		res.Rows = append(res.Rows, &schema.Row{
			Values: []*schema.SQLValue{
				{Value: &schema.SQLValue_S{S: c.Name()}},
				{Value: &schema.SQLValue_S{S: c.Type() + maxLen}},
				{Value: &schema.SQLValue_B{B: c.IsNullable()}},
				{Value: &schema.SQLValue_S{S: index}},
				{Value: &schema.SQLValue_B{B: c.IsAutoIncremental()}},
				{Value: &schema.SQLValue_B{B: unique}},
			},
		})
	}

	return res, nil
}

func (d *db) NewSQLTx(ctx context.Context, opts *sql.TxOptions) (tx *sql.SQLTx, err error) {
	txCtx, txCancel := context.WithCancel(context.Background())

	txChan := make(chan *sql.SQLTx)
	errChan := make(chan error)

	defer func() {
		if err != nil {
			txCancel()

			if tx != nil {
				tx.Cancel()
			}
		}
	}()

	go func() {
		tx, err = d.sqlEngine.NewTx(txCtx, opts)
		if err != nil {
			errChan <- err
		} else {
			txChan <- tx
		}
	}()

	select {
	case <-ctx.Done():
		{
			return nil, ctx.Err()
		}
	case tx = <-txChan:
		{
			return tx, nil
		}
	case err = <-errChan:
		{
			return nil, err
		}
	}
}

func (d *db) SQLExec(ctx context.Context, tx *sql.SQLTx, req *schema.SQLExecRequest) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	if req == nil {
		return nil, nil, ErrIllegalArguments
	}

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, nil, err
	}

	params := make(map[string]interface{})

	for _, p := range req.Params {
		params[p.Name] = schema.RawValue(p.Value)
	}

	return d.SQLExecPrepared(ctx, tx, stmts, params)
}

func (d *db) SQLExecPrepared(ctx context.Context, tx *sql.SQLTx, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	if len(stmts) == 0 {
		return nil, nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, nil, ErrIsReplica
	}

	return d.sqlEngine.ExecPreparedStmts(ctx, tx, stmts, params)
}

func (d *db) SQLQuery(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, err
	}

	stmt, ok := stmts[0].(sql.DataSource)
	if !ok {
		return nil, sql.ErrExpectingDQLStmt
	}

	return d.SQLQueryPrepared(ctx, tx, stmt, req.Params)
}

func (d *db) SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, namedParams []*schema.NamedParam) (*schema.SQLQueryResult, error) {
	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	r, err := d.SQLQueryRowReader(ctx, tx, stmt, params)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	colDescriptors, err := r.Columns(ctx)
	if err != nil {
		return nil, err
	}

	cols := make([]*schema.Column, len(colDescriptors))

	for i, c := range colDescriptors {
		des := &sql.ColDescriptor{
			AggFn:  c.AggFn,
			Table:  c.Table,
			Column: c.Column,
			Type:   c.Type,
		}
		cols[i] = &schema.Column{Name: des.Selector(), Type: des.Type}
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 1; ; l++ {
		row, err := r.Read(ctx)
		if err == sql.ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		rrow := &schema.Row{
			Columns: make([]string, len(res.Columns)),
			Values:  make([]*schema.SQLValue, len(res.Columns)),
		}

		for i := range colDescriptors {
			rrow.Columns[i] = cols[i].Name

			v := row.ValuesByPosition[i]

			_, isNull := v.(*sql.NullValue)
			if isNull {
				rrow.Values[i] = &schema.SQLValue{Value: &schema.SQLValue_Null{}}
			} else {
				rrow.Values[i] = typedValueToRowValue(v)
			}
		}

		res.Rows = append(res.Rows, rrow)

		if l == d.maxResultSize {
			return res, fmt.Errorf("%w: found at least %d rows (the maximum limit). "+
				"Query constraints can be applied using the LIMIT clause",
				ErrResultSizeLimitReached, d.maxResultSize)
		}
	}

	return res, nil
}

func (d *db) SQLQueryRowReader(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.sqlEngine.QueryPreparedStmt(ctx, tx, stmt, params)
}

func (d *db) InferParameters(ctx context.Context, tx *sql.SQLTx, sql string) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.sqlEngine.InferParameters(ctx, tx, sql)
}

func (d *db) InferParametersPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.SQLStmt) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.sqlEngine.InferParametersPreparedStmts(ctx, tx, []sql.SQLStmt{stmt})
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
