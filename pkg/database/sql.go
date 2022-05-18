/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrSQLNotReady = errors.New("SQL catalog not yet replicated")

func (d *db) reloadSQLCatalog() error {
	err := d.sqlEngine.SetCurrentDatabase(dbInstanceName)
	if err == sql.ErrDatabaseDoesNotExist {
		return ErrSQLNotReady
	}

	return err
}

func (d *db) VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	if req == nil || req.SqlGetRequest == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	catalog, err := d.sqlEngine.Catalog(nil)
	if err != nil {
		return nil, err
	}

	table, err := catalog.GetTableByName(dbInstanceName, req.SqlGetRequest.Table)
	if err != nil {
		return nil, err
	}

	valbuf := bytes.Buffer{}

	for i, pkCol := range table.PrimaryIndex().Cols() {
		pkEncVal, err := sql.EncodeAsKey(schema.RawValue(req.SqlGetRequest.PkValues[i]), pkCol.Type(), pkCol.MaxLen())
		if err != nil {
			return nil, err
		}

		_, err = valbuf.Write(pkEncVal)
		if err != nil {
			return nil, err
		}
	}

	tx := d.st.NewTxHolder()

	// build the encoded key for the pk
	pkKey := sql.MapKey(
		[]byte{SQLPrefix},
		sql.PIndexPrefix,
		sql.EncodeID(table.Database().ID()),
		sql.EncodeID(table.ID()),
		sql.EncodeID(sql.PKIndexID),
		valbuf.Bytes())

	e, err := d.sqlGetAt(pkKey, req.SqlGetRequest.AtTx, d.st, tx)
	if err != nil {
		return nil, err
	}

	// key-value inclusion proof
	err = d.st.ReadTx(e.Tx, tx)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := tx.Proof(e.Key)
	if err != nil {
		return nil, err
	}

	var rootTx *store.Tx

	if req.ProveSinceTx == 0 {
		rootTx = tx
	} else {
		rootTx = d.st.NewTxHolder()

		err = d.st.ReadTx(req.ProveSinceTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if req.ProveSinceTx <= e.Tx {
		sourceTx = rootTx
		targetTx = tx
	} else {
		sourceTx = tx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
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
		colIdsByName[sql.EncodeSelector("", d.GetName(), table.Name(), col.Name())] = col.ID()
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
		DatabaseId:     table.Database().ID(),
		TableId:        table.ID(),
		PKIDs:          pkIDs,
		ColNamesById:   colNamesByID,
		ColIdsByName:   colIdsByName,
		ColTypesById:   colTypesByID,
		ColLenById:     colLenByID,
	}, nil
}

func (d *db) sqlGetAt(key []byte, atTx uint64, index store.KeyIndex, tx *store.Tx) (entry *schema.SQLEntry, err error) {
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

		md, val, err = d.readMetadataAndValue(key, atTx, tx)
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

func (d *db) ListTables(tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	catalog, err := d.sqlEngine.Catalog(tx)
	if err != nil {
		return nil, err
	}

	db, err := catalog.GetDatabaseByName(dbInstanceName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{{Name: "TABLE", Type: sql.VarcharType}}}

	for _, t := range db.GetTables() {
		res.Rows = append(res.Rows, &schema.Row{Values: []*schema.SQLValue{{Value: &schema.SQLValue_S{S: t.Name()}}}})
	}

	return res, nil
}

func (d *db) DescribeTable(tableName string, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	catalog, err := d.sqlEngine.Catalog(tx)
	if err != nil {
		return nil, err
	}

	table, err := catalog.GetTableByName(dbInstanceName, tableName)
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

func (d *db) NewSQLTx(ctx context.Context) (*sql.SQLTx, error) {
	return d.sqlEngine.NewTx(ctx)
}

func (d *db) SQLExec(req *schema.SQLExecRequest, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
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

	return d.SQLExecPrepared(stmts, params, tx)
}

func (d *db) SQLExecPrepared(stmts []sql.SQLStmt, params map[string]interface{}, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	if len(stmts) == 0 {
		return nil, nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, nil, ErrIsReplica
	}

	return d.sqlEngine.ExecPreparedStmts(stmts, params, tx)
}

func (d *db) SQLQuery(req *schema.SQLQueryRequest, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
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

	return d.SQLQueryPrepared(stmt, req.Params, tx)
}

func (d *db) SQLQueryPrepared(stmt sql.DataSource, namedParams []*schema.NamedParam, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	r, err := d.SQLQueryRowReader(stmt, params, tx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	colDescriptors, err := r.Columns()
	if err != nil {
		return nil, err
	}

	cols := make([]*schema.Column, len(colDescriptors))

	for i, c := range colDescriptors {
		dbname := c.Database
		if c.Database == dbInstanceName {
			dbname = d.name
		}

		des := &sql.ColDescriptor{
			AggFn:    c.AggFn,
			Database: dbname,
			Table:    c.Table,
			Column:   c.Column,
			Type:     c.Type,
		}
		cols[i] = &schema.Column{Name: des.Selector(), Type: des.Type}
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 1; ; l++ {
		row, err := r.Read()
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

func (d *db) SQLQueryRowReader(stmt sql.DataSource, params map[string]interface{}, tx *sql.SQLTx) (sql.RowReader, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	return d.sqlEngine.QueryPreparedStmt(stmt, params, tx)
}

func (d *db) InferParameters(sql string, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	return d.sqlEngine.InferParameters(sql, tx)
}

func (d *db) InferParametersPrepared(stmt sql.SQLStmt, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	return d.sqlEngine.InferParametersPreparedStmts([]sql.SQLStmt{stmt}, tx)
}

func typedValueToRowValue(tv sql.TypedValue) *schema.SQLValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: tv.Value().(int64)}}
		}
	case sql.VarcharType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_S{S: tv.Value().(string)}}
		}
	case sql.BooleanType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_B{B: tv.Value().(bool)}}
		}
	case sql.BLOBType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Bs{Bs: tv.Value().([]byte)}}
		}
	case sql.TimestampType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_Ts{Ts: sql.TimeToInt64(tv.Value().(time.Time))}}
		}
	}
	return nil
}
