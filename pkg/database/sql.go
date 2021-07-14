/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"errors"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrSQLNotReady = errors.New("SQL catalog not yet replicated")

func (d *db) VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	if req == nil {
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

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	txEntry := d.tx1

	table, err := d.sqlEngine.Catalog().GetTableByName(dbInstanceName, req.SqlGetRequest.Table)
	if err != nil {
		return nil, err
	}

	pkEncVal, err := sql.EncodeRawValue(schema.RawValue(req.SqlGetRequest.PkValue), table.PrimaryKey().Type(), true)
	if err != nil {
		return nil, err
	}

	// build the encoded key for the pk
	pkKey := sql.MapKey([]byte{SQLPrefix}, sql.RowPrefix, sql.EncodeID(table.Database().ID()), sql.EncodeID(table.ID()), sql.EncodeID(table.PrimaryKey().ID()), pkEncVal)

	e, err := d.sqlGetAt(pkKey, req.SqlGetRequest.AtTx, d.st, txEntry)
	if err != nil {
		return nil, err
	}

	// key-value inclusion proof
	err = d.st.ReadTx(e.Tx, txEntry)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := d.tx1.Proof(e.Key)
	if err != nil {
		return nil, err
	}

	var rootTx *store.Tx

	if req.ProveSinceTx == 0 {
		rootTx = txEntry
	} else {
		rootTx = d.tx2

		err = d.st.ReadTx(req.ProveSinceTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if req.ProveSinceTx <= e.Tx {
		sourceTx = rootTx
		targetTx = txEntry
	} else {
		sourceTx = txEntry
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	verifiableTx := &schema.VerifiableTx{
		Tx:        schema.TxTo(txEntry),
		DualProof: schema.DualProofTo(dualProof),
	}

	colNamesById := make(map[uint64]string, len(table.ColsByID()))
	colIdsByName := make(map[string]uint64, len(table.ColsByName()))
	colTypesById := make(map[uint64]string, len(table.ColsByID()))

	for _, col := range table.ColsByID() {
		colNamesById[col.ID()] = col.Name()
		colIdsByName[sql.EncodeSelector("", d.options.dbName, table.Name(), col.Name())] = col.ID()
		colTypesById[col.ID()] = col.Type()
	}

	return &schema.VerifiableSQLEntry{
		SqlEntry:       e,
		VerifiableTx:   verifiableTx,
		InclusionProof: schema.InclusionProofTo(inclusionProof),
		DatabaseId:     table.Database().ID(),
		TableId:        table.ID(),
		PKName:         table.PrimaryKey().Name(),
		ColNamesById:   colNamesById,
		ColIdsByName:   colIdsByName,
		ColTypesById:   colTypesById,
	}, nil
}

func (d *db) sqlGetAt(key []byte, atTx uint64, index store.KeyIndex, tx *store.Tx) (entry *schema.SQLEntry, err error) {
	var ktx uint64
	var val []byte

	if atTx == 0 {
		val, ktx, _, err = index.Get(key)
		if err != nil {
			return nil, err
		}
	} else {
		val, err = d.readValue(key, atTx, tx)
		if err != nil {
			return nil, err
		}
		ktx = atTx
	}

	return &schema.SQLEntry{Key: key, Value: val, Tx: ktx}, err
}

func (d *db) ListTables() (*schema.SQLQueryResult, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	db, err := d.sqlEngine.Catalog().GetDatabaseByName(dbInstanceName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{{Name: "TABLE", Type: sql.VarcharType}}}

	for _, t := range db.GetTables() {
		res.Rows = append(res.Rows, &schema.Row{Values: []*schema.SQLValue{{Value: &schema.SQLValue_S{S: t.Name()}}}})
	}

	return res, nil
}

func (d *db) DescribeTable(tableName string) (*schema.SQLQueryResult, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	table, err := d.sqlEngine.Catalog().GetTableByName(dbInstanceName, tableName)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLQueryResult{Columns: []*schema.Column{
		{Name: "COLUMN", Type: sql.VarcharType},
		{Name: "TYPE", Type: sql.VarcharType},
		{Name: "NULLABLE", Type: sql.BooleanType},
		{Name: "INDEX", Type: sql.VarcharType},
	}}

	for _, c := range table.ColsByID() {
		index := "NO"

		if table.PrimaryKey().Name() == c.Name() {
			index = "PRIMARY KEY"
		}

		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}
		if indexed {
			index = "YES"
		}

		res.Rows = append(res.Rows, &schema.Row{
			Values: []*schema.SQLValue{
				{Value: &schema.SQLValue_S{S: c.Name()}},
				{Value: &schema.SQLValue_S{S: c.Type()}},
				{Value: &schema.SQLValue_B{B: c.IsNullable()}},
				{Value: &schema.SQLValue_S{S: index}},
			},
		})
	}

	return res, nil
}

func (d *db) SQLExec(req *schema.SQLExecRequest) (*schema.SQLExecResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, err
	}

	for _, stmt := range stmts {
		switch stmt.(type) {
		case *sql.UseDatabaseStmt:
			{
				return nil, errors.New("SQL statement not supported. Please use `UseDatabase` operation instead")
			}
		case *sql.CreateDatabaseStmt:
			{
				return nil, errors.New("SQL statement not supported. Please use `CreateDatabase` operation instead")
			}
		}
	}

	return d.SQLExecPrepared(stmts, req.Params, !req.NoWait)
}

func (d *db) SQLExecPrepared(stmts []sql.SQLStmt, namedParams []*schema.NamedParam, waitForIndexing bool) (*schema.SQLExecResult, error) {
	if len(stmts) == 0 {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	ddTxs, dmTxs, err := d.sqlEngine.ExecPreparedStmts(stmts, params, waitForIndexing)
	if err != nil {
		return nil, err
	}

	res := &schema.SQLExecResult{
		Ctxs: make([]*schema.TxMetadata, len(ddTxs)),
		Dtxs: make([]*schema.TxMetadata, len(dmTxs)),
	}

	for i, md := range ddTxs {
		res.Ctxs[i] = schema.TxMetatadaTo(md)
	}

	for i, md := range dmTxs {
		res.Dtxs[i] = schema.TxMetatadaTo(md)
	}

	return res, nil
}

func (d *db) UseSnapshot(req *schema.UseSnapshotRequest) error {
	if req == nil {
		return ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return err
		}
	}

	return d.sqlEngine.UseSnapshot(req.SinceTx, req.AsBeforeTx)
}

func (d *db) SQLQuery(req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	stmts, err := sql.Parse(strings.NewReader(req.Sql))
	if err != nil {
		return nil, err
	}

	stmt, ok := stmts[0].(*sql.SelectStmt)
	if !ok {
		return nil, ErrIllegalArguments
	}

	return d.SQLQueryPrepared(stmt, req.Params, !req.ReuseSnapshot)
}

func (d *db) SQLQueryPrepared(stmt *sql.SelectStmt, namedParams []*schema.NamedParam, renewSnapshot bool) (*schema.SQLQueryResult, error) {
	if d.isReplica() {
		err := d.reloadSQLCatalog()
		if err != nil {
			return nil, err
		}
	}

	r, err := d.SQLQueryRowReader(stmt, renewSnapshot)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	params := make(map[string]interface{})

	for _, p := range namedParams {
		params[p.Name] = schema.RawValue(p.Value)
	}

	r.SetParameters(params)

	colDescriptors, err := r.Columns()
	if err != nil {
		return nil, err
	}

	cols := make([]*schema.Column, len(colDescriptors))

	for i, c := range colDescriptors {
		des := &sql.ColDescriptor{
			AggFn:    c.AggFn,
			Database: d.options.dbName,
			Table:    c.Table,
			Column:   c.Column,
			Type:     c.Type,
		}
		cols[i] = &schema.Column{Name: des.Selector(), Type: des.Type}
	}

	res := &schema.SQLQueryResult{Columns: cols}

	for l := 0; l < MaxKeyScanLimit; l++ {
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

		for i, c := range colDescriptors {
			rrow.Columns[i] = cols[i].Name

			v := row.Values[c.Selector()]

			_, isNull := v.(*sql.NullValue)
			if isNull {
				rrow.Values[i] = &schema.SQLValue{Value: &schema.SQLValue_Null{}}
			} else {
				rrow.Values[i] = typedValueToRowValue(v)
			}
		}

		res.Rows = append(res.Rows, rrow)
	}

	return res, nil
}

func (d *db) SQLQueryRowReader(stmt *sql.SelectStmt, renewSnapshot bool) (sql.RowReader, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	if stmt.Limit() > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	return d.sqlEngine.QueryPreparedStmt(stmt, nil, renewSnapshot)
}

func (d *db) InferParameters(sql string) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	return d.sqlEngine.InferParameters(sql)
}

func (d *db) InferParametersPrepared(stmt sql.SQLStmt) (map[string]sql.SQLValueType, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	err := d.sqlEngine.EnsureCatalogReady()
	if err != nil {
		return nil, err
	}

	return d.sqlEngine.InferParametersPreparedStmt(stmt)
}

func typedValueToRowValue(tv sql.TypedValue) *schema.SQLValue {
	switch tv.Type() {
	case sql.IntegerType:
		{
			return &schema.SQLValue{Value: &schema.SQLValue_N{N: tv.Value().(uint64)}}
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
	}
	return nil
}
