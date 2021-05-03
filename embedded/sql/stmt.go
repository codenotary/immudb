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

package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

const (
	catalogDatabasePrefix = "CATALOG.DATABASE." // (key=CATALOG.DATABASE.{dbID}, value={dbNAME})
	catalogTablePrefix    = "CATALOG.TABLE."    // (key=CATALOG.TABLE.{dbID}{tableID}{pkID}, value={tableNAME})
	catalogColumnPrefix   = "CATALOG.COLUMN."   // (key=CATALOG.COLUMN.{dbID}{tableID}{colID}{colTYPE}, value={colNAME})
	catalogIndexPrefix    = "CATALOG.INDEX."    // (key=CATALOG.INDEX.{dbID}{tableID}{colID}, value={})
	rowPrefix             = "ROW."              // (key=ROW.{dbID}{tableID}{colID}({valLen}{val})?{pkValLen}{pkVal}, value={})
)

type SQLValueType = string

const (
	IntegerType   SQLValueType = "INTEGER"
	BooleanType                = "BOOLEAN"
	VarcharType                = "VARCHAR"
	BLOBType                   = "BLOB"
	TimestampType              = "TIMESTAMP"
)

type AggregateFn = string

const (
	COUNT AggregateFn = "COUNT"
	SUM               = "SUM"
	MAX               = "MAX"
	MIN               = "MIN"
	AVG               = "AVG"
)

type CmpOperator = int

const (
	EQ CmpOperator = iota
	NE
	LT
	LE
	GT
	GE
)

type LogicOperator = int

const (
	AND LogicOperator = iota
	OR
)

type NumOperator = int

const (
	ADDOP NumOperator = iota
	SUBSOP
	DIVOP
	MULTOP
)

type JoinType = int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
)

type SQLStmt interface {
	isDDL() bool
	CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error)
}

type TxStmt struct {
	stmts []SQLStmt
}

func (stmt *TxStmt) isDDL() bool {
	for _, stmt := range stmt.stmts {
		if stmt.isDDL() {
			return true
		}
	}
	return false
}

func (stmt *TxStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	for _, stmt := range stmt.stmts {
		cs, ds, err := stmt.CompileUsing(e, params)
		if err != nil {
			return nil, nil, err
		}

		ces = append(ces, cs...)
		des = append(des, ds...)
	}
	return
}

type CreateDatabaseStmt struct {
	DB string
}

// for writes, always needs to be up the date, doesn't matter the snapshot...
// for reading, a snapshot is created. It will wait until such tx is indexed.
// still writing to the catalog will wait the index to be up to date and locked
// conditional lock on writeLocked
func (stmt *CreateDatabaseStmt) isDDL() bool {
	return true
}

func (stmt *CreateDatabaseStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	db, err := e.catalog.newDatabase(stmt.DB)
	if err != nil {
		return nil, nil, err
	}

	kv := &store.KV{
		Key:   e.mapKey(catalogDatabasePrefix, encodeID(db.id)),
		Value: []byte(stmt.DB),
	}

	ces = append(ces, kv)

	return
}

type UseDatabaseStmt struct {
	DB string
}

func (stmt *UseDatabaseStmt) isDDL() bool {
	return false
}

func (stmt *UseDatabaseStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	exists := e.catalog.ExistDatabase(stmt.DB)
	if !exists {
		return nil, nil, ErrDatabaseDoesNotExist
	}

	e.UseDB(stmt.DB)

	return
}

type UseSnapshotStmt struct {
	sinceTx  uint64
	asBefore uint64
}

func (stmt *UseSnapshotStmt) isDDL() bool {
	return false
}

func (stmt *UseSnapshotStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	if stmt.sinceTx > 0 && stmt.sinceTx < stmt.asBefore {
		return nil, nil, ErrIllegalArguments
	}

	txID, _ := e.dataStore.Alh()
	if txID < stmt.sinceTx || txID < stmt.asBefore {
		return nil, nil, ErrTxDoesNotExist
	}

	err = e.dataStore.WaitForIndexingUpto(e.snapSinceTx)
	if err != nil {
		return nil, nil, err
	}

	e.snapSinceTx = stmt.sinceTx
	e.snapAsBeforeTx = stmt.asBefore

	return nil, nil, nil
}

type CreateTableStmt struct {
	table    string
	colsSpec []*ColSpec
	pk       string
}

func (stmt *CreateTableStmt) isDDL() bool {
	return true
}

func (stmt *CreateTableStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDB == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	db, err := e.catalog.GetDatabaseByName(e.implicitDB)
	if err != nil {
		return nil, nil, err
	}

	table, err := db.newTable(stmt.table, stmt.colsSpec, stmt.pk)
	if err != nil {
		return nil, nil, err
	}

	for colID, col := range table.GetColsByID() {
		ce := &store.KV{
			Key:   e.mapKey(catalogColumnPrefix, encodeID(db.id), encodeID(table.id), encodeID(colID), []byte(col.colType)),
			Value: []byte(col.colName),
		}
		ces = append(ces, ce)
	}

	te := &store.KV{
		Key:   e.mapKey(catalogTablePrefix, encodeID(db.id), encodeID(table.id), encodeID(table.pk.id)),
		Value: []byte(table.name),
	}
	ces = append(ces, te)

	return
}

type ColSpec struct {
	colName string
	colType SQLValueType
}

type CreateIndexStmt struct {
	table string
	col   string
}

func (stmt *CreateIndexStmt) isDDL() bool {
	return true
}

func (stmt *CreateIndexStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDB == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	table, err := e.catalog.GetTableByName(e.implicitDB, stmt.table)
	if err != nil {
		return nil, nil, err
	}

	if table.pk.colName == stmt.col {
		return nil, nil, ErrIndexAlreadyExists
	}

	col, err := table.GetColumnByName(stmt.col)
	if err != nil {
		return nil, nil, err
	}

	_, exists := table.indexes[col.id]
	if exists {
		return nil, nil, ErrIndexAlreadyExists
	}

	table.indexes[col.id] = struct{}{}

	te := &store.KV{
		Key:   e.mapKey(catalogIndexPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(col.id)),
		Value: []byte(table.name),
	}
	ces = append(ces, te)

	return
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func (stmt *AddColumnStmt) isDDL() bool {
	return true
}

func (stmt *AddColumnStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type UpsertIntoStmt struct {
	tableRef *TableRef
	cols     []string
	rows     []*RowSpec
}

type RowSpec struct {
	Values []ValueExp
}

func (r *RowSpec) bytes(catalog *Catalog, t *Table, cols []string, params map[string]interface{}) ([]byte, error) {
	valbuf := bytes.Buffer{}

	colCount := 0

	for i, val := range r.Values {
		col, err := t.GetColumnByName(cols[i])
		if err != nil {
			return nil, err
		}

		sval, err := val.substitute(params)
		if err != nil {
			return nil, err
		}

		rval, err := sval.reduce(catalog, nil, t.db.name, t.name)
		if err != nil {
			return nil, err
		}

		_, isNull := rval.(*NullValue)
		if isNull {
			continue
		}

		b := make([]byte, encIDLen)
		binary.BigEndian.PutUint64(b, uint64(col.id))

		_, err = valbuf.Write(b)
		if err != nil {
			return nil, err
		}

		valb, err := EncodeValue(rval, col.colType, !asKey)
		if err != nil {
			return nil, err
		}

		_, err = valbuf.Write(valb)
		if err != nil {
			return nil, err
		}

		colCount++
	}

	b := make([]byte, encLenLen+len(valbuf.Bytes()))
	binary.BigEndian.PutUint32(b, uint32(colCount))
	copy(b[encLenLen:], valbuf.Bytes())

	return b, nil
}

func (stmt *UpsertIntoStmt) isDDL() bool {
	return false
}

func (stmt *UpsertIntoStmt) Validate(table *Table) (map[uint64]int, error) {
	pkIncluded := false
	selByColID := make(map[uint64]int, len(stmt.cols))

	for i, c := range stmt.cols {
		col, err := table.GetColumnByName(c)
		if err != nil {
			return nil, err
		}

		if table.pk.colName == c {
			pkIncluded = true
		}

		_, duplicated := selByColID[col.id]
		if duplicated {
			return nil, ErrDuplicatedColumn
		}

		selByColID[col.id] = i
	}

	if !pkIncluded {
		return nil, ErrPKCanNotBeNull
	}

	return selByColID, nil
}

func (stmt *UpsertIntoStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	table, err := stmt.tableRef.referencedTable(e)
	if err != nil {
		return nil, nil, err
	}

	cs, err := stmt.Validate(table)
	if err != nil {
		return nil, nil, err
	}

	for _, row := range stmt.rows {
		if len(row.Values) != len(stmt.cols) {
			return nil, nil, ErrInvalidNumberOfValues
		}

		pkVal := row.Values[cs[table.pk.id]]

		val, err := pkVal.substitute(params)
		if err != nil {
			return nil, nil, err
		}

		rval, err := val.reduce(e.catalog, nil, e.implicitDB, table.name)
		if err != nil {
			return nil, nil, err
		}

		_, isNull := rval.(*NullValue)
		if isNull {
			return nil, nil, ErrPKCanNotBeNull
		}

		pkEncVal, err := EncodeValue(rval, table.pk.colType, asKey)
		if err != nil {
			return nil, nil, err
		}

		bs, err := row.bytes(e.catalog, table, stmt.cols, params)
		if err != nil {
			return nil, nil, err
		}

		// create entry for the column which is the pk
		pke := &store.KV{
			Key:   e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(table.pk.id), pkEncVal),
			Value: bs,
		}
		des = append(des, pke)

		// create entries for each indexed column, with value as value for pk column
		for colID := range table.indexes {
			colPos, defined := cs[colID]
			if !defined {
				return nil, nil, ErrIndexedColumnCanNotBeNull
			}

			cVal := row.Values[colPos]

			val, err := cVal.substitute(params)
			if err != nil {
				return nil, nil, err
			}

			rval, err := val.reduce(e.catalog, nil, e.implicitDB, table.name)
			if err != nil {
				return nil, nil, err
			}

			_, isNull := rval.(*NullValue)
			if isNull {
				return nil, nil, ErrIndexedColumnCanNotBeNull
			}

			col, err := table.GetColumnByID(colID)
			if err != nil {
				return nil, nil, err
			}

			encVal, err := EncodeValue(rval, col.colType, asKey)
			if err != nil {
				return nil, nil, err
			}

			ie := &store.KV{
				Key:   e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(colID), encVal, pkEncVal),
				Value: nil,
			}
			des = append(des, ie)
		}
	}

	return
}

type ValueExp interface {
	jointColumnTo(col *Column, tableAlias string) (*ColSelector, error)
	substitute(params map[string]interface{}) (ValueExp, error)
	reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error)
}

type TypedValue interface {
	Type() SQLValueType
	Value() interface{}
	Compare(val TypedValue) (int, error)
}

type NullValue struct {
	t SQLValueType
}

func (n *NullValue) Type() SQLValueType {
	return n.t
}

func (n *NullValue) Value() interface{} {
	return nil
}

func (n *NullValue) Compare(val TypedValue) (int, error) {
	if n.t != "" && val.Type() != "" && n.t != val.Type() {
		return 0, ErrNotComparableValues
	}

	_, isNull := val.(*NullValue)
	if isNull {
		return 0, nil
	}

	return -1, nil
}

func (n *NullValue) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (n *NullValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return n, nil
}

func (n *NullValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return n, nil
}

type Number struct {
	val uint64
}

func (v *Number) Type() SQLValueType {
	return IntegerType
}

func (v *Number) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (v *Number) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Number) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Number) Value() interface{} {
	return v.val
}

func (v *Number) Compare(val TypedValue) (int, error) {
	_, isNull := val.(*NullValue)
	if isNull {
		return 1, nil
	}

	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().(uint64)

	if v.val == rval {
		return 0, nil
	}

	if v.val > rval {
		return 1, nil
	}

	return -1, nil
}

type Varchar struct {
	val string
}

func (v *Varchar) Type() SQLValueType {
	return VarcharType
}

func (v *Varchar) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (v *Varchar) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Varchar) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Varchar) Value() interface{} {
	return v.val
}

func (v *Varchar) Compare(val TypedValue) (int, error) {
	_, isNull := val.(*NullValue)
	if isNull {
		return 1, nil
	}

	if val.Type() != VarcharType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().(string)

	return bytes.Compare([]byte(v.val), []byte(rval)), nil
}

type Bool struct {
	val bool
}

func (v *Bool) Type() SQLValueType {
	return BooleanType
}

func (v *Bool) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (v *Bool) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Bool) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Bool) Value() interface{} {
	return v.val
}

func (v *Bool) Compare(val TypedValue) (int, error) {
	_, isNull := val.(*NullValue)
	if isNull {
		return 1, nil
	}

	if val.Type() != BooleanType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().(bool)

	if v.val == rval {
		return 0, nil
	}

	if v.val {
		return 1, nil
	}

	return -1, nil
}

type Blob struct {
	val []byte
}

func (v *Blob) Type() SQLValueType {
	return BLOBType
}

func (v *Blob) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (v *Blob) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Blob) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Blob) Value() interface{} {
	return v.val
}

func (v *Blob) Compare(val TypedValue) (int, error) {
	_, isNull := val.(*NullValue)
	if isNull {
		return 1, nil
	}

	if val.Type() != BLOBType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().([]byte)

	return bytes.Compare(v.val, rval), nil
}

type SysFn struct {
	fn string
}

func (v *SysFn) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (v *SysFn) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *SysFn) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if strings.ToUpper(v.fn) == "NOW" {
		return &Number{val: uint64(time.Now().UnixNano())}, nil
	}

	return nil, errors.New("not yet supported")
}

type Param struct {
	id string
}

func (p *Param) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (p *Param) substitute(params map[string]interface{}) (ValueExp, error) {
	val, ok := params[p.id]
	if !ok {
		return nil, ErrIllegalArguments
	}

	if val == nil {
		return &NullValue{}, nil
	}

	switch v := val.(type) {
	case bool:
		{
			return &Bool{val: v}, nil
		}
	case string:
		{
			return &Varchar{val: v}, nil
		}
	case int:
		{
			return &Number{val: uint64(v)}, nil
		}
	case uint64:
		{
			return &Number{val: v}, nil
		}
	case []byte:
		{
			return &Blob{val: v}, nil
		}
	}

	return nil, ErrIllegalArguments
}

func (p *Param) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

type Comparison int

const (
	EqualTo Comparison = iota
	LowerThan
	LowerOrEqualTo
	GreaterThan
	GreaterOrEqualTo
)

type DataSource interface {
	Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol) (RowReader, error)
	Alias() string
}

type SelectStmt struct {
	distinct  bool
	selectors []Selector
	ds        DataSource
	joins     []*JoinSpec
	where     ValueExp
	groupBy   []*ColSelector
	having    ValueExp
	limit     uint64
	orderBy   []*OrdCol
	as        string
}

func (stmt *SelectStmt) isDDL() bool {
	return false
}

func (stmt *SelectStmt) Limit() uint64 {
	return stmt.limit
}

func (stmt *SelectStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	if stmt.distinct {
		return nil, nil, ErrNoSupported
	}

	if stmt.groupBy == nil && stmt.having != nil {
		return nil, nil, ErrInvalidCondition
	}

	if len(stmt.orderBy) > 1 {
		return nil, nil, ErrLimitedOrderBy
	}

	if len(stmt.selectors) == 0 {
		return nil, nil, ErrIllegalArguments
	}

	if len(stmt.orderBy) > 0 {
		tableRef, ok := stmt.ds.(*TableRef)
		if !ok {
			return nil, nil, ErrLimitedOrderBy
		}

		table, err := tableRef.referencedTable(e)
		if err != nil {
			return nil, nil, err
		}

		col, err := table.GetColumnByName(stmt.orderBy[0].sel.col)
		if err != nil {
			return nil, nil, ErrLimitedOrderBy
		}

		if table.pk.id == col.id {
			return nil, nil, nil
		}

		_, indexed := table.indexes[col.id]
		if !indexed {
			return nil, nil, ErrLimitedOrderBy
		}
	}

	return nil, nil, nil
}

func (stmt *SelectStmt) Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol) (RowReader, error) {
	// Ordering is only supported at TableRef level
	if ordCol != nil {
		return nil, ErrLimitedOrderBy
	}

	var orderByCol *OrdCol

	if len(stmt.orderBy) > 0 {
		orderByCol = stmt.orderBy[0]
	}

	rowReader, err := stmt.ds.Resolve(e, snap, params, orderByCol)
	if err != nil {
		return nil, err
	}

	if stmt.joins != nil {
		rowReader, err = e.newJointRowReader(snap, params, rowReader, stmt.joins)
		if err != nil {
			return nil, err
		}
	}

	if stmt.where != nil {
		rowReader, err = e.newConditionalRowReader(rowReader, stmt.where, params)
		if err != nil {
			return nil, err
		}
	}

	containsAggregations := false
	for _, sel := range stmt.selectors {
		_, containsAggregations = sel.(*AggColSelector)
		if containsAggregations {
			break
		}
	}

	if containsAggregations {
		var groupBy []*ColSelector
		if stmt.groupBy != nil {
			groupBy = stmt.groupBy
		}

		rowReader, err = e.newGroupedRowReader(rowReader, stmt.selectors, groupBy)
		if err != nil {
			return nil, err
		}

		if stmt.having != nil {
			rowReader, err = e.newConditionalRowReader(rowReader, stmt.having, params)
			if err != nil {
				return nil, err
			}
		}
	}

	return e.newProjectedRowReader(rowReader, stmt.as, stmt.selectors, stmt.limit)
}

func (stmt *SelectStmt) Alias() string {
	if stmt.as == "" {
		return stmt.ds.Alias()
	}

	return stmt.as
}

type TableRef struct {
	db       string
	table    string
	asBefore uint64
	as       string
}

func (stmt *TableRef) referencedTable(e *Engine) (*Table, error) {
	if e == nil {
		return nil, ErrIllegalArguments
	}

	var db string

	if db != "" {
		exists := e.catalog.ExistDatabase(stmt.db)
		if !exists {
			return nil, ErrDatabaseDoesNotExist
		}

		db = stmt.db
	}

	if db == "" {
		if e.implicitDB == "" {
			return nil, ErrNoDatabaseSelected
		}

		db = e.implicitDB
	}

	table, err := e.catalog.GetTableByName(db, stmt.table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (stmt *TableRef) Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol) (RowReader, error) {
	if e == nil || snap == nil || (ordCol != nil && ordCol.sel == nil) {
		return nil, ErrIllegalArguments
	}

	table, err := stmt.referencedTable(e)
	if err != nil {
		return nil, err
	}

	colName := table.pk.colName
	cmp := GreaterOrEqualTo
	var initKeyVal []byte

	if ordCol != nil {
		if ordCol.sel.db != "" && ordCol.sel.db != table.db.name {
			return nil, ErrInvalidColumn
		}

		if ordCol.sel.table != "" && ordCol.sel.table != table.name {
			return nil, ErrInvalidColumn
		}

		col, err := table.GetColumnByName(ordCol.sel.col)
		if err != nil {
			return nil, err
		}

		// if it's not PK then it must be an indexed column
		if table.pk.colName != ordCol.sel.col {
			_, indexed := table.indexes[col.id]
			if !indexed {
				return nil, ErrColumnNotIndexed
			}
		}

		colName = col.colName
		cmp = ordCol.cmp

		if ordCol.useInitKeyVal {
			if len(ordCol.initKeyVal) > encLenLen+len(maxKeyVal(col.colType)) {
				return nil, ErrMaxKeyLengthExceeded
			}
			initKeyVal = ordCol.initKeyVal
		}

		if !ordCol.useInitKeyVal && (cmp == LowerThan || cmp == LowerOrEqualTo) {
			initKeyVal = maxKeyVal(col.colType)
		}
	}

	asBefore := stmt.asBefore
	if asBefore == 0 {
		asBefore = e.snapAsBeforeTx
	}

	return e.newRawRowReader(snap, table, asBefore, stmt.as, colName, cmp, initKeyVal)
}

func (stmt *TableRef) Alias() string {
	if stmt.as == "" {
		return stmt.table
	}
	return stmt.as
}

type JoinSpec struct {
	joinType JoinType
	ds       DataSource
	cond     ValueExp
}

type GroupBySpec struct {
	cols []string
}

type OrdCol struct {
	sel           *ColSelector
	cmp           Comparison
	initKeyVal    []byte
	useInitKeyVal bool
}

type Selector interface {
	ValueExp
	resolve(implicitDB, implicitTable string) (aggFn, db, table, col string)
	alias() string
	setAlias(alias string)
}

type ColSelector struct {
	db    string
	table string
	col   string
	as    string
}

func (sel *ColSelector) resolve(implicitDB, implicitTable string) (aggFn, db, table, col string) {
	db = implicitDB
	if sel.db != "" {
		db = sel.db
	}

	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return "", db, table, sel.col
}

func (sel *ColSelector) alias() string {
	return sel.as
}

func (sel *ColSelector) setAlias(alias string) {
	sel.as = alias
}

func (sel *ColSelector) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	if sel.db != "" && sel.db != col.table.db.name {
		return nil, ErrJointColumnNotFound
	}

	if sel.table != tableAlias {
		return nil, ErrJointColumnNotFound
	}

	if sel.col != col.colName {
		return nil, ErrJointColumnNotFound
	}

	return sel, nil
}

func (sel *ColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *ColSelector) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	aggFn, db, table, col := sel.resolve(implicitDB, implicitTable)

	v, ok := row.Values[EncodeSelector(aggFn, db, table, col)]
	if !ok {
		return nil, ErrColumnDoesNotExist
	}

	return v, nil
}

type AggColSelector struct {
	aggFn AggregateFn
	db    string
	table string
	col   string
	as    string
}

func EncodeSelector(aggFn, db, table, col string) string {
	return aggFn + "(" + db + "." + table + "." + col + ")"
}

func (sel *AggColSelector) resolve(implicitDB, implicitTable string) (aggFn, db, table, col string) {
	db = implicitDB
	if sel.db != "" {
		db = sel.db
	}

	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return sel.aggFn, db, table, sel.col
}

func (sel *AggColSelector) alias() string {
	return sel.as
}

func (sel *AggColSelector) setAlias(alias string) {
	sel.as = alias
}

func (sel *AggColSelector) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (sel *AggColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *AggColSelector) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	v, ok := row.Values[EncodeSelector(sel.resolve(implicitDB, implicitTable))]
	if !ok {
		return nil, ErrColumnDoesNotExist
	}
	return v, nil
}

type NumExp struct {
	op          NumOperator
	left, right ValueExp
}

func (bexp *NumExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *NumExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *NumExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	nl, isNumber := vl.Value().(uint64)
	if !isNumber {
		return nil, ErrInvalidCondition
	}

	nr, isNumber := vr.Value().(uint64)
	if !isNumber {
		return nil, ErrInvalidCondition
	}

	switch bexp.op {
	case ADDOP:
		{
			return &Number{val: nl + nr}, nil
		}
	case SUBSOP:
		{
			return &Number{val: nl - nr}, nil
		}
	case DIVOP:
		{
			if nr == 0 {
				return nil, ErrDivisionByZero
			}

			return &Number{val: nl / nr}, nil
		}
	case MULTOP:
		{
			return &Number{val: nl * nr}, nil
		}
	}

	return nil, ErrUnexpected
}

type NotBoolExp struct {
	exp ValueExp
}

func (bexp *NotBoolExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return bexp.exp.jointColumnTo(col, tableAlias)
}

func (bexp *NotBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rexp, err := bexp.exp.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.exp = rexp

	return bexp, nil
}

func (bexp *NotBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	v, err := bexp.exp.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	r, isBool := v.Value().(bool)
	if !isBool {
		return nil, ErrInvalidCondition
	}

	v.(*Bool).val = !r

	return v, nil
}

type LikeBoolExp struct {
	sel     Selector
	pattern string
}

func (bexp *LikeBoolExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *LikeBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *LikeBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	v, ok := row.Values[EncodeSelector(bexp.sel.resolve(implicitDB, implicitTable))]
	if !ok {
		return nil, ErrColumnDoesNotExist
	}

	if v.Type() != VarcharType {
		return nil, ErrInvalidColumn
	}

	matched, err := regexp.MatchString(bexp.pattern, v.Value().(string))
	if err != nil {
		return nil, err
	}

	return &Bool{val: matched}, nil
}

type CmpBoolExp struct {
	op          CmpOperator
	left, right ValueExp
}

func (bexp *CmpBoolExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	if bexp.op != EQ {
		return nil, ErrJointColumnNotFound
	}

	selLeft, okLeft := bexp.left.(*ColSelector)
	selRight, okRight := bexp.right.(*ColSelector)

	if !okLeft || !okRight {
		return nil, ErrJointColumnNotFound
	}

	_, lErr := selLeft.jointColumnTo(col, tableAlias)
	_, rErr := selRight.jointColumnTo(col, tableAlias)

	if lErr == nil && rErr == nil {
		return nil, ErrInvalidJointColumn
	}

	if lErr == nil && rErr == ErrJointColumnNotFound {
		return selRight, nil
	}

	if rErr == nil && lErr == ErrJointColumnNotFound {
		return selLeft, nil
	}

	if lErr != nil {
		return nil, lErr
	}

	return nil, rErr
}

func (bexp *CmpBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *CmpBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	r, err := vl.Compare(vr)
	if err != nil {
		return nil, err
	}

	return &Bool{val: cmpSatisfiesOp(r, bexp.op)}, nil
}

func cmpSatisfiesOp(cmp int, op CmpOperator) bool {
	switch cmp {
	case 0:
		{
			return op == EQ || op == LE || op == GE
		}
	case -1:
		{
			return op == NE || op == LT || op == LE
		}
	case 1:
		{
			return op == NE || op == GT || op == GE
		}
	}
	return false
}

type BinBoolExp struct {
	op          LogicOperator
	left, right ValueExp
}

func (bexp *BinBoolExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *BinBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rlexp, err := bexp.left.substitute(params)
	if err != nil {
		return nil, err
	}

	rrexp, err := bexp.right.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.left = rlexp
	bexp.right = rrexp

	return bexp, nil
}

func (bexp *BinBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	bl, isBool := vl.(*Bool)
	if !isBool {
		return nil, ErrInvalidValue
	}

	br, isBool := vr.(*Bool)
	if !isBool {
		return nil, ErrInvalidValue
	}

	switch bexp.op {
	case AND:
		{
			return &Bool{val: bl.val && br.val}, nil
		}
	case OR:
		{
			return &Bool{val: bl.val || br.val}, nil
		}
	}

	return nil, ErrUnexpected
}

type ExistsBoolExp struct {
	q *SelectStmt
}

func (bexp *ExistsBoolExp) jointColumnTo(col *Column, tableAlias string) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *ExistsBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *ExistsBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, errors.New("not yet supported")
}
