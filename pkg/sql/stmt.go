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
	StringType                 = "STRING"
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
		ds = append(ds, ds...)
	}
	return
}

type CreateDatabaseStmt struct {
	db string
}

// for writes, always needs to be up the date, doesn't matter the snapshot...
// for reading, a snapshot is created. It will wait until such tx is indexed.
// still writing to the catalog will wait the index to be up to date and locked
// conditional lock on writeLocked
func (stmt *CreateDatabaseStmt) isDDL() bool {
	return true
}

func (stmt *CreateDatabaseStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	db, err := e.catalog.newDatabase(stmt.db)
	if err != nil {
		return nil, nil, err
	}

	kv := &store.KV{
		Key:   e.mapKey(catalogDatabasePrefix, encodeID(db.id)),
		Value: []byte(stmt.db),
	}

	ces = append(ces, kv)

	return
}

type UseDatabaseStmt struct {
	db string
}

func (stmt *UseDatabaseStmt) isDDL() bool {
	return false
}

func (stmt *UseDatabaseStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	exists := e.catalog.ExistDatabase(stmt.db)
	if !exists {
		return nil, nil, ErrDatabaseDoesNotExist
	}

	e.implicitDB = stmt.db

	return
}

type UseSnapshotStmt struct {
	since, upTo string
}

func (stmt *UseSnapshotStmt) isDDL() bool {
	return false
}

func (stmt *UseSnapshotStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
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

	db := e.catalog.dbsByName[e.implicitDB]

	table, err := db.newTable(stmt.table, stmt.colsSpec, stmt.pk)
	if err != nil {
		return nil, nil, err
	}

	for colID, col := range table.colsByID {
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

	table, exists := e.catalog.dbsByName[e.implicitDB].tablesByName[stmt.table]
	if !exists {
		return nil, nil, ErrTableDoesNotExist
	}

	if table.pk.colName == stmt.col {
		return nil, nil, ErrIndexAlreadyExists
	}

	col, exists := table.colsByName[stmt.col]
	if !exists {
		return nil, nil, ErrColumnDoesNotExist
	}

	_, exists = table.indexes[col.id]
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
	Values []Value
}

func (r *RowSpec) Bytes(t *Table, cols []string) ([]byte, error) {
	valbuf := bytes.Buffer{}

	// len(stmt.cols)
	var b [encLenLen]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(cols)))
	_, err := valbuf.Write(b[:])
	if err != nil {
		return nil, err
	}

	for i, val := range r.Values {
		col, _ := t.colsByName[cols[i]]

		// len(colName) + colName
		b := make([]byte, encLenLen+len(col.colName))
		binary.BigEndian.PutUint32(b, uint32(len(col.colName)))
		copy(b[encLenLen:], []byte(col.colName))

		_, err = valbuf.Write(b)
		if err != nil {
			return nil, err
		}

		valb, err := encodeValue(val, col.colType, !asKey)
		if err != nil {
			return nil, err
		}

		_, err = valbuf.Write(valb)
		if err != nil {
			return nil, err
		}
	}

	return valbuf.Bytes(), nil
}

func (stmt *UpsertIntoStmt) isDDL() bool {
	return false
}

func (stmt *UpsertIntoStmt) Validate(table *Table) (map[uint64]int, error) {
	pkIncluded := false
	selByColID := make(map[uint64]int, len(stmt.cols))

	for i, c := range stmt.cols {
		col, exists := table.colsByName[c]
		if !exists {
			return nil, ErrInvalidColumn
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
		pkEncVal, err := encodeValue(pkVal, table.pk.colType, asKey)
		if err != nil {
			return nil, nil, err
		}

		bs, err := row.Bytes(table, stmt.cols)
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
			cVal := row.Values[cs[colID]]
			encVal, err := encodeValue(cVal, table.colsByID[colID].colType, asKey)
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

type Value interface {
	Value() interface{}
	Compare(val Value, params map[string]interface{}) (CmpOperator, error)
	jointColumnTo(col *Column) (*ColSelector, error)
	eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error)
}

type Number struct {
	val uint64
}

func (v *Number) Value() interface{} {
	return v.val
}

func (v *Number) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *Number) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	ov, isNumber := val.(*Number)
	if !isNumber {
		return 0, ErrNotComparableValues
	}

	if v.val == ov.val {
		return EQ, nil
	}

	if v.val > ov.val {
		return GT, nil
	}

	return LT, nil
}

func (v *Number) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

type String struct {
	val string
}

func (v *String) Value() interface{} {
	return v.val
}

func (v *String) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *String) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	ov, isString := val.(*String)
	if !isString {
		return 0, ErrNotComparableValues
	}

	r := bytes.Compare([]byte(v.val), []byte(ov.val))

	if r == 0 {
		return EQ, nil
	}

	if r < 0 {
		return LT, nil
	}

	return GT, nil
}

func (v *String) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

type Bool struct {
	val bool
}

func (v *Bool) Value() interface{} {
	return v.val
}

func (v *Bool) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *Bool) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	ov, isBool := val.(*Bool)
	if !isBool {
		return 0, ErrNotComparableValues
	}

	if v.val == ov.val {
		return EQ, nil
	}

	return NE, nil
}

func (v *Bool) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

type Blob struct {
	val []byte
}

func (v *Blob) Value() interface{} {
	return v.val
}

func (v *Blob) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *Blob) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	ov, isBlob := val.(*Blob)
	if !isBlob {
		return 0, ErrNotComparableValues
	}

	r := bytes.Compare(v.val, ov.val)

	if r == 0 {
		return EQ, nil
	}

	if r < 0 {
		return LT, nil
	}

	return GT, nil
}

func (v *Blob) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

type SysFn struct {
	fn string
}

func (v *SysFn) Value() interface{} {
	return nil
}

func (v *SysFn) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *SysFn) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	return 0, errors.New("not yet supported")
}

func (v *SysFn) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

type Param struct {
	id string
}

func (v *Param) Value() interface{} {
	return nil
}

func (v *Param) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return v, nil
}

func (v *Param) Compare(val Value, params map[string]interface{}) (CmpOperator, error) {
	return 0, errors.New("not yet supported")
}

func (v *Param) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
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
	Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol, alias string) (RowReader, error)
}

type SelectStmt struct {
	distinct  bool
	selectors []Selector
	ds        DataSource
	joins     []*JoinSpec
	where     BoolExp
	groupBy   []*ColSelector
	having    BoolExp
	limit     uint64
	orderBy   []*OrdCol
	as        string
}

func (stmt *SelectStmt) isDDL() bool {
	return false
}

func (stmt *SelectStmt) CompileUsing(e *Engine, params map[string]interface{}) (ces []*store.KV, des []*store.KV, err error) {
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

		col, colExists := table.colsByName[stmt.orderBy[0].sel.col]
		if !colExists {
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

func (stmt *SelectStmt) Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol, alias string) (RowReader, error) {
	// Ordering is only supported at TableRef level
	if ordCol != nil {
		return nil, ErrLimitedOrderBy
	}

	var orderByCol *OrdCol

	if len(stmt.orderBy) > 0 {
		orderByCol = stmt.orderBy[0]
	}

	rowReader, err := stmt.ds.Resolve(e, snap, params, orderByCol, stmt.as)
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
		rowReader, err = e.newConditionalRowReader(snap, rowReader, stmt.where, params)
		if err != nil {
			return nil, err
		}
	}

	if stmt.groupBy != nil {
		// groupedRowReader
		return nil, errors.New("not yet supported")
	}

	if stmt.having != nil {
		if stmt.groupBy == nil {
			return nil, ErrInvalidCondition
		}

		rowReader, err = e.newConditionalRowReader(snap, rowReader, stmt.having, params)
		if err != nil {
			return nil, err
		}
	}

	return e.newProjectedRowReader(snap, rowReader, stmt.selectors)
}

type TableRef struct {
	db    string
	table string
	as    string
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

	table, exists := e.catalog.dbsByName[db].tablesByName[stmt.table]
	if !exists {
		return nil, ErrTableDoesNotExist
	}

	return table, nil
}

func (stmt *TableRef) Resolve(e *Engine, snap *store.Snapshot, params map[string]interface{}, ordCol *OrdCol, alias string) (RowReader, error) {
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

		col, exist := table.colsByName[ordCol.sel.col]
		if !exist {
			return nil, ErrColumnDoesNotExist
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
			if len(initKeyVal) > len(maxKeyVal(col.colType)) {
				return nil, ErrIllegalArguments
			}
			initKeyVal = ordCol.initKeyVal
		}

		if !ordCol.useInitKeyVal && (cmp == LowerThan || cmp == LowerOrEqualTo) {
			initKeyVal = maxKeyVal(col.colType)
		}
	}

	return e.newRawRowReader(snap, table, alias, colName, cmp, initKeyVal)
}

type JoinSpec struct {
	joinType JoinType
	ds       DataSource
	cond     BoolExp
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
	resolve(implicitDB, implicitTable string) string
	alias() string
}

type ColSelector struct {
	db    string
	table string
	col   string
	as    string
}

func (sel *ColSelector) resolve(implicitDB, implicitTable string) string {
	db := implicitDB
	if sel.db != "" {
		db = sel.db
	}

	table := implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return db + "." + table + "." + sel.col
}

func (sel *ColSelector) alias() string {
	return sel.as
}

type AggColSelector struct {
	aggFn AggregateFn
	db    string
	table string
	col   string
	as    string
}

func (sel *AggColSelector) resolve(implicitDB, implicitTable string) string {
	db := implicitDB
	if sel.db != "" {
		db = sel.db
	}

	table := implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return sel.aggFn + "(" + db + "." + table + "." + sel.col + ")"
}

func (sel *AggColSelector) alias() string {
	return sel.as
}

type BoolExp interface {
	jointColumnTo(col *Column) (*ColSelector, error)
	eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error)
}

func (bexp *ColSelector) jointColumnTo(col *Column) (*ColSelector, error) {
	if bexp.db != "" && bexp.db != col.table.db.name {
		return nil, ErrJointColumnNotFound
	}

	if bexp.table != "" && bexp.table != col.table.name {
		return nil, ErrJointColumnNotFound
	}

	if bexp.col != col.colName {
		return nil, ErrJointColumnNotFound
	}

	return bexp, nil
}

func (bexp *ColSelector) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	v, ok := row.Values[bexp.resolve(implicitDB, implicitTable)]
	if !ok {
		return nil, ErrColumnDoesNotExist
	}
	return v, nil
}

type NotBoolExp struct {
	exp BoolExp
}

func (bexp *NotBoolExp) jointColumnTo(col *Column) (*ColSelector, error) {
	return bexp.exp.jointColumnTo(col)
}

func (bexp *NotBoolExp) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	v, err := bexp.exp.eval(row, implicitDB, implicitTable, params)
	if err != nil {
		return nil, err
	}
	return v, nil
}

type LikeBoolExp struct {
	col     *ColSelector
	pattern string
}

func (bexp *LikeBoolExp) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *LikeBoolExp) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return nil, errors.New("not yet supported")
}

type CmpBoolExp struct {
	op          CmpOperator
	left, right BoolExp
}

func (bexp *CmpBoolExp) jointColumnTo(col *Column) (*ColSelector, error) {
	if bexp.op != EQ {
		return nil, ErrJointColumnNotFound
	}

	selLeft, okLeft := bexp.left.(*ColSelector)
	selRight, okRight := bexp.right.(*ColSelector)

	if !okLeft || !okRight {
		return nil, ErrJointColumnNotFound
	}

	_, errLeft := selLeft.jointColumnTo(col)
	_, errRight := selRight.jointColumnTo(col)

	if errLeft != nil && errLeft != ErrJointColumnNotFound {
		return nil, errLeft
	}

	if errRight != nil && errRight != ErrJointColumnNotFound {
		return nil, errRight
	}

	if errLeft == nil && errRight == nil {
		return nil, ErrInvalidJointColumn
	}

	if errLeft == nil {
		return selRight, nil
	}

	return selLeft, nil
}

func (bexp *CmpBoolExp) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	vl, err := bexp.left.eval(row, implicitDB, implicitTable, params)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.eval(row, implicitDB, implicitTable, params)
	if err != nil {
		return nil, err
	}

	r, err := vl.Compare(vr, params)
	if err != nil {
		return nil, err
	}

	return &Bool{val: cmpSatisfies(r, bexp.op)}, nil
}

func cmpSatisfies(cmp1, cmp2 CmpOperator) bool {
	switch cmp1 {
	case EQ:
		{
			return cmp2 == EQ || cmp2 == LE || cmp2 == GE
		}
	case LT:
		{
			return cmp2 == NE || cmp2 == LT || cmp2 == LE
		}
	case GT:
		{
			return cmp2 == NE || cmp2 == GT || cmp2 == GE
		}
	}
	return false
}

type BinBoolExp struct {
	op          LogicOperator
	left, right BoolExp
}

func (bexp *BinBoolExp) jointColumnTo(col *Column) (*ColSelector, error) {
	jcolLeft, errLeft := bexp.left.jointColumnTo(col)
	if errLeft != nil && errLeft != ErrJointColumnNotFound {
		return nil, errLeft
	}

	jcolRight, errRight := bexp.left.jointColumnTo(col)
	if errRight != nil && errRight != ErrJointColumnNotFound {
		return nil, errRight
	}

	if errLeft == ErrJointColumnNotFound && errRight == ErrJointColumnNotFound {
		return nil, ErrJointColumnNotFound
	}

	if errLeft == nil && errRight == nil && jcolLeft != jcolRight {
		return nil, ErrInvalidJointColumn
	}

	if errLeft == nil {
		return jcolLeft, nil
	}

	return jcolRight, nil
}

func (bexp *BinBoolExp) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	vl, err := bexp.left.eval(row, implicitDB, implicitTable, params)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.eval(row, implicitDB, implicitTable, params)
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

func (bexp *ExistsBoolExp) jointColumnTo(col *Column) (*ColSelector, error) {
	return nil, ErrJointColumnNotFound
}

func (bexp *ExistsBoolExp) eval(row *Row, implicitDB, implicitTable string, params map[string]interface{}) (Value, error) {
	return nil, errors.New("not yet supported")
}
