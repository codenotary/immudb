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
	"github.com/codenotary/immudb/embedded/tbtree"
)

const (
	catalogDatabasePrefix = "CATALOG.DATABASE." // (key=CATALOG.DATABASE.{dbID}, value={dbNAME})
	catalogTablePrefix    = "CATALOG.TABLE."    // (key=CATALOG.TABLE.{dbID}{tableID}{pkID}, value={tableNAME})
	catalogColumnPrefix   = "CATALOG.COLUMN."   // (key=CATALOG.COLUMN.{dbID}{tableID}{colID}{colTYPE}, value={colNAME})
	catalogIndexPrefix    = "CATALOG.INDEX."    // (key=CATALOG.INDEX.{dbID}{tableID}{colID}, value={})
	rowPrefix             = "ROW."              // (key=ROW.{dbID}{tableID}{colID}({valLen}{val})?{pkVal}, value={})
)

type SQLValueType = string

const (
	IntegerType   SQLValueType = "INTEGER"
	BooleanType                = "BOOLEAN"
	StringType                 = "STRING"
	BLOBType                   = "BLOB"
	TimestampType              = "TIMESTAMP"
)

type AggregateFn = int

const (
	COUNT AggregateFn = iota
	SUM
	MAX
	MIN
	AVG
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
	CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error)
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

func (stmt *TxStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	for _, stmt := range stmt.stmts {
		cs, ds, err := stmt.CompileUsing(e)
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

func (stmt *CreateDatabaseStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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

func (stmt *UseDatabaseStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	exists := e.catalog.ExistDatabase(stmt.db)
	if !exists {
		return nil, nil, ErrDatabaseDoesNotExist
	}

	e.implicitDatabase = stmt.db

	return
}

type UseSnapshotStmt struct {
	since, upTo string
}

func (stmt *UseSnapshotStmt) isDDL() bool {
	return false
}

func (stmt *UseSnapshotStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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

func (stmt *CreateTableStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDatabase == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	db := e.catalog.dbsByName[e.implicitDatabase]

	table, err := db.newTable(stmt.table, stmt.colsSpec, stmt.pk)
	if err != nil {
		return nil, nil, err
	}

	for colID, col := range table.colsByID {
		ce := &store.KV{
			Key:   e.mapKey(catalogColumnPrefix, encodeID(db.id), encodeID(table.id), encodeID(colID), []byte(col.colType)),
			Value: nil,
		}
		ces = append(ces, ce)
	}

	te := &store.KV{
		Key:   e.mapKey(catalogTablePrefix, encodeID(db.id), encodeID(db.id), encodeID(table.pk.id)),
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

func (stmt *CreateIndexStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	// index cannot be created for pk nor for certain types such as blob

	return nil, nil, errors.New("not yet supported")
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func (stmt *AddColumnStmt) isDDL() bool {
	return true
}

func (stmt *AddColumnStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type UpsertIntoStmt struct {
	tableRef *TableRef
	cols     []string
	rows     []*RowSpec
}

type RowSpec struct {
	Values []interface{}
}

func (r *RowSpec) Bytes(t *Table, cols []string) ([]byte, error) {
	valbuf := bytes.Buffer{}

	// len(stmt.cols)
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(cols)))
	_, err := valbuf.Write(b[:])
	if err != nil {
		return nil, err
	}

	for i, val := range r.Values {
		col, _ := t.colsByName[cols[i]]

		// len(colName) + colName
		b := make([]byte, 4+len(col.colName))
		binary.BigEndian.PutUint32(b, uint32(len(col.colName)))
		copy(b[4:], []byte(col.colName))

		_, err = valbuf.Write(b)
		if err != nil {
			return nil, err
		}

		switch col.colType {
		case StringType:
			{
				v, ok := val.(string)
				if !ok {
					return nil, ErrInvalidValue
				}

				// len(v) + v
				b := make([]byte, 4+len(v))
				binary.BigEndian.PutUint32(b, uint32(len(v)))
				copy(b[4:], []byte(v))

				_, err = valbuf.Write(b)
				if err != nil {
					return nil, err
				}
			}
		case IntegerType:
			{
				v, ok := val.(uint64)
				if !ok {
					return nil, ErrInvalidValue
				}

				b := make([]byte, 8)
				binary.BigEndian.PutUint64(b, v)

				_, err = valbuf.Write(b)
				if err != nil {
					return nil, err
				}
			}
		}

		/*
			boolean  bool
			blob     []byte
			time
		*/
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

func (stmt *UpsertIntoStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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
		encVal, err := encodeValue(pkVal, table.pk.colType, asPK)
		if err != nil {
			return nil, nil, err
		}

		bs, err := row.Bytes(table, stmt.cols)
		if err != nil {
			return nil, nil, err
		}

		// create entry for the column which is the pk
		pke := &store.KV{
			Key:   e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(table.pk.id), encVal),
			Value: bs,
		}
		des = append(des, pke)

		// create entries for each indexed column, with value as value for pk column
		for colID := range table.indexes {
			cVal := row.Values[cs[colID]]
			encVal, err := encodeValue(cVal, table.colsByID[colID].colType, !asPK)
			if err != nil {
				return nil, nil, err
			}

			ie := &store.KV{
				Key:   e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(colID), encVal, encVal),
				Value: nil,
			}
			des = append(des, ie)
		}
	}

	return
}

type SysFn struct {
	fn string
}

type Param struct {
	id string
}

type RowReader interface {
	Read() (*Row, error)
	Close() error
}

type Row struct {
	Values map[string]interface{}
}

type RawRowReader struct {
	e      *Engine
	snap   *tbtree.Snapshot
	table  *Table
	ordCol *OrdCol
	reader *store.KeyReader
}

func newRawRowReader(e *Engine, snap *tbtree.Snapshot, table *Table, ordCol *OrdCol) (*RawRowReader, error) {
	usePK := ordCol == nil || table.pk.colName == ordCol.col.col

	desc := false
	if ordCol != nil {
		desc = ordCol.desc
	}

	var col *Column

	if usePK {
		col = table.pk
	} else {
		col = table.colsByName[ordCol.col.col]
	}

	prefix := e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(col.id))

	var skey []byte

	if desc {
		encMaxPKValue, err := maxPKVal(table.pk.colType)
		if err != nil {
			return nil, err
		}

		if usePK {
			skey = make([]byte, len(prefix)+len(encMaxPKValue))
			copy(skey, prefix)
			copy(skey[len(prefix):], encMaxPKValue)
		} else {
			encMaxIdxValue, err := maxPKVal(col.colType)
			if err != nil {
				return nil, err
			}

			skey = e.mapKey(rowPrefix, encodeID(table.db.id), encodeID(table.id), encodeID(col.id), encMaxIdxValue, encMaxPKValue)
		}
	}

	rSpec := &tbtree.ReaderSpec{
		SeekKey:       skey,
		InclusiveSeek: true,
		Prefix:        prefix,
		DescOrder:     desc,
	}

	r, err := e.dataStore.NewKeyReader(snap, rSpec)
	if err != nil {
		return nil, err
	}

	return &RawRowReader{
		e:      e,
		snap:   snap,
		table:  table,
		ordCol: ordCol,
		reader: r,
	}, nil
}

func (r *RawRowReader) Read() (*Row, error) {
	mkey, vref, _, _, err := r.reader.Read()
	if err != nil {
		return nil, err
	}

	var v []byte

	//decompose key, determine if it's pk, when it's pk, the value holds the actual row data
	if r.ordCol == nil || r.table.pk.colName == r.ordCol.col.col {
		v, err = vref.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		_, _, _, _, pkVal, err := r.e.unmapRow(mkey)
		if err != nil {
			return nil, err
		}

		v, _, _, err = r.snap.Get(r.e.mapKey(rowPrefix, encodeID(r.table.db.id), encodeID(r.table.id), encodeID(r.table.pk.id), pkVal))
	}

	values := make(map[string]interface{}, len(r.table.colsByID))

	// len(stmt.cols)
	if len(v) < 4 {
		return nil, store.ErrCorruptedData
	}

	voff := 0

	cols := int(binary.BigEndian.Uint32(v[voff:]))
	voff += 4

	for i := 0; i < cols; i++ {
		cNameLen := int(binary.BigEndian.Uint32(v[voff:]))
		voff += 4
		colName := string(v[voff : voff+cNameLen])
		voff += cNameLen

		col, ok := r.table.colsByName[colName]
		if !ok {
			return nil, ErrInvalidColumn
		}

		switch col.colType {
		case StringType:
			{
				vlen := int(binary.BigEndian.Uint32(v[voff:]))
				voff += 4

				v := string(v[voff : voff+vlen])
				voff += vlen
				values[colName] = v
			}
		case IntegerType:
			{
				v := binary.BigEndian.Uint64(v[voff:])
				voff += 8
				values[colName] = v
			}
		}
	}

	return &Row{Values: values}, nil
}

func (r *RawRowReader) Close() error {
	return r.reader.Close()
}

type DataSource interface {
	Resolve(e *Engine, snap *tbtree.Snapshot, ordCol *OrdCol) (RowReader, error)
}

type SelectStmt struct {
	distinct  bool
	selectors []Selector
	ds        DataSource
	join      *JoinSpec
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

func (stmt *SelectStmt) CompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	if len(stmt.orderBy) > 1 {
		return nil, nil, ErrLimitedOrderBy
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

		col, colExists := table.colsByName[stmt.orderBy[0].col.col]
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

func (stmt *SelectStmt) Resolve(e *Engine, snap *tbtree.Snapshot, ordCol *OrdCol) (RowReader, error) {
	if ordCol != nil {
		return nil, ErrLimitedOrderBy
	}

	_, _, err := stmt.CompileUsing(e)
	if err != nil {
		return nil, err
	}

	var orderByCol *OrdCol

	if len(stmt.orderBy) > 0 {
		orderByCol = stmt.orderBy[0]
	}

	rowReader, err := stmt.ds.Resolve(e, snap, orderByCol)
	if err != nil {
		return nil, err
	}

	if stmt.join != nil {
		// JointRowReader
	}

	if stmt.where != nil {
		// FilteredRowReader
	}

	//	rowBuilder := newRowBuilder(stmt.selectors)
	// another to filter selected rows
	//	&RowReaderWithrowReader

	if stmt.groupBy != nil {
		// GroupedRowReader
	}

	if stmt.having != nil {
		// FilteredRowReader
	}

	return rowReader, err

	/*cols := make(map[string]*Column, len(stmt.selectors))

	for _, s := range stmt.selectors {
		colSel := s.(*ColSelector)
		cols[colSel.col] = table.cols[colSel.col]
	}

	return &RowReader{reader: r, cols: cols}, nil
	*/
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
		if e.implicitDatabase == "" {
			return nil, ErrNoDatabaseSelected
		}

		db = e.implicitDatabase
	}

	table, exists := e.catalog.dbsByName[db].tablesByName[stmt.table]
	if !exists {
		return nil, ErrTableDoesNotExist
	}

	return table, nil
}

func (stmt *TableRef) Resolve(e *Engine, snap *tbtree.Snapshot, ordCol *OrdCol) (RowReader, error) {
	table, err := stmt.referencedTable(e)
	if err != nil {
		return nil, err
	}

	return newRawRowReader(e, snap, table, ordCol)
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
	col  *ColSelector
	desc bool
}

type Selector interface {
}

type ColSelector struct {
	db    string
	table string
	col   string
	as    string
}

type AggSelector struct {
	aggFn AggregateFn
	as    string
}

type AggColSelector struct {
	aggFn AggregateFn
	db    string
	table string
	col   string
	as    string
}

type BoolExp interface {
}

type NotBoolExp struct {
	exp BoolExp
}

type LikeBoolExp struct {
	col     *ColSelector
	pattern string
}

type CmpBoolExp struct {
	op          CmpOperator
	left, right BoolExp
}

type BinBoolExp struct {
	op          LogicOperator
	left, right BoolExp
}

type ExistsBoolExp struct {
	q *SelectStmt
}
