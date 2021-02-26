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

const patternSeparator = "/"

const catalogDatabasePrefix = "CATALOG/DATABASE/"
const catalogDatabase = catalogDatabasePrefix + "%s" // e.g. CATALOG/DATABASE/db1

const catalogTablePrefix = "CATALOG/TABLE/"
const catalogTable = catalogTablePrefix + "%s/%s/%s" // e.g. CATALOG/TABLE/db1/table1/col1

const catalogColumnPrefix = "CATALOG/COLUMN/"
const catalogColumn = catalogColumnPrefix + "%s/%s/%s/%s" // e.g. "CATALOG/COLUMN/db1/table1/col1/INTEGER"

const catalogIndexPrefix = "CATALOG/INDEX/"
const catalogIndex = catalogIndexPrefix + "%s/%s/%s" // e.g. CATALOG/INDEX/db1/table1/col1

const dataRow = "DATA/%s/%s/%s/%v" // e.g. DATA/db1/table1/col1/1

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
	ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error)
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

func (stmt *TxStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	for _, stmt := range stmt.stmts {
		cs, ds, err := stmt.ValidateAndCompileUsing(e)
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

func (stmt *CreateDatabaseStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	exists := e.catalog.ExistDatabase(stmt.db)
	if exists {
		return nil, nil, ErrDatabaseAlreadyExists
	}

	kv := &store.KV{
		Key:   e.mapKey(catalogDatabase, stmt.db),
		Value: nil,
	}

	ces = append(ces, kv)

	e.catalog.databases[stmt.db] = &Database{
		name:   stmt.db,
		tables: map[string]*Table{},
	}

	return
}

type UseDatabaseStmt struct {
	db string
}

func (stmt *UseDatabaseStmt) isDDL() bool {
	return false
}

func (stmt *UseDatabaseStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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

func (stmt *UseSnapshotStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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

func (stmt *CreateTableStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDatabase == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	exists := e.catalog.databases[e.implicitDatabase].ExistTable(stmt.table)
	if exists {
		return nil, nil, ErrTableAlreadyExists
	}

	table := &Table{
		name:    stmt.table,
		cols:    make(map[string]*Column, 0),
		indexes: make(map[string]struct{}, 0),
	}

	validPK := false
	for _, cs := range stmt.colsSpec {
		ce := &store.KV{
			Key:   e.mapKey(catalogColumn, e.implicitDatabase, stmt.table, cs.colName, cs.colType),
			Value: nil,
		}
		ces = append(ces, ce)

		_, colExists := table.cols[cs.colName]
		if colExists {
			return nil, nil, ErrDuplicatedColumn
		}

		table.cols[cs.colName] = &Column{
			colName: cs.colName,
			colType: cs.colType,
		}

		if stmt.pk == cs.colName {
			if cs.colType != IntegerType {
				return nil, nil, ErrInvalidPKType
			}
			validPK = true

			table.pk = cs.colName
		}
	}
	if !validPK {
		return nil, nil, ErrInvalidPK
	}

	te := &store.KV{
		Key:   e.mapKey(catalogTable, e.implicitDatabase, stmt.table, stmt.pk),
		Value: nil,
	}
	ces = append(ces, te)

	e.catalog.databases[e.implicitDatabase].tables[stmt.table] = table

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

func (stmt *CreateIndexStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
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

func (stmt *AddColumnStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type UpsertIntoStmt struct {
	table string
	cols  []string
	rows  []*Row
}

func (stmt *UpsertIntoStmt) isDDL() bool {
	return false
}

func (stmt *UpsertIntoStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDatabase == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	table, exists := e.catalog.databases[e.implicitDatabase].tables[stmt.table]
	if !exists {
		return nil, nil, ErrTableDoesNotExist
	}

	pkIncluded := false
	cs := make(map[string]struct{}, len(stmt.cols))

	for _, c := range stmt.cols {
		_, exists := table.cols[c]
		if !exists {
			return nil, nil, ErrInvalidColumn
		}

		if table.pk == c {
			pkIncluded = true
		}

		_, duplicated := cs[c]
		if duplicated {
			return nil, nil, ErrDuplicatedColumn
		}

		cs[c] = struct{}{}
	}
	if !pkIncluded {
		return nil, nil, ErrPKCanNotBeNull
	}

	for _, row := range stmt.rows {
		if len(row.values) != len(stmt.cols) {
			return nil, nil, ErrInvalidNumberOfValues
		}

		valbuf := &bytes.Buffer{}

		// len(stmt.cols)
		var b [4]byte
		binary.BigEndian.PutUint32(b[:], uint32(len(stmt.cols)))

		_, err = valbuf.Write(b[:])
		if err != nil {
			return nil, nil, err
		}

		values := make(map[string]interface{}, 0)

		for i, val := range row.values {
			col, _ := table.cols[stmt.cols[i]]

			// len(colName) + colName
			b := make([]byte, 4+len(col.colName))
			binary.BigEndian.PutUint32(b, uint32(len(col.colName)))
			copy(b[4:], []byte(col.colName))

			_, err = valbuf.Write(b)
			if err != nil {
				return nil, nil, err
			}

			switch col.colType {
			case StringType:
				{
					v, ok := val.(string)
					if !ok {
						return nil, nil, ErrInvalidValue
					}

					// len(v) + v
					b := make([]byte, 4+len(v))
					binary.BigEndian.PutUint32(b, uint32(len(v)))
					copy(b[4:], []byte(v))

					_, err = valbuf.Write(b)
					if err != nil {
						return nil, nil, err
					}

					values[col.colName] = v
				}
			case IntegerType:
				{
					v, ok := val.(uint64)
					if !ok {
						return nil, nil, ErrInvalidValue
					}

					b := make([]byte, 8)
					binary.BigEndian.PutUint64(b, v)

					_, err = valbuf.Write(b)
					if err != nil {
						return nil, nil, err
					}

					values[col.colName] = b
				}
			}

			/*
				boolean  bool
				blob     []byte
				time ?
			*/
		}

		// create entry for the column which is the pk
		pke := &store.KV{
			Key:   e.mapKey(dataRow, e.implicitDatabase, table.name, table.pk, values[table.pk]),
			Value: valbuf.Bytes(),
		}
		des = append(des, pke)

		// create entries for each indexed column, with value as value for pk column
		for ic := range table.indexes {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], values[table.pk].(uint64))

			ie := &store.KV{
				Key:   e.mapKey(dataRow, e.implicitDatabase, table.name, ic, values[ic]),
				Value: b[:],
			}
			des = append(des, ie)
		}
	}

	return
}

type Row struct {
	values []interface{}
}

type String string

func (s String) ToBytes() []byte {
	return []byte(s)
}

type SysFn struct {
	fn string
}

type Param struct {
	id string
}

type SelectStmt struct {
	distinct  bool
	selectors []Selector
	ds        DataSource
	join      *JoinSpec
	where     BoolExp
	groupBy   []*ColSelector
	having    BoolExp
	offset    uint64
	limit     uint64
	orderBy   []*OrdCol
	as        string
}

func (stmt *SelectStmt) isDDL() bool {
	return false
}

func (stmt *SelectStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type DataSource interface {
}

type TableRef struct {
	db    string
	table string
	as    string
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
