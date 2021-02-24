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
	"errors"

	"github.com/codenotary/immudb/embedded/store"
)

const catalogDatabase = "CATALOG/DATABASE/%s"
const catalogTable = "CATALOG/TABLE/%s/%s"

type SQLValueType = int

const (
	IntegerType SQLValueType = iota
	BooleanType
	StringType
	BLOBType
	TimestampType
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
	ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error)
}

type TxStmt struct {
	stmts []SQLStmt
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

func (stmt *CreateDatabaseStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	exists, err := e.ExistDatabase(stmt.db)
	if err != nil {
		return nil, nil, err
	}

	if exists {
		return nil, nil, ErrDatabaseAlreadyExists
	}

	kv := &store.KV{
		Key:   mapKey(catalogDatabase, e.prefix, stmt.db),
		Value: nil,
	}

	ces = append(ces, kv)

	return
}

type UseDatabaseStmt struct {
	db string
}

func (stmt *UseDatabaseStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	exists, err := e.ExistDatabase(stmt.db)
	if err != nil {
		return nil, nil, err
	}

	if !exists {
		return nil, nil, ErrDatabaseNoExists
	}

	e.implicitDatabase = stmt.db

	return
}

type UseSnapshotStmt struct {
	since, upTo string
}

func (stmt *UseSnapshotStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type CreateTableStmt struct {
	table    string
	colsSpec []*ColSpec
}

func (stmt *CreateTableStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	if e.implicitDatabase == "" {
		return nil, nil, ErrNoDatabaseSelected
	}

	mk := mapKey(catalogTable, e.prefix, e.implicitDatabase, stmt.table)

	exists, err := existKey(mk, e.catalogStore)
	if err != nil {
		return nil, nil, err
	}

	if exists {
		return nil, nil, ErrTableAlreadyExists
	}

	kv := &store.KV{
		Key:   mk,
		Value: nil,
	}

	ces = append(ces, kv)

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

func (stmt *CreateIndexStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func (stmt *AddColumnStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type AlterColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func (stmt *AlterColumnStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type InsertIntoStmt struct {
	table string
	cols  []string
	rows  []*Row
}

func (stmt *InsertIntoStmt) ValidateAndCompileUsing(e *Engine) (ces []*store.KV, des []*store.KV, err error) {
	return nil, nil, errors.New("not yet supported")
}

type Row struct {
	values []Value
}

type Value interface {
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
