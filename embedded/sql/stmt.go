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

package sql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/google/uuid"
)

const (
	catalogPrefix       = "CTL."
	catalogTablePrefix  = "CTL.TABLE."  // (key=CTL.TABLE.{1}{tableID}, value={tableNAME})
	catalogColumnPrefix = "CTL.COLUMN." // (key=CTL.COLUMN.{1}{tableID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogIndexPrefix  = "CTL.INDEX."  // (key=CTL.INDEX.{1}{tableID}{indexID}, value={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)})

	RowPrefix = "R." // (key=R.{1}{tableID}{0}({null}({pkVal}{padding}{pkValLen})?)+, value={count (colID valLen val)+})

	MappedPrefix = "M." // (key=M.{tableID}{indexID}({null}({val}{padding}{valLen})?)*({pkVal}{padding}{pkValLen})+, value={count (colID valLen val)+})
)

const DatabaseID = uint32(1) // deprecated but left to maintain backwards compatibility
const PKIndexID = uint32(0)

const (
	nullableFlag      byte = 1 << iota
	autoIncrementFlag byte = 1 << iota
)

const revCol = "_rev"

type SQLValueType = string

const (
	IntegerType   SQLValueType = "INTEGER"
	BooleanType   SQLValueType = "BOOLEAN"
	VarcharType   SQLValueType = "VARCHAR"
	UUIDType      SQLValueType = "UUID"
	BLOBType      SQLValueType = "BLOB"
	Float64Type   SQLValueType = "FLOAT"
	TimestampType SQLValueType = "TIMESTAMP"
	AnyType       SQLValueType = "ANY"
)

func IsNumericType(t SQLValueType) bool {
	return t == IntegerType || t == Float64Type
}

type Permission = string

const (
	PermissionReadOnly  Permission = "READ"
	PermissionReadWrite Permission = "READWRITE"
	PermissionAdmin     Permission = "ADMIN"
)

type AggregateFn = string

const (
	COUNT AggregateFn = "COUNT"
	SUM   AggregateFn = "SUM"
	MAX   AggregateFn = "MAX"
	MIN   AggregateFn = "MIN"
	AVG   AggregateFn = "AVG"
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

const (
	NowFnCall       string = "NOW"
	UUIDFnCall      string = "RANDOM_UUID"
	DatabasesFnCall string = "DATABASES"
	TablesFnCall    string = "TABLES"
	TableFnCall     string = "TABLE"
	UsersFnCall     string = "USERS"
	ColumnsFnCall   string = "COLUMNS"
	IndexesFnCall   string = "INDEXES"
)

type SQLStmt interface {
	execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error)
	inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error
}

type BeginTransactionStmt struct {
}

func (stmt *BeginTransactionStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *BeginTransactionStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, ErrNestedTxNotSupported
	}

	err := tx.RequireExplicitClose()
	if err == nil {
		// current tx can be reused as no changes were already made
		return tx, nil
	}

	// commit current transaction and start a fresh one

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return tx.engine.NewTx(ctx, tx.opts.WithExplicitClose(true))
}

type CommitStmt struct {
}

func (stmt *CommitStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CommitStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.Commit(ctx)
}

type RollbackStmt struct {
}

func (stmt *RollbackStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RollbackStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.IsExplicitCloseRequired() {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.Cancel()
}

type CreateDatabaseStmt struct {
	DB          string
	ifNotExists bool
}

func (stmt *CreateDatabaseStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateDatabaseStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: database creation can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.CreateDatabase(ctx, stmt.DB, stmt.ifNotExists)
}

type UseDatabaseStmt struct {
	DB string
}

func (stmt *UseDatabaseStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseDatabaseStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if stmt.DB == "" {
		return nil, fmt.Errorf("%w: no database name was provided", ErrIllegalArguments)
	}

	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: database selection can NOT be executed within a transaction block", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return tx, tx.engine.multidbHandler.UseDatabase(ctx, stmt.DB)
}

type UseSnapshotStmt struct {
	period period
}

func (stmt *UseSnapshotStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseSnapshotStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return nil, ErrNoSupported
}

type CreateUserStmt struct {
	username   string
	password   string
	permission Permission
}

func (stmt *CreateUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user creation can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.CreateUser(ctx, stmt.username, stmt.password, stmt.permission)
}

type AlterUserStmt struct {
	username   string
	password   string
	permission Permission
}

func (stmt *AlterUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AlterUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user modification can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.AlterUser(ctx, stmt.username, stmt.password, stmt.permission)
}

type DropUserStmt struct {
	username string
}

func (stmt *DropUserStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropUserStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: user deletion can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}

	return nil, tx.engine.multidbHandler.DropUser(ctx, stmt.username)
}

type CreateTableStmt struct {
	table       string
	ifNotExists bool
	colsSpec    []*ColSpec
	pkColNames  []string
}

func NewCreateTableStmt(table string, ifNotExists bool, colsSpec []*ColSpec, pkColNames []string) *CreateTableStmt {
	return &CreateTableStmt{table: table, ifNotExists: ifNotExists, colsSpec: colsSpec, pkColNames: pkColNames}
}

func (stmt *CreateTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if stmt.ifNotExists && tx.catalog.ExistTable(stmt.table) {
		return tx, nil
	}

	colSpecs := make(map[uint32]*ColSpec, len(stmt.colsSpec))
	for i, cs := range stmt.colsSpec {
		colSpecs[uint32(i)+1] = cs
	}

	table, err := tx.catalog.newTable(stmt.table, colSpecs, uint32(len(colSpecs)))
	if err != nil {
		return nil, err
	}

	createIndexStmt := &CreateIndexStmt{unique: true, table: table.name, cols: stmt.pkColNames}
	_, err = createIndexStmt.execAt(ctx, tx, params)
	if err != nil {
		return nil, err
	}

	for _, col := range table.cols {
		if col.autoIncrement {
			if len(table.primaryIndex.cols) > 1 || col.id != table.primaryIndex.cols[0].id {
				return nil, ErrLimitedAutoIncrement
			}
		}

		err := persistColumn(tx, col)
		if err != nil {
			return nil, err
		}
	}

	mappedKey := MapKey(tx.sqlPrefix(), catalogTablePrefix, EncodeID(DatabaseID), EncodeID(table.id))

	err = tx.set(mappedKey, nil, []byte(table.name))
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

func persistColumn(tx *SQLTx, col *Column) error {
	//{auto_incremental | nullable}{maxLen}{colNAME})
	v := make([]byte, 1+4+len(col.colName))

	if col.autoIncrement {
		v[0] = v[0] | autoIncrementFlag
	}

	if col.notNull {
		v[0] = v[0] | nullableFlag
	}

	binary.BigEndian.PutUint32(v[1:], uint32(col.MaxLen()))

	copy(v[5:], []byte(col.Name()))

	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogColumnPrefix,
		EncodeID(DatabaseID),
		EncodeID(col.table.id),
		EncodeID(col.id),
		[]byte(col.colType),
	)

	return tx.set(mappedKey, nil, v)
}

type ColSpec struct {
	colName       string
	colType       SQLValueType
	maxLen        int
	autoIncrement bool
	notNull       bool
}

func NewColSpec(name string, colType SQLValueType, maxLen int, autoIncrement bool, notNull bool) *ColSpec {
	return &ColSpec{
		colName:       name,
		colType:       colType,
		maxLen:        maxLen,
		autoIncrement: autoIncrement,
		notNull:       notNull,
	}
}

type CreateIndexStmt struct {
	unique      bool
	ifNotExists bool
	table       string
	cols        []string
}

func NewCreateIndexStmt(table string, cols []string, isUnique bool) *CreateIndexStmt {
	return &CreateIndexStmt{unique: isUnique, table: table, cols: cols}
}

func (stmt *CreateIndexStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateIndexStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if len(stmt.cols) < 1 {
		return nil, ErrIllegalArguments
	}

	if len(stmt.cols) > MaxNumberOfColumnsInIndex {
		return nil, ErrMaxNumberOfColumnsInIndexExceeded
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	colIDs := make([]uint32, len(stmt.cols))

	indexKeyLen := 0

	for i, colName := range stmt.cols {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		if variableSizedType(col.colType) && !tx.engine.lazyIndexConstraintValidation && (col.MaxLen() == 0 || col.MaxLen() > MaxKeyLen) {
			return nil, fmt.Errorf("%w: can not create index using column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
		}

		indexKeyLen += col.MaxLen()

		colIDs[i] = col.id
	}

	if !tx.engine.lazyIndexConstraintValidation && indexKeyLen > MaxKeyLen {
		return nil, fmt.Errorf("%w: can not create index using columns '%v'. Max key length is %d", ErrLimitedKeyType, stmt.cols, MaxKeyLen)
	}

	if stmt.unique && table.primaryIndex != nil {
		// check table is empty
		pkPrefix := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id))
		_, _, err := tx.getWithPrefix(ctx, pkPrefix, nil)
		if errors.Is(err, store.ErrIndexNotFound) {
			return nil, ErrTableDoesNotExist
		}
		if err == nil {
			return nil, ErrLimitedIndexCreation
		} else if !errors.Is(err, store.ErrKeyNotFound) {
			return nil, err
		}
	}

	index, err := table.newIndex(stmt.unique, colIDs)
	if errors.Is(err, ErrIndexAlreadyExists) && stmt.ifNotExists {
		return tx, nil
	}
	if err != nil {
		return nil, err
	}

	// v={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)}
	// TODO: currently only ASC order is supported
	colSpecLen := EncIDLen + 1

	encodedValues := make([]byte, 1+len(index.cols)*colSpecLen)

	if index.IsUnique() {
		encodedValues[0] = 1
	}

	for i, col := range index.cols {
		copy(encodedValues[1+i*colSpecLen:], EncodeID(col.id))
	}

	mappedKey := MapKey(tx.sqlPrefix(), catalogIndexPrefix, EncodeID(DatabaseID), EncodeID(table.id), EncodeID(index.id))

	err = tx.set(mappedKey, nil, encodedValues)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func NewAddColumnStmt(table string, colSpec *ColSpec) *AddColumnStmt {
	return &AddColumnStmt{table: table, colSpec: colSpec}
}

func (stmt *AddColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AddColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.newColumn(stmt.colSpec)
	if err != nil {
		return nil, err
	}

	err = persistColumn(tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type RenameTableStmt struct {
	oldName string
	newName string
}

func (stmt *RenameTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RenameTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.renameTable(stmt.oldName, stmt.newName)
	if err != nil {
		return nil, err
	}

	// update table name
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogTablePrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
	)
	err = tx.set(mappedKey, nil, []byte(stmt.newName))
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type RenameColumnStmt struct {
	table   string
	oldName string
	newName string
}

func NewRenameColumnStmt(table, oldName, newName string) *RenameColumnStmt {
	return &RenameColumnStmt{table: table, oldName: oldName, newName: newName}
}

func (stmt *RenameColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RenameColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.renameColumn(stmt.oldName, stmt.newName)
	if err != nil {
		return nil, err
	}

	err = persistColumn(tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

type DropColumnStmt struct {
	table   string
	colName string
}

func NewDropColumnStmt(table, colName string) *DropColumnStmt {
	return &DropColumnStmt{table: table, colName: colName}
}

func (stmt *DropColumnStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *DropColumnStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.GetColumnByName(stmt.colName)
	if err != nil {
		return nil, err
	}

	err = table.deleteColumn(col)
	if err != nil {
		return nil, err
	}

	err = persistColumnDeletion(ctx, tx, col)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

func persistColumnDeletion(ctx context.Context, tx *SQLTx, col *Column) error {
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogColumnPrefix,
		EncodeID(DatabaseID),
		EncodeID(col.table.id),
		EncodeID(col.id),
		[]byte(col.colType),
	)

	return tx.delete(ctx, mappedKey)
}

type UpsertIntoStmt struct {
	isInsert   bool
	tableRef   *tableRef
	cols       []string
	rows       []*RowSpec
	onConflict *OnConflictDo
}

func NewUpserIntoStmt(table string, cols []string, rows []*RowSpec, isInsert bool, onConflict *OnConflictDo) *UpsertIntoStmt {
	return &UpsertIntoStmt{
		isInsert:   isInsert,
		tableRef:   NewTableRef(table, ""),
		cols:       cols,
		rows:       rows,
		onConflict: onConflict,
	}
}

type RowSpec struct {
	Values []ValueExp
}

func NewRowSpec(values []ValueExp) *RowSpec {
	return &RowSpec{
		Values: values,
	}
}

type OnConflictDo struct {
}

func (stmt *UpsertIntoStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	for _, row := range stmt.rows {
		if len(stmt.cols) != len(row.Values) {
			return ErrInvalidNumberOfValues
		}

		for i, val := range row.Values {
			table, err := stmt.tableRef.referencedTable(tx)
			if err != nil {
				return err
			}

			col, err := table.GetColumnByName(stmt.cols[i])
			if err != nil {
				return err
			}

			err = val.requiresType(col.colType, make(map[string]ColDescriptor), params, table.name)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (stmt *UpsertIntoStmt) validate(table *Table) (map[uint32]int, error) {
	selPosByColID := make(map[uint32]int, len(stmt.cols))

	for i, c := range stmt.cols {
		col, err := table.GetColumnByName(c)
		if err != nil {
			return nil, err
		}

		_, duplicated := selPosByColID[col.id]
		if duplicated {
			return nil, fmt.Errorf("%w (%s)", ErrDuplicatedColumn, col.colName)
		}

		selPosByColID[col.id] = i
	}

	return selPosByColID, nil
}

func (stmt *UpsertIntoStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	table, err := stmt.tableRef.referencedTable(tx)
	if err != nil {
		return nil, err
	}

	selPosByColID, err := stmt.validate(table)
	if err != nil {
		return nil, err
	}

	for _, row := range stmt.rows {
		if len(row.Values) != len(stmt.cols) {
			return nil, ErrInvalidNumberOfValues
		}

		valuesByColID := make(map[uint32]TypedValue)

		var pkMustExist bool

		for colID, col := range table.colsByID {
			colPos, specified := selPosByColID[colID]
			if !specified {
				// TODO: Default values
				if col.notNull && !col.autoIncrement {
					return nil, fmt.Errorf("%w (%s)", ErrNotNullableColumnCannotBeNull, col.colName)
				}

				// inject auto-incremental pk value
				if stmt.isInsert && col.autoIncrement {
					// current implementation assumes only PK can be set as autoincremental
					table.maxPK++

					pkCol := table.primaryIndex.cols[0]
					valuesByColID[pkCol.id] = &Integer{val: table.maxPK}

					if _, ok := tx.firstInsertedPKs[table.name]; !ok {
						tx.firstInsertedPKs[table.name] = table.maxPK
					}
					tx.lastInsertedPKs[table.name] = table.maxPK
				}

				continue
			}

			// value was specified
			cVal := row.Values[colPos]

			val, err := cVal.substitute(params)
			if err != nil {
				return nil, err
			}

			rval, err := val.reduce(tx, nil, table.name)
			if err != nil {
				return nil, err
			}

			if rval.IsNull() {
				if col.notNull || col.autoIncrement {
					return nil, fmt.Errorf("%w (%s)", ErrNotNullableColumnCannotBeNull, col.colName)
				}

				continue
			}

			if col.autoIncrement {
				// validate specified value
				nl, isNumber := rval.RawValue().(int64)
				if !isNumber {
					return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
				}

				pkMustExist = nl <= table.maxPK

				if _, ok := tx.firstInsertedPKs[table.name]; !ok {
					tx.firstInsertedPKs[table.name] = nl
				}
				tx.lastInsertedPKs[table.name] = nl
			}

			valuesByColID[colID] = rval
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		// pk entry
		mappedPKey := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals, pkEncVals)

		_, err = tx.get(ctx, mappedPKey)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, err
		}

		if errors.Is(err, store.ErrKeyNotFound) && pkMustExist {
			return nil, fmt.Errorf("%w: specified value must be greater than current one", ErrInvalidValue)
		}

		if stmt.isInsert {
			if err == nil && stmt.onConflict == nil {
				return nil, store.ErrKeyAlreadyExists
			}

			if err == nil && stmt.onConflict != nil {
				// TODO: conflict resolution may be extended. Currently only supports "ON CONFLICT DO NOTHING"
				continue
			}
		}

		err = tx.doUpsert(ctx, pkEncVals, valuesByColID, table, !stmt.isInsert)
		if err != nil {
			return nil, err
		}
	}

	return tx, nil
}

func (tx *SQLTx) encodeRowValue(valuesByColID map[uint32]TypedValue, table *Table) ([]byte, error) {
	valbuf := bytes.Buffer{}

	// null values are not serialized
	encodedVals := 0
	for _, v := range valuesByColID {
		if !v.IsNull() {
			encodedVals++
		}
	}

	b := make([]byte, EncLenLen)
	binary.BigEndian.PutUint32(b, uint32(encodedVals))

	_, err := valbuf.Write(b)
	if err != nil {
		return nil, err
	}

	for _, col := range table.cols {
		rval, specified := valuesByColID[col.id]
		if !specified || rval.IsNull() {
			continue
		}

		b := make([]byte, EncIDLen)
		binary.BigEndian.PutUint32(b, uint32(col.id))

		_, err = valbuf.Write(b)
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}

		encVal, err := EncodeValue(rval, col.colType, col.MaxLen())
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}

		_, err = valbuf.Write(encVal)
		if err != nil {
			return nil, fmt.Errorf("%w: table: %s, column: %s", err, table.name, col.colName)
		}
	}

	return valbuf.Bytes(), nil
}

func (tx *SQLTx) doUpsert(ctx context.Context, pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table, reuseIndex bool) error {
	var reusableIndexEntries map[uint32]struct{}

	if reuseIndex && len(table.indexes) > 1 {
		currPKRow, err := tx.fetchPKRow(ctx, table, valuesByColID)
		if err == nil {
			currValuesByColID := make(map[uint32]TypedValue, len(currPKRow.ValuesBySelector))

			for _, col := range table.cols {
				encSel := EncodeSelector("", table.name, col.colName)
				currValuesByColID[col.id] = currPKRow.ValuesBySelector[encSel]
			}

			reusableIndexEntries, err = tx.deprecateIndexEntries(pkEncVals, currValuesByColID, valuesByColID, table)
			if err != nil {
				return err
			}
		} else if !errors.Is(err, ErrNoMoreRows) {
			return err
		}
	}

	rowKey := MapKey(tx.sqlPrefix(), RowPrefix, EncodeID(DatabaseID), EncodeID(table.id), EncodeID(PKIndexID), pkEncVals)

	encodedRowValue, err := tx.encodeRowValue(valuesByColID, table)
	if err != nil {
		return err
	}

	err = tx.set(rowKey, nil, encodedRowValue)
	if err != nil {
		return err
	}

	// create in-memory and validate entries for secondary indexes
	for _, index := range table.indexes {
		if index.IsPrimary() {
			continue
		}

		if reusableIndexEntries != nil {
			_, reusable := reusableIndexEntries[index.id]
			if reusable {
				continue
			}
		}

		encodedValues := make([][]byte, 2+len(index.cols))
		encodedValues[0] = EncodeID(table.id)
		encodedValues[1] = EncodeID(index.id)

		indexKeyLen := 0

		for i, col := range index.cols {
			rval, specified := valuesByColID[col.id]
			if !specified {
				rval = &NullValue{t: col.colType}
			}

			encVal, n, err := EncodeValueAsKey(rval, col.colType, col.MaxLen())
			if err != nil {
				return fmt.Errorf("%w: index on '%s' and column '%s'", err, index.Name(), col.colName)
			}

			if n > MaxKeyLen {
				return fmt.Errorf("%w: can not index entry for column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
			}

			indexKeyLen += n

			encodedValues[i+2] = encVal
		}

		if indexKeyLen > MaxKeyLen {
			return fmt.Errorf("%w: can not index entry using columns '%v'. Max key length is %d", ErrLimitedKeyType, index.cols, MaxKeyLen)
		}

		smkey := MapKey(tx.sqlPrefix(), MappedPrefix, encodedValues...)

		// no other equivalent entry should be already indexed
		if index.IsUnique() {
			_, valRef, err := tx.getWithPrefix(ctx, smkey, nil)
			if err == nil && (valRef.KVMetadata() == nil || !valRef.KVMetadata().Deleted()) {
				return store.ErrKeyAlreadyExists
			} else if !errors.Is(err, store.ErrKeyNotFound) {
				return err
			}
		}

		err = tx.setTransient(smkey, nil, encodedRowValue) // only-indexable
		if err != nil {
			return err
		}
	}

	tx.updatedRows++

	return nil
}

func encodedKey(index *Index, valuesByColID map[uint32]TypedValue) ([]byte, error) {
	valbuf := bytes.Buffer{}

	indexKeyLen := 0

	for _, col := range index.cols {
		rval, specified := valuesByColID[col.id]
		if !specified || rval.IsNull() {
			return nil, ErrPKCanNotBeNull
		}

		encVal, n, err := EncodeValueAsKey(rval, col.colType, col.MaxLen())
		if err != nil {
			return nil, fmt.Errorf("%w: index of table '%s' and column '%s'", err, index.table.name, col.colName)
		}

		if n > MaxKeyLen {
			return nil, fmt.Errorf("%w: invalid key entry for column '%s'. Max key length for variable columns is %d", ErrLimitedKeyType, col.colName, MaxKeyLen)
		}

		indexKeyLen += n

		_, err = valbuf.Write(encVal)
		if err != nil {
			return nil, err
		}
	}

	if indexKeyLen > MaxKeyLen {
		return nil, fmt.Errorf("%w: invalid key entry using columns '%v'. Max key length is %d", ErrLimitedKeyType, index.cols, MaxKeyLen)
	}

	return valbuf.Bytes(), nil
}

func (tx *SQLTx) fetchPKRow(ctx context.Context, table *Table, valuesByColID map[uint32]TypedValue) (*Row, error) {
	pkRanges := make(map[uint32]*typedValueRange, len(table.primaryIndex.cols))

	for _, pkCol := range table.primaryIndex.cols {
		pkVal := valuesByColID[pkCol.id]

		pkRanges[pkCol.id] = &typedValueRange{
			lRange: &typedValueSemiRange{val: pkVal, inclusive: true},
			hRange: &typedValueSemiRange{val: pkVal, inclusive: true},
		}
	}

	scanSpecs := &ScanSpecs{
		Index:         table.primaryIndex,
		rangesByColID: pkRanges,
	}

	r, err := newRawRowReader(tx, nil, table, period{}, table.name, scanSpecs)
	if err != nil {
		return nil, err
	}

	defer func() {
		r.Close()
	}()

	return r.Read(ctx)
}

// deprecateIndexEntries mark previous index entries as deleted
func (tx *SQLTx) deprecateIndexEntries(
	pkEncVals []byte,
	currValuesByColID, newValuesByColID map[uint32]TypedValue,
	table *Table) (reusableIndexEntries map[uint32]struct{}, err error) {

	encodedRowValue, err := tx.encodeRowValue(currValuesByColID, table)
	if err != nil {
		return nil, err
	}

	reusableIndexEntries = make(map[uint32]struct{})

	for _, index := range table.indexes {
		if index.IsPrimary() {
			continue
		}

		encodedValues := make([][]byte, 2+len(index.cols)+1)
		encodedValues[0] = EncodeID(table.id)
		encodedValues[1] = EncodeID(index.id)
		encodedValues[len(encodedValues)-1] = pkEncVals

		// existent index entry is deleted only if it differs from existent one
		sameIndexKey := true

		for i, col := range index.cols {
			currVal, specified := currValuesByColID[col.id]
			if !specified {
				currVal = &NullValue{t: col.colType}
			}

			newVal, specified := newValuesByColID[col.id]
			if !specified {
				newVal = &NullValue{t: col.colType}
			}

			r, err := currVal.Compare(newVal)
			if err != nil {
				return nil, err
			}

			sameIndexKey = sameIndexKey && r == 0

			encVal, _, _ := EncodeValueAsKey(currVal, col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		// mark existent index entry as deleted
		if sameIndexKey {
			reusableIndexEntries[index.id] = struct{}{}
		} else {
			md := store.NewKVMetadata()

			md.AsDeleted(true)

			err = tx.set(MapKey(tx.sqlPrefix(), MappedPrefix, encodedValues...), md, encodedRowValue)
			if err != nil {
				return nil, err
			}
		}
	}

	return reusableIndexEntries, nil
}

type UpdateStmt struct {
	tableRef *tableRef
	where    ValueExp
	updates  []*colUpdate
	indexOn  []string
	limit    ValueExp
	offset   ValueExp
}

type colUpdate struct {
	col string
	op  CmpOperator
	val ValueExp
}

func (stmt *UpdateStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	selectStmt := &SelectStmt{
		ds:    stmt.tableRef,
		where: stmt.where,
	}

	err := selectStmt.inferParameters(ctx, tx, params)
	if err != nil {
		return err
	}

	table, err := stmt.tableRef.referencedTable(tx)
	if err != nil {
		return err
	}

	for _, update := range stmt.updates {
		col, err := table.GetColumnByName(update.col)
		if err != nil {
			return err
		}

		err = update.val.requiresType(col.colType, make(map[string]ColDescriptor), params, table.name)
		if err != nil {
			return err
		}
	}

	return nil
}

func (stmt *UpdateStmt) validate(table *Table) error {
	colIDs := make(map[uint32]struct{}, len(stmt.updates))

	for _, update := range stmt.updates {
		if update.op != EQ {
			return ErrIllegalArguments
		}

		col, err := table.GetColumnByName(update.col)
		if err != nil {
			return err
		}

		if table.PrimaryIndex().IncludesCol(col.id) {
			return ErrPKCanNotBeUpdated
		}

		_, duplicated := colIDs[col.id]
		if duplicated {
			return ErrDuplicatedColumn
		}

		colIDs[col.id] = struct{}{}
	}

	return nil
}

func (stmt *UpdateStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		limit:   stmt.limit,
		offset:  stmt.offset,
	}

	rowReader, err := selectStmt.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	err = stmt.validate(table)
	if err != nil {
		return nil, err
	}

	cols, err := rowReader.colsBySelector(ctx)
	if err != nil {
		return nil, err
	}

	for {
		row, err := rowReader.Read(ctx)
		if errors.Is(err, ErrNoMoreRows) {
			break
		} else if err != nil {
			return nil, err
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.name, col.colName)
			valuesByColID[col.id] = row.ValuesBySelector[encSel]
		}

		for _, update := range stmt.updates {
			col, err := table.GetColumnByName(update.col)
			if err != nil {
				return nil, err
			}

			sval, err := update.val.substitute(params)
			if err != nil {
				return nil, err
			}

			rval, err := sval.reduce(tx, row, table.name)
			if err != nil {
				return nil, err
			}

			err = rval.requiresType(col.colType, cols, nil, table.name)
			if err != nil {
				return nil, err
			}

			valuesByColID[col.id] = rval
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		// primary index entry
		mkey := MapKey(tx.sqlPrefix(), MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals, pkEncVals)

		// mkey must exist
		_, err = tx.get(ctx, mkey)
		if err != nil {
			return nil, err
		}

		err = tx.doUpsert(ctx, pkEncVals, valuesByColID, table, true)
		if err != nil {
			return nil, err
		}
	}

	return tx, nil
}

type DeleteFromStmt struct {
	tableRef *tableRef
	where    ValueExp
	indexOn  []string
	orderBy  []*OrdCol
	limit    ValueExp
	offset   ValueExp
}

func NewDeleteFromStmt(table string, where ValueExp, orderBy []*OrdCol, limit ValueExp) *DeleteFromStmt {
	return &DeleteFromStmt{
		tableRef: NewTableRef(table, ""),
		where:    where,
		orderBy:  orderBy,
		limit:    limit,
	}
}

func (stmt *DeleteFromStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		orderBy: stmt.orderBy,
	}
	return selectStmt.inferParameters(ctx, tx, params)
}

func (stmt *DeleteFromStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		orderBy: stmt.orderBy,
		limit:   stmt.limit,
		offset:  stmt.offset,
	}

	rowReader, err := selectStmt.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	for {
		row, err := rowReader.Read(ctx)
		if errors.Is(err, ErrNoMoreRows) {
			break
		}
		if err != nil {
			return nil, err
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.name, col.colName)
			valuesByColID[col.id] = row.ValuesBySelector[encSel]
		}

		pkEncVals, err := encodedKey(table.primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		err = tx.deleteIndexEntries(pkEncVals, valuesByColID, table)
		if err != nil {
			return nil, err
		}

		tx.updatedRows++
	}

	return tx, nil
}

func (tx *SQLTx) deleteIndexEntries(pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table) error {
	encodedRowValue, err := tx.encodeRowValue(valuesByColID, table)
	if err != nil {
		return err
	}

	for _, index := range table.indexes {
		if !index.IsPrimary() {
			continue
		}

		encodedValues := make([][]byte, 3+len(index.cols))
		encodedValues[0] = EncodeID(DatabaseID)
		encodedValues[1] = EncodeID(table.id)
		encodedValues[2] = EncodeID(index.id)

		for i, col := range index.cols {
			val, specified := valuesByColID[col.id]
			if !specified {
				val = &NullValue{t: col.colType}
			}

			encVal, _, _ := EncodeValueAsKey(val, col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		md := store.NewKVMetadata()

		md.AsDeleted(true)

		err := tx.set(MapKey(tx.sqlPrefix(), RowPrefix, encodedValues...), md, encodedRowValue)
		if err != nil {
			return err
		}
	}

	return nil
}

type ValueExp interface {
	inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error)
	requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error
	substitute(params map[string]interface{}) (ValueExp, error)
	reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error)
	reduceSelectors(row *Row, implicitTable string) ValueExp
	isConstant() bool
	selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error
}

type typedValueRange struct {
	lRange *typedValueSemiRange
	hRange *typedValueSemiRange
}

type typedValueSemiRange struct {
	val       TypedValue
	inclusive bool
}

func (r *typedValueRange) unitary() bool {
	// TODO: this simplified implementation doesn't cover all unitary cases e.g. 3<=v<4
	if r.lRange == nil || r.hRange == nil {
		return false
	}

	res, _ := r.lRange.val.Compare(r.hRange.val)
	return res == 0 && r.lRange.inclusive && r.hRange.inclusive
}

func (r *typedValueRange) refineWith(refiningRange *typedValueRange) error {
	if r.lRange == nil {
		r.lRange = refiningRange.lRange
	} else if r.lRange != nil && refiningRange.lRange != nil {
		maxRange, err := maxSemiRange(r.lRange, refiningRange.lRange)
		if err != nil {
			return err
		}
		r.lRange = maxRange
	}

	if r.hRange == nil {
		r.hRange = refiningRange.hRange
	} else if r.hRange != nil && refiningRange.hRange != nil {
		minRange, err := minSemiRange(r.hRange, refiningRange.hRange)
		if err != nil {
			return err
		}
		r.hRange = minRange
	}

	return nil
}

func (r *typedValueRange) extendWith(extendingRange *typedValueRange) error {
	if r.lRange == nil || extendingRange.lRange == nil {
		r.lRange = nil
	} else {
		minRange, err := minSemiRange(r.lRange, extendingRange.lRange)
		if err != nil {
			return err
		}
		r.lRange = minRange
	}

	if r.hRange == nil || extendingRange.hRange == nil {
		r.hRange = nil
	} else {
		maxRange, err := maxSemiRange(r.hRange, extendingRange.hRange)
		if err != nil {
			return err
		}
		r.hRange = maxRange
	}

	return nil
}

func maxSemiRange(or1, or2 *typedValueSemiRange) (*typedValueSemiRange, error) {
	r, err := or1.val.Compare(or2.val)
	if err != nil {
		return nil, err
	}

	maxVal := or1.val
	if r < 0 {
		maxVal = or2.val
	}

	return &typedValueSemiRange{
		val:       maxVal,
		inclusive: or1.inclusive && or2.inclusive,
	}, nil
}

func minSemiRange(or1, or2 *typedValueSemiRange) (*typedValueSemiRange, error) {
	r, err := or1.val.Compare(or2.val)
	if err != nil {
		return nil, err
	}

	minVal := or1.val
	if r > 0 {
		minVal = or2.val
	}

	return &typedValueSemiRange{
		val:       minVal,
		inclusive: or1.inclusive || or2.inclusive,
	}, nil
}

type TypedValue interface {
	ValueExp
	Type() SQLValueType
	RawValue() interface{}
	Compare(val TypedValue) (int, error)
	IsNull() bool
}

func NewNull(t SQLValueType) *NullValue {
	return &NullValue{t: t}
}

type NullValue struct {
	t SQLValueType
}

func (n *NullValue) Type() SQLValueType {
	return n.t
}

func (n *NullValue) RawValue() interface{} {
	return nil
}

func (n *NullValue) IsNull() bool {
	return true
}

func (n *NullValue) Compare(val TypedValue) (int, error) {
	if n.t != AnyType && val.Type() != AnyType && n.t != val.Type() {
		return 0, ErrNotComparableValues
	}

	if val.RawValue() == nil {
		return 0, nil
	}

	return -1, nil
}

func (v *NullValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return v.t, nil
}

func (v *NullValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if v.t == t {
		return nil
	}

	if v.t != AnyType {
		return ErrInvalidTypes
	}

	v.t = t

	return nil
}

func (v *NullValue) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *NullValue) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *NullValue) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *NullValue) isConstant() bool {
	return true
}

func (v *NullValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type Integer struct {
	val int64
}

func NewInteger(val int64) *Integer {
	return &Integer{val: val}
}

func (v *Integer) Type() SQLValueType {
	return IntegerType
}

func (v *Integer) IsNull() bool {
	return false
}

func (v *Integer) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *Integer) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	return nil
}

func (v *Integer) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Integer) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Integer) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Integer) isConstant() bool {
	return true
}

func (v *Integer) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Integer) RawValue() interface{} {
	return v.val
}

func (v *Integer) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() == Float64Type {
		r, err := val.Compare(v)
		return r * -1, err
	}

	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(int64)

	if v.val == rval {
		return 0, nil
	}

	if v.val > rval {
		return 1, nil
	}

	return -1, nil
}

type Timestamp struct {
	val time.Time
}

func (v *Timestamp) Type() SQLValueType {
	return TimestampType
}

func (v *Timestamp) IsNull() bool {
	return false
}

func (v *Timestamp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return TimestampType, nil
}

func (v *Timestamp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != TimestampType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
	}

	return nil
}

func (v *Timestamp) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Timestamp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Timestamp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Timestamp) isConstant() bool {
	return true
}

func (v *Timestamp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Timestamp) RawValue() interface{} {
	return v.val
}

func (v *Timestamp) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != TimestampType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(time.Time)

	if v.val.Before(rval) {
		return -1, nil
	}

	if v.val.After(rval) {
		return 1, nil
	}

	return 0, nil
}

type Varchar struct {
	val string
}

func NewVarchar(val string) *Varchar {
	return &Varchar{val: val}
}

func (v *Varchar) Type() SQLValueType {
	return VarcharType
}

func (v *Varchar) IsNull() bool {
	return false
}

func (v *Varchar) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (v *Varchar) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}

	return nil
}

func (v *Varchar) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Varchar) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Varchar) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Varchar) isConstant() bool {
	return true
}

func (v *Varchar) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Varchar) RawValue() interface{} {
	return v.val
}

func (v *Varchar) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != VarcharType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(string)

	return bytes.Compare([]byte(v.val), []byte(rval)), nil
}

type UUID struct {
	val uuid.UUID
}

func NewUUID(val uuid.UUID) *UUID {
	return &UUID{val: val}
}

func (v *UUID) Type() SQLValueType {
	return UUIDType
}

func (v *UUID) IsNull() bool {
	return false
}

func (v *UUID) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return UUIDType, nil
}

func (v *UUID) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != UUIDType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, UUIDType, t)
	}

	return nil
}

func (v *UUID) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *UUID) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *UUID) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *UUID) isConstant() bool {
	return true
}

func (v *UUID) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *UUID) RawValue() interface{} {
	return v.val
}

func (v *UUID) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != UUIDType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(uuid.UUID)

	return bytes.Compare(v.val[:], rval[:]), nil
}

type Bool struct {
	val bool
}

func NewBool(val bool) *Bool {
	return &Bool{val: val}
}

func (v *Bool) Type() SQLValueType {
	return BooleanType
}

func (v *Bool) IsNull() bool {
	return false
}

func (v *Bool) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (v *Bool) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	return nil
}

func (v *Bool) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Bool) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Bool) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Bool) isConstant() bool {
	return true
}

func (v *Bool) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Bool) RawValue() interface{} {
	return v.val
}

func (v *Bool) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != BooleanType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().(bool)

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

func NewBlob(val []byte) *Blob {
	return &Blob{val: val}
}

func (v *Blob) Type() SQLValueType {
	return BLOBType
}

func (v *Blob) IsNull() bool {
	return false
}

func (v *Blob) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return BLOBType, nil
}

func (v *Blob) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BLOBType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BLOBType, t)
	}

	return nil
}

func (v *Blob) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Blob) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Blob) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Blob) isConstant() bool {
	return true
}

func (v *Blob) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Blob) RawValue() interface{} {
	return v.val
}

func (v *Blob) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != BLOBType {
		return 0, ErrNotComparableValues
	}

	rval := val.RawValue().([]byte)

	return bytes.Compare(v.val, rval), nil
}

type Float64 struct {
	val float64
}

func NewFloat64(val float64) *Float64 {
	return &Float64{val: val}
}

func (v *Float64) Type() SQLValueType {
	return Float64Type
}

func (v *Float64) IsNull() bool {
	return false
}

func (v *Float64) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return Float64Type, nil
}

func (v *Float64) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != Float64Type {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, Float64Type, t)
	}

	return nil
}

func (v *Float64) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Float64) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Float64) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *Float64) isConstant() bool {
	return true
}

func (v *Float64) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Float64) RawValue() interface{} {
	return v.val
}

func (v *Float64) Compare(val TypedValue) (int, error) {
	convVal, err := mayApplyImplicitConversion(val.RawValue(), Float64Type)
	if err != nil {
		return 0, err
	}

	if convVal == nil {
		return 1, nil
	}

	rval, ok := convVal.(float64)
	if !ok {
		return 0, ErrNotComparableValues
	}

	if v.val == rval {
		return 0, nil
	}

	if v.val > rval {
		return 1, nil
	}

	return -1, nil
}

type FnCall struct {
	fn     string
	params []ValueExp
}

func (v *FnCall) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if strings.ToUpper(v.fn) == NowFnCall {
		return TimestampType, nil
	}

	if strings.ToUpper(v.fn) == UUIDFnCall {
		return UUIDType, nil
	}

	return AnyType, fmt.Errorf("%w: unknown function %s", ErrIllegalArguments, v.fn)
}

func (v *FnCall) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if strings.ToUpper(v.fn) == NowFnCall {
		if t != TimestampType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
		}

		return nil
	}

	if strings.ToUpper(v.fn) == UUIDFnCall {
		if t != UUIDType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, UUIDType, t)
		}

		return nil
	}

	return fmt.Errorf("%w: unkown function %s", ErrIllegalArguments, v.fn)
}

func (v *FnCall) substitute(params map[string]interface{}) (val ValueExp, err error) {
	ps := make([]ValueExp, len(v.params))

	for i, p := range v.params {
		ps[i], err = p.substitute(params)
		if err != nil {
			return nil, err
		}
	}

	return &FnCall{
		fn:     v.fn,
		params: ps,
	}, nil
}

func (v *FnCall) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if strings.ToUpper(v.fn) == NowFnCall {
		if len(v.params) > 0 {
			return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, NowFnCall, len(v.params))
		}
		return &Timestamp{val: tx.Timestamp().Truncate(time.Microsecond).UTC()}, nil
	}

	if strings.ToUpper(v.fn) == UUIDFnCall {
		if len(v.params) > 0 {
			return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, UUIDFnCall, len(v.params))
		}
		return &UUID{val: uuid.New()}, nil
	}

	return nil, fmt.Errorf("%w: unkown function %s", ErrIllegalArguments, v.fn)
}

func (v *FnCall) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return v
}

func (v *FnCall) isConstant() bool {
	return false
}

func (v *FnCall) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type Cast struct {
	val ValueExp
	t   SQLValueType
}

func (c *Cast) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	_, err := c.val.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	// val type may be restricted by compatible conversions, but multiple types may be compatible...

	return c.t, nil
}

func (c *Cast) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if c.t != t {
		return fmt.Errorf("%w: can not use value cast to %s as %s", ErrInvalidTypes, c.t, t)
	}

	return nil
}

func (c *Cast) substitute(params map[string]interface{}) (ValueExp, error) {
	val, err := c.val.substitute(params)
	if err != nil {
		return nil, err
	}
	c.val = val
	return c, nil
}

func (c *Cast) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	val, err := c.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	conv, err := getConverter(val.Type(), c.t)
	if conv == nil {
		return nil, err
	}

	return conv(val)
}

func (c *Cast) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &Cast{
		val: c.val.reduceSelectors(row, implicitTable),
		t:   c.t,
	}
}

func (c *Cast) isConstant() bool {
	return c.val.isConstant()
}

func (c *Cast) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type Param struct {
	id  string
	pos int
}

func (v *Param) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	t, ok := params[v.id]
	if !ok {
		params[v.id] = AnyType
		return AnyType, nil
	}

	return t, nil
}

func (v *Param) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	currT, ok := params[v.id]
	if ok && currT != t && currT != AnyType {
		return ErrInferredMultipleTypes
	}

	params[v.id] = t

	return nil
}

func (p *Param) substitute(params map[string]interface{}) (ValueExp, error) {
	val, ok := params[p.id]
	if !ok {
		return nil, fmt.Errorf("%w(%s)", ErrMissingParameter, p.id)
	}

	if val == nil {
		return &NullValue{t: AnyType}, nil
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
			return &Integer{val: int64(v)}, nil
		}
	case uint:
		{
			return &Integer{val: int64(v)}, nil
		}
	case uint64:
		{
			return &Integer{val: int64(v)}, nil
		}
	case int64:
		{
			return &Integer{val: v}, nil
		}
	case []byte:
		{
			return &Blob{val: v}, nil
		}
	case time.Time:
		{
			return &Timestamp{val: v.Truncate(time.Microsecond).UTC()}, nil
		}
	case float64:
		{
			return &Float64{val: v}, nil
		}
	}

	return nil, ErrUnsupportedParameter
}

func (p *Param) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (p *Param) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return p
}

func (p *Param) isConstant() bool {
	return true
}

func (v *Param) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
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
	SQLStmt
	Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error)
	Alias() string
}

type SelectStmt struct {
	distinct  bool
	selectors []Selector
	ds        DataSource
	indexOn   []string
	joins     []*JoinSpec
	where     ValueExp
	groupBy   []*ColSelector
	having    ValueExp
	orderBy   []*OrdCol
	limit     ValueExp
	offset    ValueExp
	as        string
}

func NewSelectStmt(
	selectors []Selector,
	ds DataSource,
	where ValueExp,
	orderBy []*OrdCol,
	limit ValueExp,
	offset ValueExp,
) *SelectStmt {
	return &SelectStmt{
		selectors: selectors,
		ds:        ds,
		where:     where,
		orderBy:   orderBy,
		limit:     limit,
		offset:    offset,
	}
}

func (stmt *SelectStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	_, err := stmt.execAt(ctx, tx, nil)
	if err != nil {
		return err
	}

	// TODO (jeroiraz) may be optimized so to resolve the query statement just once
	rowReader, err := stmt.Resolve(ctx, tx, nil, nil)
	if err != nil {
		return err
	}
	defer rowReader.Close()

	return rowReader.InferParameters(ctx, params)
}

func (stmt *SelectStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if stmt.groupBy == nil && stmt.having != nil {
		return nil, ErrHavingClauseRequiresGroupClause
	}

	if len(stmt.groupBy) > 1 {
		return nil, ErrLimitedGroupBy
	}

	if len(stmt.orderBy) > 1 {
		return nil, ErrLimitedOrderBy
	}

	if len(stmt.orderBy) > 0 {
		tableRef, ok := stmt.ds.(*tableRef)
		if !ok {
			return nil, ErrLimitedOrderBy
		}

		table, err := tableRef.referencedTable(tx)
		if err != nil {
			return nil, err
		}

		colName := stmt.orderBy[0].sel.col

		indexed, err := table.IsIndexed(colName)
		if err != nil {
			return nil, err
		}

		if !indexed {
			return nil, ErrLimitedOrderBy
		}
	}

	return tx, nil
}

func (stmt *SelectStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	scanSpecs, err := stmt.genScanSpecs(tx, params)
	if err != nil {
		return nil, err
	}

	rowReader, err := stmt.ds.Resolve(ctx, tx, params, scanSpecs)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rowReader.Close()
		}
	}()

	if stmt.joins != nil {
		jointRowReader, err := newJointRowReader(rowReader, stmt.joins)
		if err != nil {
			return nil, err
		}
		rowReader = jointRowReader
	}

	if stmt.where != nil {
		rowReader = newConditionalRowReader(rowReader, stmt.where)
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

		groupedRowReader, err := newGroupedRowReader(rowReader, stmt.selectors, groupBy)
		if err != nil {
			return nil, err
		}
		rowReader = groupedRowReader

		if stmt.having != nil {
			rowReader = newConditionalRowReader(rowReader, stmt.having)
		}
	}

	projectedRowReader, err := newProjectedRowReader(ctx, rowReader, stmt.as, stmt.selectors)
	if err != nil {
		return nil, err
	}
	rowReader = projectedRowReader

	if stmt.distinct {
		distinctRowReader, err := newDistinctRowReader(ctx, rowReader)
		if err != nil {
			return nil, err
		}
		rowReader = distinctRowReader
	}

	if stmt.offset != nil {
		offset, err := evalExpAsInt(tx, stmt.offset, params)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid offset", err)
		}

		rowReader = newOffsetRowReader(rowReader, offset)
	}

	if stmt.limit != nil {
		limit, err := evalExpAsInt(tx, stmt.limit, params)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid limit", err)
		}

		if limit < 0 {
			return nil, fmt.Errorf("%w: invalid limit", ErrIllegalArguments)
		}

		if limit > 0 {
			rowReader = newLimitRowReader(rowReader, limit)
		}
	}

	return rowReader, nil
}

func evalExpAsInt(tx *SQLTx, exp ValueExp, params map[string]interface{}) (int, error) {
	offset, err := exp.substitute(params)
	if err != nil {
		return 0, err
	}

	texp, err := offset.reduce(tx, nil, "")
	if err != nil {
		return 0, err
	}

	convVal, err := mayApplyImplicitConversion(texp.RawValue(), IntegerType)
	if err != nil {
		return 0, ErrInvalidValue
	}

	num, ok := convVal.(int64)
	if !ok {
		return 0, ErrInvalidValue
	}

	if num > math.MaxInt32 {
		return 0, ErrInvalidValue
	}

	return int(num), nil
}

func (stmt *SelectStmt) Alias() string {
	if stmt.as == "" {
		return stmt.ds.Alias()
	}

	return stmt.as
}

func (stmt *SelectStmt) genScanSpecs(tx *SQLTx, params map[string]interface{}) (*ScanSpecs, error) {
	tableRef, isTableRef := stmt.ds.(*tableRef)
	if !isTableRef {
		return nil, nil
	}

	table, err := tableRef.referencedTable(tx)
	if err != nil {
		return nil, err
	}

	rangesByColID := make(map[uint32]*typedValueRange)
	if stmt.where != nil {
		err = stmt.where.selectorRanges(table, tableRef.Alias(), params, rangesByColID)
		if err != nil {
			return nil, err
		}
	}

	var preferredIndex *Index

	if len(stmt.indexOn) > 0 {
		cols := make([]*Column, len(stmt.indexOn))

		for i, colName := range stmt.indexOn {
			col, err := table.GetColumnByName(colName)
			if err != nil {
				return nil, err
			}

			cols[i] = col
		}

		index, err := table.GetIndexByName(indexName(table.name, cols))
		if err != nil {
			return nil, err
		}

		preferredIndex = index
	}

	var sortingIndex *Index
	var descOrder bool

	if stmt.orderBy == nil {
		if preferredIndex == nil {
			sortingIndex = table.primaryIndex
		} else {
			sortingIndex = preferredIndex
		}
	}

	if len(stmt.orderBy) > 0 {
		col, err := table.GetColumnByName(stmt.orderBy[0].sel.col)
		if err != nil {
			return nil, err
		}

		for _, idx := range table.indexesByColID[col.id] {
			if idx.sortableUsing(col.id, rangesByColID) {
				if preferredIndex == nil || idx.id == preferredIndex.id {
					sortingIndex = idx
					break
				}
			}
		}

		descOrder = stmt.orderBy[0].descOrder
	}

	if sortingIndex == nil {
		return nil, ErrNoAvailableIndex
	}

	if tableRef.history && !sortingIndex.IsPrimary() {
		return nil, fmt.Errorf("%w: historical queries are supported over primary index", ErrIllegalArguments)
	}

	return &ScanSpecs{
		Index:          sortingIndex,
		rangesByColID:  rangesByColID,
		IncludeHistory: tableRef.history,
		DescOrder:      descOrder,
	}, nil
}

type UnionStmt struct {
	distinct    bool
	left, right DataSource
}

func (stmt *UnionStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	err := stmt.left.inferParameters(ctx, tx, params)
	if err != nil {
		return err
	}

	return stmt.right.inferParameters(ctx, tx, params)
}

func (stmt *UnionStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	_, err := stmt.left.execAt(ctx, tx, params)
	if err != nil {
		return tx, err
	}

	return stmt.right.execAt(ctx, tx, params)
}

func (stmt *UnionStmt) resolveUnionAll(ctx context.Context, tx *SQLTx, params map[string]interface{}) (ret RowReader, err error) {
	leftRowReader, err := stmt.left.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			leftRowReader.Close()
		}
	}()

	rightRowReader, err := stmt.right.Resolve(ctx, tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rightRowReader.Close()
		}
	}()

	rowReader, err := newUnionRowReader(ctx, []RowReader{leftRowReader, rightRowReader})
	if err != nil {
		return nil, err
	}

	return rowReader, nil
}

func (stmt *UnionStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (ret RowReader, err error) {
	rowReader, err := stmt.resolveUnionAll(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			rowReader.Close()
		}
	}()

	if stmt.distinct {
		distinctReader, err := newDistinctRowReader(ctx, rowReader)
		if err != nil {
			return nil, err
		}
		rowReader = distinctReader
	}

	return rowReader, nil
}

func (stmt *UnionStmt) Alias() string {
	return ""
}

func NewTableRef(table string, as string) *tableRef {
	return &tableRef{
		table: table,
		as:    as,
	}
}

type tableRef struct {
	table   string
	history bool
	period  period
	as      string
}

type period struct {
	start *openPeriod
	end   *openPeriod
}

type openPeriod struct {
	inclusive bool
	instant   periodInstant
}

type periodInstant struct {
	exp         ValueExp
	instantType instantType
}

type instantType = int

const (
	txInstant instantType = iota
	timeInstant
)

func (i periodInstant) resolve(tx *SQLTx, params map[string]interface{}, asc, inclusive bool) (uint64, error) {
	exp, err := i.exp.substitute(params)
	if err != nil {
		return 0, err
	}

	instantVal, err := exp.reduce(tx, nil, "")
	if err != nil {
		return 0, err
	}

	if i.instantType == txInstant {
		txID, ok := instantVal.RawValue().(int64)
		if !ok {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be a positive integer, %s given", ErrIllegalArguments, instantVal.Type())
		}

		if txID <= 0 {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be a positive integer, %d given", ErrIllegalArguments, txID)
		}

		if inclusive {
			return uint64(txID), nil
		}

		if asc {
			return uint64(txID + 1), nil
		}

		if txID <= 1 {
			return 0, fmt.Errorf("%w: invalid tx range, tx ID must be greater than 1, %d given", ErrIllegalArguments, txID)
		}

		return uint64(txID - 1), nil
	} else {

		var ts time.Time

		if instantVal.Type() == TimestampType {
			ts = instantVal.RawValue().(time.Time)
		} else {
			conv, err := getConverter(instantVal.Type(), TimestampType)
			if err != nil {
				return 0, err
			}

			tval, err := conv(instantVal)
			if err != nil {
				return 0, err
			}

			ts = tval.RawValue().(time.Time)
		}

		sts := ts

		if asc {
			if !inclusive {
				sts = sts.Add(1 * time.Second)
			}

			txHdr, err := tx.engine.store.FirstTxSince(sts)
			if err != nil {
				return 0, err
			}

			return txHdr.ID, nil
		}

		if !inclusive {
			sts = sts.Add(-1 * time.Second)
		}

		txHdr, err := tx.engine.store.LastTxUntil(sts)
		if err != nil {
			return 0, err
		}

		return txHdr.ID, nil
	}
}

func (stmt *tableRef) referencedTable(tx *SQLTx) (*Table, error) {
	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (stmt *tableRef) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *tableRef) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *tableRef) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	if tx == nil {
		return nil, ErrIllegalArguments
	}

	table, err := stmt.referencedTable(tx)
	if err != nil {
		return nil, err
	}

	return newRawRowReader(tx, params, table, stmt.period, stmt.as, scanSpecs)
}

func (stmt *tableRef) Alias() string {
	if stmt.as == "" {
		return stmt.table
	}
	return stmt.as
}

type JoinSpec struct {
	joinType JoinType
	ds       DataSource
	cond     ValueExp
	indexOn  []string
}

type OrdCol struct {
	sel       *ColSelector
	descOrder bool
}

func NewOrdCol(table string, col string, descOrder bool) *OrdCol {
	return &OrdCol{
		sel:       NewColSelector(table, col),
		descOrder: descOrder,
	}
}

type Selector interface {
	ValueExp
	resolve(implicitTable string) (aggFn, table, col string)
	alias() string
	setAlias(alias string)
}

type ColSelector struct {
	table string
	col   string
	as    string
}

func NewColSelector(table, col string) *ColSelector {
	return &ColSelector{
		table: table,
		col:   col,
	}
}

func (sel *ColSelector) resolve(implicitTable string) (aggFn, table, col string) {
	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return "", table, sel.col
}

func (sel *ColSelector) alias() string {
	if sel.as == "" {
		return sel.col
	}

	return sel.as
}

func (sel *ColSelector) setAlias(alias string) {
	sel.as = alias
}

func (sel *ColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	_, table, col := sel.resolve(implicitTable)
	encSel := EncodeSelector("", table, col)

	desc, ok := cols[encSel]
	if !ok {
		return AnyType, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	return desc.Type, nil
}

func (sel *ColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	_, table, col := sel.resolve(implicitTable)
	encSel := EncodeSelector("", table, col)

	desc, ok := cols[encSel]
	if !ok {
		return fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	if desc.Type != t {
		return fmt.Errorf("%w: %v(%s) can not be interpreted as type %v", ErrInvalidTypes, desc.Type, encSel, t)
	}

	return nil
}

func (sel *ColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *ColSelector) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate in current context", ErrInvalidValue)
	}

	aggFn, table, col := sel.resolve(implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, table, col)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	return v, nil
}

func (sel *ColSelector) reduceSelectors(row *Row, implicitTable string) ValueExp {
	aggFn, table, col := sel.resolve(implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, table, col)]
	if !ok {
		return sel
	}

	return v
}

func (sel *ColSelector) isConstant() bool {
	return false
}

func (sel *ColSelector) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type AggColSelector struct {
	aggFn AggregateFn
	table string
	col   string
	as    string
}

func NewAggColSelector(aggFn AggregateFn, table, col string) *AggColSelector {
	return &AggColSelector{
		aggFn: aggFn,
		table: table,
		col:   col,
	}
}

func EncodeSelector(aggFn, table, col string) string {
	return aggFn + "(" + table + "." + col + ")"
}

func (sel *AggColSelector) resolve(implicitTable string) (aggFn, table, col string) {
	table = implicitTable
	if sel.table != "" {
		table = sel.table
	}

	return sel.aggFn, table, sel.col
}

func (sel *AggColSelector) alias() string {
	return sel.as
}

func (sel *AggColSelector) setAlias(alias string) {
	sel.as = alias
}

func (sel *AggColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if sel.aggFn == COUNT {
		return IntegerType, nil
	}

	colSelector := &ColSelector{table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		t, err := colSelector.inferType(cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}

		if t != IntegerType && t != Float64Type {
			return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, t)

		}

		return t, nil
	}

	return colSelector.inferType(cols, params, implicitTable)
}

func (sel *AggColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if sel.aggFn == COUNT {
		if t != IntegerType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
		}
		return nil
	}

	colSelector := &ColSelector{table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		if t != IntegerType && t != Float64Type {
			return fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, t)
		}
	}

	return colSelector.requiresType(t, cols, params, implicitTable)
}

func (sel *AggColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *AggColSelector) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate aggregation (%s) in current context", ErrInvalidValue, sel.aggFn)
	}

	v, ok := row.ValuesBySelector[EncodeSelector(sel.resolve(implicitTable))]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, sel.col)
	}
	return v, nil
}

func (sel *AggColSelector) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return sel
}

func (sel *AggColSelector) isConstant() bool {
	return false
}

func (sel *AggColSelector) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type NumExp struct {
	op          NumOperator
	left, right ValueExp
}

func (bexp *NumExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	// First step - check if we can infer the type of sub-expressions
	tleft, err := bexp.left.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}
	if tleft != AnyType && tleft != IntegerType && tleft != Float64Type {
		return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, tleft)
	}

	tright, err := bexp.right.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}
	if tright != AnyType && tright != IntegerType && tright != Float64Type {
		return AnyType, fmt.Errorf("%w: %v or %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, Float64Type, tright)
	}

	if tleft == IntegerType && tright == IntegerType {
		// Both sides are integer types - the result is also integer
		return IntegerType, nil
	}

	if tleft != AnyType && tright != AnyType {
		// Both sides have concrete types but at least one of them is float
		return Float64Type, nil
	}

	// Both sides are ambiguous
	return AnyType, nil
}

func copyParams(params map[string]SQLValueType) map[string]SQLValueType {
	ret := make(map[string]SQLValueType, len(params))
	for k, v := range params {
		ret[k] = v
	}
	return ret
}

func restoreParams(params, restore map[string]SQLValueType) {
	for k := range params {
		delete(params, k)
	}
	for k, v := range restore {
		params[k] = v
	}
}

func (bexp *NumExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != IntegerType && t != Float64Type {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	floatArgs := 2
	paramsOrig := copyParams(params)
	err := bexp.left.requiresType(t, cols, params, implicitTable)
	if err != nil && t == Float64Type {
		restoreParams(params, paramsOrig)
		floatArgs--
		err = bexp.left.requiresType(IntegerType, cols, params, implicitTable)
	}
	if err != nil {
		return err
	}

	paramsOrig = copyParams(params)
	err = bexp.right.requiresType(t, cols, params, implicitTable)
	if err != nil && t == Float64Type {
		restoreParams(params, paramsOrig)
		floatArgs--
		err = bexp.right.requiresType(IntegerType, cols, params, implicitTable)
	}
	if err != nil {
		return err
	}

	if t == Float64Type && floatArgs == 0 {
		// Currently this case requires explicit float cast
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	return nil
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

func (bexp *NumExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	return applyNumOperator(bexp.op, vl, vr)
}

func (bexp *NumExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &NumExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *NumExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *NumExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type NotBoolExp struct {
	exp ValueExp
}

func (bexp *NotBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	err := bexp.exp.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *NotBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	return bexp.exp.requiresType(BooleanType, cols, params, implicitTable)
}

func (bexp *NotBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	rexp, err := bexp.exp.substitute(params)
	if err != nil {
		return nil, err
	}

	bexp.exp = rexp

	return bexp, nil
}

func (bexp *NotBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	v, err := bexp.exp.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	r, isBool := v.RawValue().(bool)
	if !isBool {
		return nil, ErrInvalidCondition
	}

	return &Bool{val: !r}, nil
}

func (bexp *NotBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &NotBoolExp{
		exp: bexp.exp.reduceSelectors(row, implicitTable),
	}
}

func (bexp *NotBoolExp) isConstant() bool {
	return bexp.exp.isConstant()
}

func (bexp *NotBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type LikeBoolExp struct {
	val     ValueExp
	notLike bool
	pattern ValueExp
}

func NewLikeBoolExp(val ValueExp, notLike bool, pattern ValueExp) *LikeBoolExp {
	return &LikeBoolExp{
		val:     val,
		notLike: notLike,
		pattern: pattern,
	}
}

func (bexp *LikeBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return BooleanType, nil
}

func (bexp *LikeBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if bexp.val == nil || bexp.pattern == nil {
		return fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	if t != BooleanType {
		return fmt.Errorf("error using the value of the LIKE operator as %s: %w", t, ErrInvalidTypes)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitTable)
	if err != nil {
		return fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return nil
}

func (bexp *LikeBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	val, err := bexp.val.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	pattern, err := bexp.pattern.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return &LikeBoolExp{
		val:     val,
		notLike: bexp.notLike,
		pattern: pattern,
	}, nil
}

func (bexp *LikeBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	rval, err := bexp.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rval.Type() != VarcharType {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w (expecting %s)", ErrInvalidTypes, VarcharType)
	}

	if rval.IsNull() {
		return &Bool{val: false}, nil
	}

	rpattern, err := bexp.pattern.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rpattern.Type() != VarcharType {
		return nil, fmt.Errorf("error evaluating 'LIKE' clause: %w", ErrInvalidTypes)
	}

	matched, err := regexp.MatchString(rpattern.RawValue().(string), rval.RawValue().(string))
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return &Bool{val: matched != bexp.notLike}, nil
}

func (bexp *LikeBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *LikeBoolExp) isConstant() bool {
	return false
}

func (bexp *LikeBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type CmpBoolExp struct {
	op          CmpOperator
	left, right ValueExp
}

func NewCmpBoolExp(op CmpOperator, left, right ValueExp) *CmpBoolExp {
	return &CmpBoolExp{
		op:    op,
		left:  left,
		right: right,
	}
}

func (bexp *CmpBoolExp) Left() ValueExp {
	return bexp.left
}

func (bexp *CmpBoolExp) Right() ValueExp {
	return bexp.right
}

func (bexp *CmpBoolExp) OP() CmpOperator {
	return bexp.op
}

func (bexp *CmpBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	tleft, err := bexp.left.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	tright, err := bexp.right.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	// unification step

	if tleft == tright {
		return BooleanType, nil
	}

	if tleft != AnyType && tright != AnyType {
		return AnyType, fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, tleft, tright)
	}

	if tleft == AnyType {
		err = bexp.left.requiresType(tright, cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}

	if tright == AnyType {
		err = bexp.right.requiresType(tleft, cols, params, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}

	return BooleanType, nil
}

func (bexp *CmpBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	_, err := bexp.inferType(cols, params, implicitTable)

	return err
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

func (bexp *CmpBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	r, err := vl.Compare(vr)
	if err != nil {
		return nil, err
	}

	return &Bool{val: cmpSatisfiesOp(r, bexp.op)}, nil
}

func (bexp *CmpBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &CmpBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *CmpBoolExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *CmpBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	matchingFunc := func(left, right ValueExp) (*ColSelector, ValueExp, bool) {
		s, isSel := bexp.left.(*ColSelector)
		if isSel && s.col != revCol && bexp.right.isConstant() {
			return s, right, true
		}
		return nil, nil, false
	}

	sel, c, ok := matchingFunc(bexp.left, bexp.right)
	if !ok {
		sel, c, ok = matchingFunc(bexp.right, bexp.left)
	}

	if !ok {
		return nil
	}

	aggFn, t, col := sel.resolve(table.name)
	if aggFn != "" || t != asTable {
		return nil
	}

	column, err := table.GetColumnByName(col)
	if err != nil {
		return err
	}

	val, err := c.substitute(params)
	if errors.Is(err, ErrMissingParameter) {
		// TODO: not supported when parameters are not provided during query resolution
		return nil
	}
	if err != nil {
		return err
	}

	rval, err := val.reduce(nil, nil, table.name)
	if err != nil {
		return err
	}

	return updateRangeFor(column.id, rval, bexp.op, rangesByColID)
}

func updateRangeFor(colID uint32, val TypedValue, cmp CmpOperator, rangesByColID map[uint32]*typedValueRange) error {
	currRange, ranged := rangesByColID[colID]
	var newRange *typedValueRange

	switch cmp {
	case EQ:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
				hRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case LT:
		{
			newRange = &typedValueRange{
				hRange: &typedValueSemiRange{
					val: val,
				},
			}
		}
	case LE:
		{
			newRange = &typedValueRange{
				hRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case GT:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val: val,
				},
			}
		}
	case GE:
		{
			newRange = &typedValueRange{
				lRange: &typedValueSemiRange{
					val:       val,
					inclusive: true,
				},
			}
		}
	case NE:
		{
			return nil
		}
	}

	if !ranged {
		rangesByColID[colID] = newRange
		return nil
	}

	return currRange.refineWith(newRange)
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

func NewBinBoolExp(op LogicOperator, lrexp, rrexp ValueExp) *BinBoolExp {
	bexp := &BinBoolExp{
		op: op,
	}

	bexp.left = lrexp
	bexp.right = rrexp

	return bexp
}

func (bexp *BinBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	err := bexp.left.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *BinBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	err := bexp.left.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitTable)
	if err != nil {
		return err
	}

	return nil
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

func (bexp *BinBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, err
	}

	bl, isBool := vl.(*Bool)
	if !isBool {
		return nil, fmt.Errorf("%w (expecting boolean value)", ErrInvalidValue)
	}

	br, isBool := vr.(*Bool)
	if !isBool {
		return nil, fmt.Errorf("%w (expecting boolean value)", ErrInvalidValue)
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

func (bexp *BinBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return &BinBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitTable),
	}
}

func (bexp *BinBoolExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *BinBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	if bexp.op == AND {
		err := bexp.left.selectorRanges(table, asTable, params, rangesByColID)
		if err != nil {
			return err
		}

		return bexp.right.selectorRanges(table, asTable, params, rangesByColID)
	}

	lRanges := make(map[uint32]*typedValueRange)
	rRanges := make(map[uint32]*typedValueRange)

	err := bexp.left.selectorRanges(table, asTable, params, lRanges)
	if err != nil {
		return err
	}

	err = bexp.right.selectorRanges(table, asTable, params, rRanges)
	if err != nil {
		return err
	}

	for colID, lr := range lRanges {
		rr, ok := rRanges[colID]
		if !ok {
			continue
		}

		err = lr.extendWith(rr)
		if err != nil {
			return err
		}

		rangesByColID[colID] = lr
	}

	return nil
}

type ExistsBoolExp struct {
	q DataSource
}

func (bexp *ExistsBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return AnyType, errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *ExistsBoolExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *ExistsBoolExp) isConstant() bool {
	return false
}

func (bexp *ExistsBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type InSubQueryExp struct {
	val   ValueExp
	notIn bool
	q     *SelectStmt
}

func (bexp *InSubQueryExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	return fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *InSubQueryExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	return nil, fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	return bexp
}

func (bexp *InSubQueryExp) isConstant() bool {
	return false
}

func (bexp *InSubQueryExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

// TODO: once InSubQueryExp is supported, this struct may become obsolete by creating a ListDataSource struct
type InListExp struct {
	val    ValueExp
	notIn  bool
	values []ValueExp
}

func (bexp *InListExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) (SQLValueType, error) {
	t, err := bexp.val.inferType(cols, params, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
	}

	for _, v := range bexp.values {
		err = v.requiresType(t, cols, params, implicitTable)
		if err != nil {
			return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
		}
	}

	return BooleanType, nil
}

func (bexp *InListExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitTable string) error {
	_, err := bexp.inferType(cols, params, implicitTable)
	if err != nil {
		return err
	}

	if t != BooleanType {
		return fmt.Errorf("error inferring type in 'IN' clause: %w", ErrInvalidTypes)
	}

	return nil
}

func (bexp *InListExp) substitute(params map[string]interface{}) (ValueExp, error) {
	val, err := bexp.val.substitute(params)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	values := make([]ValueExp, len(bexp.values))

	for i, val := range bexp.values {
		values[i], err = val.substitute(params)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}
	}

	return &InListExp{
		val:    val,
		notIn:  bexp.notIn,
		values: values,
	}, nil
}

func (bexp *InListExp) reduce(tx *SQLTx, row *Row, implicitTable string) (TypedValue, error) {
	rval, err := bexp.val.reduce(tx, row, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	var found bool

	for _, v := range bexp.values {
		rv, err := v.reduce(tx, row, implicitTable)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}

		r, err := rval.Compare(rv)
		if err != nil {
			return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
		}

		if r == 0 {
			// TODO: short-circuit evaluation may be preferred when upfront static type inference is in place
			found = found || true
		}
	}

	return &Bool{val: found != bexp.notIn}, nil
}

func (bexp *InListExp) reduceSelectors(row *Row, implicitTable string) ValueExp {
	values := make([]ValueExp, len(bexp.values))

	for i, val := range bexp.values {
		values[i] = val.reduceSelectors(row, implicitTable)
	}

	return &InListExp{
		val:    bexp.val.reduceSelectors(row, implicitTable),
		values: values,
	}
}

func (bexp *InListExp) isConstant() bool {
	return false
}

func (bexp *InListExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	// TODO: may be determiined by smallest and bigggest value in the list
	return nil
}

type FnDataSourceStmt struct {
	fnCall *FnCall
	as     string
}

func (stmt *FnDataSourceStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *FnDataSourceStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *FnDataSourceStmt) Alias() string {
	if stmt.as != "" {
		return stmt.as
	}

	switch strings.ToUpper(stmt.fnCall.fn) {
	case DatabasesFnCall:
		{
			return "databases"
		}
	case TablesFnCall:
		{
			return "tables"
		}
	case TableFnCall:
		{
			return "table"
		}
	case UsersFnCall:
		{
			return "users"
		}
	case ColumnsFnCall:
		{
			return "columns"
		}
	case IndexesFnCall:
		{
			return "indexes"
		}
	}

	// not reachable
	return ""
}

func (stmt *FnDataSourceStmt) Resolve(ctx context.Context, tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (rowReader RowReader, err error) {
	if stmt.fnCall == nil {
		return nil, fmt.Errorf("%w: function is unspecified", ErrIllegalArguments)
	}

	switch strings.ToUpper(stmt.fnCall.fn) {
	case DatabasesFnCall:
		{
			return stmt.resolveListDatabases(ctx, tx, params, scanSpecs)
		}
	case TablesFnCall:
		{
			return stmt.resolveListTables(ctx, tx, params, scanSpecs)
		}
	case TableFnCall:
		{
			return stmt.resolveShowTable(ctx, tx, params, scanSpecs)
		}
	case UsersFnCall:
		{
			return stmt.resolveListUsers(ctx, tx, params, scanSpecs)
		}
	case ColumnsFnCall:
		{
			return stmt.resolveListColumns(ctx, tx, params, scanSpecs)
		}
	case IndexesFnCall:
		{
			return stmt.resolveListIndexes(ctx, tx, params, scanSpecs)
		}
	}

	return nil, fmt.Errorf("%w (%s)", ErrFunctionDoesNotExist, stmt.fnCall.fn)
}

func (stmt *FnDataSourceStmt) resolveListDatabases(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, DatabasesFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 1)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}

	var dbs []string

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	} else {
		dbs, err = tx.engine.multidbHandler.ListDatabases(ctx)
		if err != nil {
			return nil, err
		}
	}

	values := make([][]ValueExp, len(dbs))

	for i, db := range dbs {
		values[i] = []ValueExp{&Varchar{val: db}}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListTables(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, TablesFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 1)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}

	tables := tx.catalog.GetTables()

	values := make([][]ValueExp, len(tables))

	for i, t := range tables {
		values[i] = []ValueExp{&Varchar{val: t.name}}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveShowTable(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	cols := []ColDescriptor{
		{
			Column: "column_name",
			Type:   VarcharType,
		},
		{
			Column: "type_name",
			Type:   VarcharType,
		},
		{
			Column: "is_nullable",
			Type:   BooleanType,
		},
		{
			Column: "is_indexed",
			Type:   VarcharType,
		},
		{
			Column: "is_auto_increment",
			Type:   BooleanType,
		},
		{
			Column: "is_unique",
			Type:   BooleanType,
		},
	}

	tableName, _ := stmt.fnCall.params[0].reduce(tx, nil, "")
	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.cols))

	for i, c := range table.cols {
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
		for _, index := range table.GetIndexesByColID(c.ID()) {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		var maxLen string

		if c.MaxLen() > 0 && (c.Type() == VarcharType || c.Type() == BLOBType) {
			maxLen = fmt.Sprintf("(%d)", c.MaxLen())
		}

		values[i] = []ValueExp{
			&Varchar{val: c.colName},
			&Varchar{val: c.Type() + maxLen},
			&Bool{val: c.IsNullable()},
			&Varchar{val: index},
			&Bool{val: c.IsAutoIncremental()},
			&Bool{val: unique},
		}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListUsers(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, UsersFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 2)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}
	cols[1] = ColDescriptor{
		Column: "permission",
		Type:   VarcharType,
	}

	var users []User

	if tx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	} else {
		users, err = tx.engine.multidbHandler.ListUsers(ctx)
		if err != nil {
			return nil, err
		}
	}

	values := make([][]ValueExp, len(users))

	for i, user := range users {
		var perm string

		switch user.Permission() {
		case 1:
			{
				perm = "READ"
			}
		case 2:
			{
				perm = "READ/WRITE"
			}
		case 254:
			{
				perm = "ADMIN"
			}
		default:
			{
				perm = "SYSADMIN"
			}
		}

		values[i] = []ValueExp{
			&Varchar{val: user.Username()},
			&Varchar{val: perm},
		}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListColumns(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	if len(stmt.fnCall.params) != 1 {
		return nil, fmt.Errorf("%w: function '%s' expect table name as parameter", ErrIllegalArguments, ColumnsFnCall)
	}

	cols := []ColDescriptor{
		{
			Column: "table",
			Type:   VarcharType,
		},
		{
			Column: "name",
			Type:   VarcharType,
		},
		{
			Column: "type",
			Type:   VarcharType,
		},
		{
			Column: "max_length",
			Type:   IntegerType,
		},
		{
			Column: "nullable",
			Type:   BooleanType,
		},
		{
			Column: "auto_increment",
			Type:   BooleanType,
		},
		{
			Column: "indexed",
			Type:   BooleanType,
		},
		{
			Column: "primary",
			Type:   BooleanType,
		},
		{
			Column: "unique",
			Type:   BooleanType,
		},
	}

	val, err := stmt.fnCall.params[0].substitute(params)
	if err != nil {
		return nil, err
	}

	tableName, err := val.reduce(tx, nil, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.cols))

	for i, c := range table.cols {
		indexed, err := table.IsIndexed(c.Name())
		if err != nil {
			return nil, err
		}

		var unique bool
		for _, index := range table.indexesByColID[c.id] {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		values[i] = []ValueExp{
			&Varchar{val: table.name},
			&Varchar{val: c.colName},
			&Varchar{val: c.colType},
			&Integer{val: int64(c.MaxLen())},
			&Bool{val: c.IsNullable()},
			&Bool{val: c.autoIncrement},
			&Bool{val: indexed},
			&Bool{val: table.PrimaryIndex().IncludesCol(c.ID())},
			&Bool{val: unique},
		}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListIndexes(ctx context.Context, tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (RowReader, error) {
	if len(stmt.fnCall.params) != 1 {
		return nil, fmt.Errorf("%w: function '%s' expect table name as parameter", ErrIllegalArguments, IndexesFnCall)
	}

	cols := []ColDescriptor{
		{
			Column: "table",
			Type:   VarcharType,
		},
		{
			Column: "name",
			Type:   VarcharType,
		},
		{
			Column: "unique",
			Type:   BooleanType,
		},
		{
			Column: "primary",
			Type:   BooleanType,
		},
	}

	val, err := stmt.fnCall.params[0].substitute(params)
	if err != nil {
		return nil, err
	}

	tableName, err := val.reduce(tx, nil, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.catalog.GetTableByName(tableName.RawValue().(string))
	if err != nil {
		return nil, err
	}

	values := make([][]ValueExp, len(table.indexes))

	for i, index := range table.indexes {
		values[i] = []ValueExp{
			&Varchar{val: table.name},
			&Varchar{val: index.Name()},
			&Bool{val: index.unique},
			&Bool{val: index.IsPrimary()},
		}
	}

	return newValuesRowReader(tx, params, cols, stmt.Alias(), values)
}

// DropTableStmt represents a statement to delete a table.
type DropTableStmt struct {
	table string
}

func NewDropTableStmt(table string) *DropTableStmt {
	return &DropTableStmt{table: table}
}

func (stmt *DropTableStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

/*
Exec executes the delete table statement.
It the table exists, if not it does nothing.
If the table exists, it deletes all the indexes and the table itself.
Note that this is a soft delete of the index and table key,
the data is not deleted, but the metadata is updated.
*/
func (stmt *DropTableStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.catalog.ExistTable(stmt.table) {
		return nil, ErrTableDoesNotExist
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	// delete table
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogTablePrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
	)
	err = tx.delete(ctx, mappedKey)
	if err != nil {
		return nil, err
	}

	// delete columns
	cols := table.ColumnsByID()
	for _, col := range cols {
		mappedKey := MapKey(
			tx.sqlPrefix(),
			catalogColumnPrefix,
			EncodeID(DatabaseID),
			EncodeID(col.table.id),
			EncodeID(col.id),
			[]byte(col.colType),
		)
		err = tx.delete(ctx, mappedKey)
		if err != nil {
			return nil, err
		}
	}

	// delete indexes
	for _, index := range table.indexes {
		mappedKey := MapKey(
			tx.sqlPrefix(),
			catalogIndexPrefix,
			EncodeID(DatabaseID),
			EncodeID(table.id),
			EncodeID(index.id),
		)
		err = tx.delete(ctx, mappedKey)
		if err != nil {
			return nil, err
		}

		indexKey := MapKey(
			tx.sqlPrefix(),
			MappedPrefix,
			EncodeID(table.id),
			EncodeID(index.id),
		)
		err = tx.addOnCommittedCallback(func(sqlTx *SQLTx) error {
			return sqlTx.engine.store.DeleteIndex(indexKey)
		})
		if err != nil {
			return nil, err
		}
	}

	err = tx.catalog.deleteTable(table)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}

// DropIndexStmt represents a statement to delete a table.
type DropIndexStmt struct {
	table string
	cols  []string
}

func NewDropIndexStmt(table string, cols []string) *DropIndexStmt {
	return &DropIndexStmt{table: table, cols: cols}
}

func (stmt *DropIndexStmt) inferParameters(ctx context.Context, tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

/*
Exec executes the delete index statement.
If the index exists, it deletes it. Note that this is a soft delete of the index
the data is not deleted, but the metadata is updated.
*/
func (stmt *DropIndexStmt) execAt(ctx context.Context, tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.catalog.ExistTable(stmt.table) {
		return nil, ErrTableDoesNotExist
	}

	table, err := tx.catalog.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	cols := make([]*Column, len(stmt.cols))

	for i, colName := range stmt.cols {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		cols[i] = col
	}

	index, err := table.GetIndexByName(indexName(table.name, cols))
	if err != nil {
		return nil, err
	}

	// delete index
	mappedKey := MapKey(
		tx.sqlPrefix(),
		catalogIndexPrefix,
		EncodeID(DatabaseID),
		EncodeID(table.id),
		EncodeID(index.id),
	)
	err = tx.delete(ctx, mappedKey)
	if err != nil {
		return nil, err
	}

	indexKey := MapKey(
		tx.sqlPrefix(),
		MappedPrefix,
		EncodeID(table.id),
		EncodeID(index.id),
	)

	err = tx.addOnCommittedCallback(func(sqlTx *SQLTx) error {
		return sqlTx.engine.store.DeleteIndex(indexKey)
	})
	if err != nil {
		return nil, err
	}

	err = table.deleteIndex(index)
	if err != nil {
		return nil, err
	}

	tx.mutatedCatalog = true

	return tx, nil
}
