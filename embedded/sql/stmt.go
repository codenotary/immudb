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

package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

const (
	catalogDatabasePrefix = "CTL.DATABASE." // (key=CTL.DATABASE.{dbID}, value={dbNAME})
	catalogTablePrefix    = "CTL.TABLE."    // (key=CTL.TABLE.{dbID}{tableID}, value={tableNAME})
	catalogColumnPrefix   = "CTL.COLUMN."   // (key=CTL.COLUMN.{dbID}{tableID}{colID}{colTYPE}, value={(auto_incremental | nullable){maxLen}{colNAME}})
	catalogIndexPrefix    = "CTL.INDEX."    // (key=CTL.INDEX.{dbID}{tableID}{indexID}, value={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)})
	PIndexPrefix          = "R."            // (key=R.{dbID}{tableID}{0}({null}({pkVal}{padding}{pkValLen})?)+, value={count (colID valLen val)+})
	SIndexPrefix          = "E."            // (key=E.{dbID}{tableID}{indexID}({null}({val}{padding}{valLen})?)+({pkVal}{padding}{pkValLen})+, value={})
	UIndexPrefix          = "N."            // (key=N.{dbID}{tableID}{indexID}({null}({val}{padding}{valLen})?)+, value={({pkVal}{padding}{pkValLen})+})

	// Old prefixes that must not be reused:
	//  `CATALOG.DATABASE.`
	//  `CATALOG.TABLE.`
	//  `CATALOG.COLUMN.`
	//  `CATALOG.INDEX.`
	//  `P.` 				primary indexes without null support
	//  `S.` 				secondary indexes without null support
	//  `U.` 				secondary unique indexes without null support
	//  `PINDEX.` 			single-column primary indexes
	//  `SINDEX.` 			single-column secondary indexes
	//  `UINDEX.` 			single-column secondary unique indexes
)

const PKIndexID = uint32(0)

const (
	nullableFlag      byte = 1 << iota
	autoIncrementFlag byte = 1 << iota
)

type SQLValueType = string

const (
	IntegerType   SQLValueType = "INTEGER"
	BooleanType   SQLValueType = "BOOLEAN"
	VarcharType   SQLValueType = "VARCHAR"
	BLOBType      SQLValueType = "BLOB"
	TimestampType SQLValueType = "TIMESTAMP"
	AnyType       SQLValueType = "ANY"
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
	DatabasesFnCall string = "DATABASES"
	TablesFnCall    string = "TABLES"
	ColumnsFnCall   string = "COLUMNS"
	IndexesFnCall   string = "INDEXES"
)

type SQLStmt interface {
	execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error)
	inferParameters(tx *SQLTx, params map[string]SQLValueType) error
}

type BeginTransactionStmt struct {
}

func (stmt *BeginTransactionStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *BeginTransactionStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.explicitClose {
		return nil, ErrNestedTxNotSupported
	}

	if tx.updatedRows == 0 {
		tx.explicitClose = true
		return tx, nil
	}

	// explicit tx initialization with implicit tx in progress

	err := tx.commit()
	if err != nil {
		return nil, err
	}

	ntx, err := tx.engine.NewTx(tx.ctx)
	if err != nil {
		return nil, err
	}

	ntx.explicitClose = true
	return ntx, nil
}

type CommitStmt struct {
}

func (stmt *CommitStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CommitStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.explicitClose {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.commit()
}

type RollbackStmt struct {
}

func (stmt *RollbackStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RollbackStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if !tx.explicitClose {
		return nil, ErrNoOngoingTx
	}

	return nil, tx.Cancel()
}

type CreateDatabaseStmt struct {
	DB          string
	ifNotExists bool
}

func (stmt *CreateDatabaseStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateDatabaseStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.explicitClose {
		return nil, fmt.Errorf("%w: database creation can not be done within a transaction", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler != nil {
		return nil, tx.engine.multidbHandler.CreateDatabase(tx.ctx, stmt.DB, stmt.ifNotExists)
	}

	id := uint32(len(tx.catalog.dbsByID) + 1)

	db, err := tx.catalog.newDatabase(id, stmt.DB)
	if err == ErrDatabaseAlreadyExists && stmt.ifNotExists {
		return tx, nil
	}
	if err != nil {
		return nil, err
	}

	err = tx.set(mapKey(tx.sqlPrefix(), catalogDatabasePrefix, EncodeID(db.id)), nil, []byte(stmt.DB))
	if err != nil {
		return nil, err
	}

	return tx, nil
}

type UseDatabaseStmt struct {
	DB string
}

func (stmt *UseDatabaseStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseDatabaseStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if stmt.DB == "" {
		return nil, fmt.Errorf("%w: no database name was provided", ErrIllegalArguments)
	}

	if tx.explicitClose {
		return nil, fmt.Errorf("%w: database selection can NOT be executed within a transaction block", ErrNonTransactionalStmt)
	}

	if tx.engine.multidbHandler != nil {
		return tx, tx.engine.multidbHandler.UseDatabase(tx.ctx, stmt.DB)
	}

	_, exists := tx.catalog.dbsByName[stmt.DB]
	if !exists {
		return nil, ErrDatabaseDoesNotExist
	}

	tx.engine.mutex.Lock()
	tx.engine.currentDatabase = stmt.DB
	tx.engine.mutex.Unlock()

	return tx, tx.useDatabase(stmt.DB)
}

type UseSnapshotStmt struct {
	period period
}

func (stmt *UseSnapshotStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseSnapshotStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return nil, ErrNoSupported
}

func persistColumn(col *Column, tx *SQLTx) error {
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

	mappedKey := mapKey(
		tx.sqlPrefix(),
		catalogColumnPrefix,
		EncodeID(col.table.db.id),
		EncodeID(col.table.id),
		EncodeID(col.id),
		[]byte(col.colType),
	)

	return tx.set(mappedKey, nil, v)
}

type CreateTableStmt struct {
	table       string
	ifNotExists bool
	colsSpec    []*ColSpec
	pkColNames  []string
}

func (stmt *CreateTableStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateTableStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	if stmt.ifNotExists && tx.currentDB.ExistTable(stmt.table) {
		return tx, nil
	}

	table, err := tx.currentDB.newTable(stmt.table, stmt.colsSpec)
	if err != nil {
		return nil, err
	}

	createIndexStmt := &CreateIndexStmt{unique: true, table: table.name, cols: stmt.pkColNames}
	_, err = createIndexStmt.execAt(tx, params)
	if err != nil {
		return nil, err
	}

	for _, col := range table.Cols() {
		if col.autoIncrement {
			if len(table.primaryIndex.cols) > 1 || col.id != table.primaryIndex.cols[0].id {
				return nil, ErrLimitedAutoIncrement
			}
		}

		err := persistColumn(col, tx)
		if err != nil {
			return nil, err
		}
	}

	mappedKey := mapKey(tx.sqlPrefix(), catalogTablePrefix, EncodeID(tx.currentDB.id), EncodeID(table.id))

	err = tx.set(mappedKey, nil, []byte(table.name))
	if err != nil {
		return nil, err
	}

	return tx, nil
}

type ColSpec struct {
	colName       string
	colType       SQLValueType
	maxLen        int
	autoIncrement bool
	notNull       bool
}

type CreateIndexStmt struct {
	unique      bool
	ifNotExists bool
	table       string
	cols        []string
}

func (stmt *CreateIndexStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateIndexStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if len(stmt.cols) < 1 {
		return nil, ErrIllegalArguments
	}

	if len(stmt.cols) > MaxNumberOfColumnsInIndex {
		return nil, ErrMaxNumberOfColumnsInIndexExceeded
	}

	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	table, err := tx.currentDB.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	colIDs := make([]uint32, len(stmt.cols))

	for i, colName := range stmt.cols {
		col, err := table.GetColumnByName(colName)
		if err != nil {
			return nil, err
		}

		if variableSized(col.colType) && (col.MaxLen() == 0 || col.MaxLen() > maxKeyLen) {
			return nil, ErrLimitedKeyType
		}

		colIDs[i] = col.id
	}

	index, err := table.newIndex(stmt.unique, colIDs)
	if err == ErrIndexAlreadyExists && stmt.ifNotExists {
		return tx, nil
	}
	if err != nil {
		return nil, err
	}

	// check table is empty
	{
		pkPrefix := mapKey(tx.sqlPrefix(), PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(PKIndexID))
		existKey, err := tx.existKeyWith(pkPrefix, pkPrefix)
		if err != nil {
			return nil, err
		}
		if existKey {
			return nil, ErrLimitedIndexCreation
		}
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

	mappedKey := mapKey(tx.sqlPrefix(), catalogIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(index.id))

	err = tx.set(mappedKey, nil, encodedValues)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

type AddColumnStmt struct {
	table   string
	colSpec *ColSpec
}

func (stmt *AddColumnStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *AddColumnStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	table, err := tx.currentDB.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.newColumn(stmt.colSpec)
	if err != nil {
		return nil, err
	}

	err = persistColumn(col, tx)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

type RenameColumnStmt struct {
	table   string
	oldName string
	newName string
}

func (stmt *RenameColumnStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *RenameColumnStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	table, err := tx.currentDB.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	col, err := table.renameColumn(stmt.oldName, stmt.newName)
	if err != nil {
		return nil, err
	}

	err = persistColumn(col, tx)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

type UpsertIntoStmt struct {
	isInsert   bool
	tableRef   *tableRef
	cols       []string
	rows       []*RowSpec
	onConflict *OnConflictDo
}

type RowSpec struct {
	Values []ValueExp
}

type OnConflictDo struct {
}

func (stmt *UpsertIntoStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	if tx.currentDB == nil {
		return ErrNoDatabaseSelected
	}

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

			err = val.requiresType(col.colType, make(map[string]ColDescriptor), params, tx.currentDB.name, table.name)
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

func (stmt *UpsertIntoStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

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
					valuesByColID[pkCol.id] = &Number{val: table.maxPK}

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

			rval, err := val.reduce(tx.catalog, nil, tx.currentDB.name, table.name)
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
				nl, isNumber := rval.Value().(int64)
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

		pkEncVals, err := encodedPK(table, valuesByColID)
		if err != nil {
			return nil, err
		}

		// primary index entry
		mkey := mapKey(tx.sqlPrefix(), PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals)

		_, err = tx.get(mkey)
		if err != nil && err != store.ErrKeyNotFound {
			return nil, err
		}

		if err == store.ErrKeyNotFound && pkMustExist {
			return nil, fmt.Errorf("%w: specified value must be greater than current one", ErrInvalidValue)
		}

		if stmt.isInsert {
			if err == nil && stmt.onConflict == nil {
				return nil, store.ErrKeyAlreadyExists
			}

			if err == nil && stmt.onConflict != nil {
				// TODO: conflict resolution may be extended. Currently only supports "ON CONFLICT DO NOTHING"
				return tx, nil
			}
		}

		err = tx.doUpsert(pkEncVals, valuesByColID, table, !stmt.isInsert)
		if err != nil {
			return nil, err
		}
	}

	return tx, nil
}

func (tx *SQLTx) doUpsert(pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table, reuseIndex bool) error {
	var reusableIndexEntries map[uint32]struct{}

	if reuseIndex && len(table.indexes) > 1 {
		currPKRow, err := tx.fetchPKRow(table, valuesByColID)
		if err != nil && err != ErrNoMoreRows {
			return err
		}

		if err == nil {
			currValuesByColID := make(map[uint32]TypedValue, len(currPKRow.ValuesBySelector))

			for _, col := range table.cols {
				encSel := EncodeSelector("", table.db.name, table.name, col.colName)
				currValuesByColID[col.id] = currPKRow.ValuesBySelector[encSel]
			}

			reusableIndexEntries, err = tx.deprecateIndexEntries(pkEncVals, currValuesByColID, valuesByColID, table)
			if err != nil {
				return err
			}
		}
	}

	// primary index entry
	mkey := mapKey(tx.sqlPrefix(), PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals)

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
		return err
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
			return err
		}

		encVal, err := EncodeValue(rval.Value(), col.colType, col.MaxLen())
		if err != nil {
			return err
		}

		_, err = valbuf.Write(encVal)
		if err != nil {
			return err
		}
	}

	err = tx.set(mkey, nil, valbuf.Bytes())
	if err != nil {
		return err
	}

	// create entries for secondary indexes
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

		var prefix string
		var encodedValues [][]byte
		var val []byte

		if index.IsUnique() {
			prefix = UIndexPrefix
			encodedValues = make([][]byte, 3+len(index.cols))
			val = pkEncVals
		} else {
			prefix = SIndexPrefix
			encodedValues = make([][]byte, 4+len(index.cols))
			encodedValues[len(encodedValues)-1] = pkEncVals
		}

		encodedValues[0] = EncodeID(table.db.id)
		encodedValues[1] = EncodeID(table.id)
		encodedValues[2] = EncodeID(index.id)

		for i, col := range index.cols {
			rval, specified := valuesByColID[col.id]
			if !specified {
				rval = &NullValue{t: col.colType}
			}

			encVal, err := EncodeAsKey(rval.Value(), col.colType, col.MaxLen())
			if err != nil {
				return err
			}

			encodedValues[i+3] = encVal
		}

		mkey := mapKey(tx.sqlPrefix(), prefix, encodedValues...)

		if index.IsUnique() {
			// mkey must not exist
			_, err := tx.get(mkey)
			if err == nil {
				return store.ErrKeyAlreadyExists
			}
			if err != store.ErrKeyNotFound {
				return err
			}
		}

		err = tx.set(mkey, nil, val)
		if err != nil {
			return err
		}
	}

	tx.updatedRows++

	return nil
}

func encodedPK(table *Table, valuesByColID map[uint32]TypedValue) ([]byte, error) {
	valbuf := bytes.Buffer{}

	for _, col := range table.primaryIndex.cols {
		rval, specified := valuesByColID[col.id]
		if !specified || rval.IsNull() {
			return nil, ErrPKCanNotBeNull
		}

		encVal, err := EncodeAsKey(rval.Value(), col.colType, col.MaxLen())
		if err != nil {
			return nil, err
		}

		_, err = valbuf.Write(encVal)
		if err != nil {
			return nil, err
		}
	}

	return valbuf.Bytes(), nil
}

func (tx *SQLTx) fetchPKRow(table *Table, valuesByColID map[uint32]TypedValue) (*Row, error) {
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

	return r.Read()
}

// deprecateIndexEntries mark previous index entries as deleted
func (tx *SQLTx) deprecateIndexEntries(
	pkEncVals []byte,
	currValuesByColID, newValuesByColID map[uint32]TypedValue,
	table *Table) (reusableIndexEntries map[uint32]struct{}, err error) {

	reusableIndexEntries = make(map[uint32]struct{})

	for _, index := range table.indexes {
		if index.IsPrimary() {
			continue
		}

		var prefix string
		var encodedValues [][]byte

		if index.IsUnique() {
			prefix = UIndexPrefix
			encodedValues = make([][]byte, 3+len(index.cols))
		} else {
			prefix = SIndexPrefix
			encodedValues = make([][]byte, 4+len(index.cols))
			encodedValues[len(encodedValues)-1] = pkEncVals
		}

		encodedValues[0] = EncodeID(table.db.id)
		encodedValues[1] = EncodeID(table.id)
		encodedValues[2] = EncodeID(index.id)

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

			encVal, _ := EncodeAsKey(currVal.Value(), col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		// mark existent index entry as deleted
		if sameIndexKey {
			reusableIndexEntries[index.id] = struct{}{}
		} else {
			md := store.NewKVMetadata()

			md.AsDeleted(true)

			err = tx.set(mapKey(tx.sqlPrefix(), prefix, encodedValues...), md, nil)
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
	limit    int
}

type colUpdate struct {
	col string
	op  CmpOperator
	val ValueExp
}

func (stmt *UpdateStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	if tx.currentDB == nil {
		return ErrNoDatabaseSelected
	}

	selectStmt := &SelectStmt{
		ds:    stmt.tableRef,
		where: stmt.where,
	}

	err := selectStmt.inferParameters(tx, params)
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

		err = update.val.requiresType(col.colType, make(map[string]ColDescriptor), params, tx.currentDB.name, table.name)
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

func (stmt *UpdateStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		limit:   stmt.limit,
	}

	rowReader, err := selectStmt.Resolve(tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	err = stmt.validate(table)
	if err != nil {
		return nil, err
	}

	cols, err := rowReader.colsBySelector()
	if err != nil {
		return nil, err
	}

	for {
		row, err := rowReader.Read()
		if err == ErrNoMoreRows {
			break
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.db.name, table.name, col.colName)
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

			rval, err := sval.reduce(tx.catalog, row, table.db.name, table.name)
			if err != nil {
				return nil, err
			}

			err = rval.requiresType(col.colType, cols, nil, table.db.name, table.name)
			if err != nil {
				return nil, err
			}

			valuesByColID[col.id] = rval
		}

		pkEncVals, err := encodedPK(table, valuesByColID)
		if err != nil {
			return nil, err
		}

		// primary index entry
		mkey := mapKey(tx.sqlPrefix(), PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals)

		// mkey must exist
		_, err = tx.get(mkey)
		if err != nil {
			return nil, err
		}

		err = tx.doUpsert(pkEncVals, valuesByColID, table, true)
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
	limit    int
}

func (stmt *DeleteFromStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	selectStmt := &SelectStmt{
		ds:    stmt.tableRef,
		where: stmt.where,
	}
	return selectStmt.inferParameters(tx, params)
}

func (stmt *DeleteFromStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	selectStmt := &SelectStmt{
		ds:      stmt.tableRef,
		where:   stmt.where,
		indexOn: stmt.indexOn,
		limit:   stmt.limit,
	}

	rowReader, err := selectStmt.Resolve(tx, params, nil)
	if err != nil {
		return nil, err
	}
	defer rowReader.Close()

	table := rowReader.ScanSpecs().Index.table

	for {
		row, err := rowReader.Read()
		if err == ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.ValuesBySelector))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.db.name, table.name, col.colName)
			valuesByColID[col.id] = row.ValuesBySelector[encSel]
		}

		pkEncVals, err := encodedPK(table, valuesByColID)
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

func (sqlTx *SQLTx) deleteIndexEntries(pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table) error {
	for _, index := range table.indexes {
		var prefix string
		var encodedValues [][]byte

		if index.IsUnique() {
			if index.IsPrimary() {
				prefix = PIndexPrefix
			} else {
				prefix = UIndexPrefix
			}

			encodedValues = make([][]byte, 3+len(index.cols))
		} else {
			prefix = SIndexPrefix
			encodedValues = make([][]byte, 4+len(index.cols))
			encodedValues[len(encodedValues)-1] = pkEncVals
		}

		encodedValues[0] = EncodeID(table.db.id)
		encodedValues[1] = EncodeID(table.id)
		encodedValues[2] = EncodeID(index.id)

		for i, col := range index.cols {
			val, specified := valuesByColID[col.id]
			if !specified {
				val = &NullValue{t: col.colType}
			}

			encVal, _ := EncodeAsKey(val.Value(), col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		md := store.NewKVMetadata()

		md.AsDeleted(true)

		err := sqlTx.set(mapKey(sqlTx.sqlPrefix(), prefix, encodedValues...), md, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

type ValueExp interface {
	inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error)
	requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error
	substitute(params map[string]interface{}) (ValueExp, error)
	reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error)
	reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp
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
	Value() interface{}
	Compare(val TypedValue) (int, error)
	IsNull() bool
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

func (n *NullValue) IsNull() bool {
	return true
}

func (n *NullValue) Compare(val TypedValue) (int, error) {
	if n.t != AnyType && val.Type() != AnyType && n.t != val.Type() {
		return 0, ErrNotComparableValues
	}

	if val.Value() == nil {
		return 0, nil
	}

	return -1, nil
}

func (v *NullValue) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return v.t, nil
}

func (v *NullValue) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
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

func (v *NullValue) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *NullValue) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *NullValue) isConstant() bool {
	return true
}

func (v *NullValue) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

type Number struct {
	val int64
}

func (v *Number) Type() SQLValueType {
	return IntegerType
}

func (v *Number) IsNull() bool {
	return false
}

func (v *Number) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *Number) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	return nil
}

func (v *Number) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Number) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Number) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *Number) isConstant() bool {
	return true
}

func (v *Number) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Number) Value() interface{} {
	return v.val
}

func (v *Number) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != IntegerType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().(int64)

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

func (v *Timestamp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return TimestampType, nil
}

func (v *Timestamp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != TimestampType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
	}

	return nil
}

func (v *Timestamp) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Timestamp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Timestamp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *Timestamp) isConstant() bool {
	return true
}

func (v *Timestamp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Timestamp) Value() interface{} {
	return v.val
}

func (v *Timestamp) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != TimestampType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().(time.Time)

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

func (v *Varchar) Type() SQLValueType {
	return VarcharType
}

func (v *Varchar) IsNull() bool {
	return false
}

func (v *Varchar) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (v *Varchar) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != VarcharType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, VarcharType, t)
	}

	return nil
}

func (v *Varchar) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Varchar) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Varchar) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *Varchar) isConstant() bool {
	return true
}

func (v *Varchar) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Varchar) Value() interface{} {
	return v.val
}

func (v *Varchar) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
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

func (v *Bool) IsNull() bool {
	return false
}

func (v *Bool) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (v *Bool) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	return nil
}

func (v *Bool) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Bool) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Bool) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *Bool) isConstant() bool {
	return true
}

func (v *Bool) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Bool) Value() interface{} {
	return v.val
}

func (v *Bool) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
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

func (v *Blob) IsNull() bool {
	return false
}

func (v *Blob) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return BLOBType, nil
}

func (v *Blob) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BLOBType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BLOBType, t)
	}

	return nil
}

func (v *Blob) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *Blob) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return v, nil
}

func (v *Blob) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *Blob) isConstant() bool {
	return true
}

func (v *Blob) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	return nil
}

func (v *Blob) Value() interface{} {
	return v.val
}

func (v *Blob) Compare(val TypedValue) (int, error) {
	if val.IsNull() {
		return 1, nil
	}

	if val.Type() != BLOBType {
		return 0, ErrNotComparableValues
	}

	rval := val.Value().([]byte)

	return bytes.Compare(v.val, rval), nil
}

type FnCall struct {
	fn     string
	params []ValueExp
}

func (v *FnCall) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if strings.ToUpper(v.fn) == NowFnCall {
		return TimestampType, nil
	}

	return AnyType, fmt.Errorf("%w: unkown function %s", ErrIllegalArguments, v.fn)
}

func (v *FnCall) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if strings.ToUpper(v.fn) == NowFnCall {
		if t != TimestampType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, TimestampType, t)
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

func (v *FnCall) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if strings.ToUpper(v.fn) == NowFnCall {
		if len(v.params) > 0 {
			return nil, fmt.Errorf("%w: '%s' function does not expect any argument but %d were provided", ErrIllegalArguments, NowFnCall, len(v.params))
		}
		return &Timestamp{val: time.Now().UTC()}, nil
	}

	return nil, fmt.Errorf("%w: unkown function %s", ErrIllegalArguments, v.fn)
}

func (v *FnCall) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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

type converterFunc func(TypedValue) (TypedValue, error)

func getConverter(src, dst SQLValueType) (converterFunc, error) {
	if dst == TimestampType {

		if src == IntegerType {
			return func(val TypedValue) (TypedValue, error) {
				if val.Value() == nil {
					return &NullValue{t: TimestampType}, nil
				}
				return &Timestamp{val: time.Unix(val.Value().(int64), 0).UTC()}, nil
			}, nil
		}

		if src == VarcharType {
			return func(val TypedValue) (TypedValue, error) {
				if val.Value() == nil {
					return &NullValue{t: TimestampType}, nil
				}

				str := val.Value().(string)
				for _, layout := range []string{
					"2006-01-02 15:04:05.999999",
					"2006-01-02 15:04",
					"2006-01-02",
				} {
					t, err := time.ParseInLocation(layout, str, time.UTC)
					if err == nil {
						return &Timestamp{val: t.UTC()}, nil
					}
				}

				if len(str) > 30 {
					str = str[:30] + "..."
				}

				return nil, fmt.Errorf(
					"%w: can not cast string '%s' as a TIMESTAMP",
					ErrIllegalArguments,
					str,
				)
			}, nil
		}

		return nil, fmt.Errorf(
			"%w: only INTEGER and VARCHAR types can be cast as TIMESTAMP",
			ErrUnsupportedCast,
		)
	}

	return nil, fmt.Errorf(
		"%w: can not cast %s value as %s",
		ErrUnsupportedCast,
		src, dst,
	)
}

func (c *Cast) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	_, err := c.val.inferType(cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	// val type may be restricted by compatible conversions, but multiple types may be compatible...

	return c.t, nil
}

func (c *Cast) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if c.t != t {
		return fmt.Errorf(
			"%w: can not use value cast to %s as %s",
			ErrInvalidTypes,
			c.t,
			t,
		)
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

func (c *Cast) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	val, err := c.val.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	conv, err := getConverter(val.Type(), c.t)
	if conv == nil {
		return nil, err
	}

	return conv(val)
}

func (c *Cast) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return &Cast{
		val: c.val.reduceSelectors(row, implicitDB, implicitTable),
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

func (v *Param) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	t, ok := params[v.id]
	if !ok {
		params[v.id] = AnyType
		return AnyType, nil
	}

	return t, nil
}

func (v *Param) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
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
			return &Number{val: int64(v)}, nil
		}
	case uint:
		{
			return &Number{val: int64(v)}, nil
		}
	case uint64:
		{
			return &Number{val: int64(v)}, nil
		}
	case int64:
		{
			return &Number{val: v}, nil
		}
	case []byte:
		{
			return &Blob{val: v}, nil
		}
	case time.Time:
		{
			return &Timestamp{val: v}, nil
		}
	}

	return nil, ErrUnsupportedParameter
}

func (p *Param) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, ErrUnexpected
}

func (p *Param) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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
	Resolve(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error)
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
	limit     int
	orderBy   []*OrdCol
	as        string
}

func (stmt *SelectStmt) Limit() int {
	return stmt.limit
}

func (stmt *SelectStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	_, err := stmt.execAt(tx, nil)
	if err != nil {
		return err
	}

	// TODO (jeroiraz) may be optimized so to resolve the query statement just once
	rowReader, err := stmt.Resolve(tx, nil, nil)
	if err != nil {
		return err
	}
	defer rowReader.Close()

	return rowReader.InferParameters(params)
}

func (stmt *SelectStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

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

		col, err := table.GetColumnByName(stmt.orderBy[0].sel.col)
		if err != nil {
			return nil, err
		}

		_, indexed := table.indexesByColID[col.id]
		if !indexed {
			return nil, ErrLimitedOrderBy
		}
	}

	return tx, nil
}

func (stmt *SelectStmt) Resolve(tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	scanSpecs, err := stmt.genScanSpecs(tx, params)
	if err != nil {
		return nil, err
	}

	rowReader, err = stmt.ds.Resolve(tx, params, scanSpecs)
	if err != nil {
		return nil, err
	}

	if stmt.joins != nil {
		rowReader, err = newJointRowReader(rowReader, stmt.joins)
		if err != nil {
			return nil, err
		}
	}

	if stmt.where != nil {
		rowReader, err = newConditionalRowReader(rowReader, stmt.where)
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

		rowReader, err = newGroupedRowReader(rowReader, stmt.selectors, groupBy)
		if err != nil {
			return nil, err
		}

		if stmt.having != nil {
			rowReader, err = newConditionalRowReader(rowReader, stmt.having)
			if err != nil {
				return nil, err
			}
		}
	}

	rowReader, err = newProjectedRowReader(rowReader, stmt.as, stmt.selectors)
	if err != nil {
		return nil, err
	}

	if stmt.distinct {
		rowReader, err = newDistinctRowReader(rowReader)
		if err != nil {
			return nil, err
		}
	}

	if stmt.limit > 0 {
		return newLimitRowReader(rowReader, stmt.limit)
	}

	return rowReader, nil
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

		index, ok := table.indexesByName[indexName(table.name, cols)]
		if !ok {
			return nil, ErrNoAvailableIndex
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

	return &ScanSpecs{
		Index:         sortingIndex,
		rangesByColID: rangesByColID,
		DescOrder:     descOrder,
	}, nil
}

type UnionStmt struct {
	distinct    bool
	left, right DataSource
}

func (stmt *UnionStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	err := stmt.left.inferParameters(tx, params)
	if err != nil {
		return err
	}

	return stmt.right.inferParameters(tx, params)
}

func (stmt *UnionStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	_, err := stmt.left.execAt(tx, params)
	if err != nil {
		return tx, err
	}

	return stmt.right.execAt(tx, params)
}

func (stmt *UnionStmt) Resolve(tx *SQLTx, params map[string]interface{}, _ *ScanSpecs) (rowReader RowReader, err error) {
	leftRowReader, err := stmt.left.Resolve(tx, params, nil)
	if err != nil {
		return nil, err
	}

	rightRowReader, err := stmt.right.Resolve(tx, params, nil)
	if err != nil {
		return nil, err
	}

	rowReader, err = newUnionRowReader([]RowReader{leftRowReader, rightRowReader})
	if err != nil {
		return nil, err
	}

	if stmt.distinct {
		return newDistinctRowReader(rowReader)
	}

	return rowReader, nil
}

func (stmt *UnionStmt) Alias() string {
	return ""
}

type tableRef struct {
	db     string
	table  string
	period period
	as     string
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

	instantVal, err := exp.reduce(tx.catalog, nil, tx.currentDB.name, "")
	if err != nil {
		return 0, err
	}

	if i.instantType == txInstant {
		txID, ok := instantVal.Value().(int64)
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
			ts = instantVal.Value().(time.Time)
		} else {
			conv, err := getConverter(instantVal.Type(), TimestampType)
			if err != nil {
				return 0, err
			}

			tval, err := conv(instantVal)
			if err != nil {
				return 0, err
			}

			ts = tval.Value().(time.Time)
		}

		sts := ts

		if asc {
			if !inclusive {
				sts = sts.Add(1 * time.Second)
			}

			tx, err := tx.engine.store.FirstTxSince(sts)
			if err != nil {
				return 0, err
			}

			return tx.Header().ID, nil
		}

		if !inclusive {
			sts = sts.Add(-1 * time.Second)
		}

		tx, err := tx.engine.store.LastTxUntil(sts)
		if err != nil {
			return 0, err
		}

		return tx.Header().ID, nil
	}
}

func (stmt *tableRef) referencedTable(tx *SQLTx) (*Table, error) {
	if tx.currentDB == nil {
		return nil, ErrNoDatabaseSelected
	}

	if stmt.db != "" && stmt.db != tx.currentDB.name {
		return nil,
			fmt.Errorf(
				"%w: statements must only involve current selected database '%s' but '%s' was referenced",
				ErrNoSupported, tx.currentDB.name, stmt.db,
			)
	}

	table, err := tx.currentDB.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (stmt *tableRef) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *tableRef) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *tableRef) Resolve(tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
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
	if sel.as == "" {
		return sel.col
	}

	return sel.as
}

func (sel *ColSelector) setAlias(alias string) {
	sel.as = alias
}

func (sel *ColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	_, db, table, col := sel.resolve(implicitDB, implicitTable)
	encSel := EncodeSelector("", db, table, col)

	desc, ok := cols[encSel]
	if !ok {
		return AnyType, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	return desc.Type, nil
}

func (sel *ColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	_, db, table, col := sel.resolve(implicitDB, implicitTable)
	encSel := EncodeSelector("", db, table, col)

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

func (sel *ColSelector) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate in current context", ErrInvalidValue)
	}

	aggFn, db, table, col := sel.resolve(implicitDB, implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, db, table, col)]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, col)
	}

	return v, nil
}

func (sel *ColSelector) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	aggFn, db, table, col := sel.resolve(implicitDB, implicitTable)

	v, ok := row.ValuesBySelector[EncodeSelector(aggFn, db, table, col)]
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

func (sel *AggColSelector) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if sel.aggFn == COUNT {
		return IntegerType, nil
	}

	colSelector := &ColSelector{db: sel.db, table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		err := colSelector.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
		if err != nil {
			return AnyType, err
		}

		return IntegerType, nil
	}

	return colSelector.inferType(cols, params, implicitDB, implicitTable)
}

func (sel *AggColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if sel.aggFn == COUNT {
		if t != IntegerType {
			return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
		}
		return nil
	}

	colSelector := &ColSelector{db: sel.db, table: sel.table, col: sel.col}

	if sel.aggFn == SUM || sel.aggFn == AVG {
		return colSelector.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
	}

	return colSelector.requiresType(t, cols, params, implicitDB, implicitTable)
}

func (sel *AggColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *AggColSelector) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, fmt.Errorf("%w: no row to evaluate aggregation (%s) in current context", ErrInvalidValue, sel.aggFn)
	}

	v, ok := row.ValuesBySelector[EncodeSelector(sel.resolve(implicitDB, implicitTable))]
	if !ok {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, sel.col)
	}
	return v, nil
}

func (sel *AggColSelector) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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

func (bexp *NumExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	err := bexp.left.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	err = bexp.right.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return IntegerType, nil
}

func (bexp *NumExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, IntegerType, t)
	}

	err := bexp.left.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return err
	}

	err = bexp.right.requiresType(IntegerType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return err
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

func (bexp *NumExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	vl, err := bexp.left.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	vr, err := bexp.right.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, err
	}

	nl, isNumber := vl.Value().(int64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
	}

	nr, isNumber := vr.Value().(int64)
	if !isNumber {
		return nil, fmt.Errorf("%w (expecting numeric value)", ErrInvalidValue)
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

func (bexp *NumExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return &NumExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitDB, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitDB, implicitTable),
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

func (bexp *NotBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	err := bexp.exp.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *NotBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	return bexp.exp.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
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

	return &Bool{val: !r}, nil
}

func (bexp *NotBoolExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return &NotBoolExp{
		exp: bexp.exp.reduceSelectors(row, implicitDB, implicitTable),
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

func (bexp *LikeBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return BooleanType, nil
}

func (bexp *LikeBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if bexp.val == nil || bexp.pattern == nil {
		return fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	if t != BooleanType {
		return fmt.Errorf("error using the value of the LIKE operator as %s: %w", t, ErrInvalidTypes)
	}

	err := bexp.pattern.requiresType(VarcharType, cols, params, implicitDB, implicitTable)
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

func (bexp *LikeBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if bexp.val == nil || bexp.pattern == nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", ErrInvalidCondition)
	}

	rval, err := bexp.val.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rval.Type() != VarcharType {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w (expecting %s)", ErrInvalidTypes, VarcharType)
	}

	rpattern, err := bexp.pattern.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	if rpattern.Type() != VarcharType {
		return nil, fmt.Errorf("error evaluating 'LIKE' clause: %w", ErrInvalidTypes)
	}

	matched, err := regexp.MatchString(rpattern.Value().(string), rval.Value().(string))
	if err != nil {
		return nil, fmt.Errorf("error in 'LIKE' clause: %w", err)
	}

	return &Bool{val: matched != bexp.notLike}, nil
}

func (bexp *LikeBoolExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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

func (bexp *CmpBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	tleft, err := bexp.left.inferType(cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	tright, err := bexp.right.inferType(cols, params, implicitDB, implicitTable)
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
		err = bexp.left.requiresType(tright, cols, params, implicitDB, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}

	if tright == AnyType {
		err = bexp.right.requiresType(tleft, cols, params, implicitDB, implicitTable)
		if err != nil {
			return AnyType, err
		}
	}

	return BooleanType, nil
}

func (bexp *CmpBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	_, err := bexp.inferType(cols, params, implicitDB, implicitTable)

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

func (bexp *CmpBoolExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return &CmpBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitDB, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitDB, implicitTable),
	}
}

func (bexp *CmpBoolExp) isConstant() bool {
	return bexp.left.isConstant() && bexp.right.isConstant()
}

func (bexp *CmpBoolExp) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
	matchingFunc := func(left, right ValueExp) (*ColSelector, ValueExp, bool) {
		s, isSel := bexp.left.(*ColSelector)
		if isSel && bexp.right.isConstant() {
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

	aggFn, db, t, col := sel.resolve(table.db.name, table.name)
	if aggFn != "" || db != table.db.name || t != asTable {
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

	rval, err := val.reduce(nil, nil, table.db.name, table.name)
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

func (bexp *BinBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	err := bexp.left.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, err
	}

	return BooleanType, nil
}

func (bexp *BinBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BooleanType {
		return fmt.Errorf("%w: %v can not be interpreted as type %v", ErrInvalidTypes, BooleanType, t)
	}

	err := bexp.left.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
	if err != nil {
		return err
	}

	err = bexp.right.requiresType(BooleanType, cols, params, implicitDB, implicitTable)
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

func (bexp *BinBoolExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return &BinBoolExp{
		op:    bexp.op,
		left:  bexp.left.reduceSelectors(row, implicitDB, implicitTable),
		right: bexp.right.reduceSelectors(row, implicitDB, implicitTable),
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
	q *SelectStmt
}

func (bexp *ExistsBoolExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return AnyType, errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	return errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *ExistsBoolExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, errors.New("not yet supported")
}

func (bexp *ExistsBoolExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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

func (bexp *InSubQueryExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	return fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) substitute(params map[string]interface{}) (ValueExp, error) {
	return bexp, nil
}

func (bexp *InSubQueryExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	return nil, fmt.Errorf("error inferring type in 'IN' clause: %w", ErrNoSupported)
}

func (bexp *InSubQueryExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
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

func (bexp *InListExp) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	t, err := bexp.val.inferType(cols, params, implicitDB, implicitTable)
	if err != nil {
		return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
	}

	for _, v := range bexp.values {
		err = v.requiresType(t, cols, params, implicitDB, implicitTable)
		if err != nil {
			return AnyType, fmt.Errorf("error inferring type in 'IN' clause: %w", err)
		}
	}

	return BooleanType, nil
}

func (bexp *InListExp) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	_, err := bexp.inferType(cols, params, implicitDB, implicitTable)
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

func (bexp *InListExp) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	rval, err := bexp.val.reduce(catalog, row, implicitDB, implicitTable)
	if err != nil {
		return nil, fmt.Errorf("error evaluating 'IN' clause: %w", err)
	}

	var found bool

	for _, v := range bexp.values {
		rv, err := v.reduce(catalog, row, implicitDB, implicitTable)
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

func (bexp *InListExp) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	values := make([]ValueExp, len(bexp.values))

	for i, val := range bexp.values {
		values[i] = val.reduceSelectors(row, implicitDB, implicitTable)
	}

	return &InListExp{
		val:    bexp.val.reduceSelectors(row, implicitDB, implicitTable),
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

func (stmt *FnDataSourceStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return tx, nil
}

func (stmt *FnDataSourceStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
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

func (stmt *FnDataSourceStmt) Resolve(tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (rowReader RowReader, err error) {
	if stmt.fnCall == nil {
		return nil, fmt.Errorf("%w: function is unspecified", ErrIllegalArguments)
	}

	switch strings.ToUpper(stmt.fnCall.fn) {
	case DatabasesFnCall:
		{
			return stmt.resolveListDatabases(tx, params, scanSpecs)
		}
	case TablesFnCall:
		{
			return stmt.resolveListTables(tx, params, scanSpecs)
		}
	case ColumnsFnCall:
		{
			return stmt.resolveListColumns(tx, params, scanSpecs)
		}
	case IndexesFnCall:
		{
			return stmt.resolveListIndexes(tx, params, scanSpecs)
		}
	}

	return nil, fmt.Errorf("%w (%s)", ErrFunctionDoesNotExist, stmt.fnCall.fn)
}

func (stmt *FnDataSourceStmt) resolveListDatabases(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (rowReader RowReader, err error) {
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
		dbs = make([]string, len(tx.catalog.Databases()))

		for i, db := range tx.catalog.Databases() {
			dbs[i] = db.name
		}
	} else {
		dbs, err = tx.engine.multidbHandler.ListDatabases(tx.ctx)
		if err != nil {
			return nil, err
		}
	}

	values := make([][]ValueExp, len(dbs))

	for i, db := range dbs {
		values[i] = []ValueExp{&Varchar{val: db}}
	}

	return newValuesRowReader(tx, cols, "*", stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListTables(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (rowReader RowReader, err error) {
	if len(stmt.fnCall.params) > 0 {
		return nil, fmt.Errorf("%w: function '%s' expect no parameters but %d were provided", ErrIllegalArguments, TablesFnCall, len(stmt.fnCall.params))
	}

	cols := make([]ColDescriptor, 1)
	cols[0] = ColDescriptor{
		Column: "name",
		Type:   VarcharType,
	}

	db := tx.Database()

	tables := db.GetTables()

	values := make([][]ValueExp, len(tables))

	for i, t := range tables {
		values[i] = []ValueExp{&Varchar{val: t.name}}
	}

	return newValuesRowReader(tx, cols, db.name, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListColumns(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
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

	tableName, err := val.reduce(tx.catalog, nil, tx.currentDB.name, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.currentDB.GetTableByName(tableName.Value().(string))
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
		for _, index := range table.IndexesByColID(c.ID()) {
			if index.IsUnique() && len(index.Cols()) == 1 {
				unique = true
				break
			}
		}

		values[i] = []ValueExp{
			&Varchar{val: table.name},
			&Varchar{val: c.colName},
			&Varchar{val: c.colType},
			&Number{val: int64(c.MaxLen())},
			&Bool{val: c.IsNullable()},
			&Bool{val: c.autoIncrement},
			&Bool{val: indexed},
			&Bool{val: table.PrimaryIndex().IncludesCol(c.ID())},
			&Bool{val: unique},
		}
	}

	return newValuesRowReader(tx, cols, table.db.name, stmt.Alias(), values)
}

func (stmt *FnDataSourceStmt) resolveListIndexes(tx *SQLTx, params map[string]interface{}, ScanSpecs *ScanSpecs) (RowReader, error) {
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

	tableName, err := val.reduce(tx.catalog, nil, tx.currentDB.name, "")
	if err != nil {
		return nil, err
	}

	if tableName.Type() != VarcharType {
		return nil, fmt.Errorf("%w: expected '%s' for table name but type '%s' given instead", ErrIllegalArguments, VarcharType, tableName.Type())
	}

	table, err := tx.currentDB.GetTableByName(tableName.Value().(string))
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

	return newValuesRowReader(tx, cols, table.db.name, stmt.Alias(), values)
}
