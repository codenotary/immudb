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

	err := tx.commit()
	if err != nil {
		return nil, err
	}

	return tx.engine.newTx(true)
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
	DB string
}

func (stmt *CreateDatabaseStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *CreateDatabaseStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	id := uint32(len(tx.catalog.dbsByID) + 1)

	db, err := tx.catalog.newDatabase(id, stmt.DB)
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
	return tx, tx.useDatabase(stmt.DB)
}

type UseSnapshotStmt struct {
	sinceTx  uint64
	asBefore uint64
}

func (stmt *UseSnapshotStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *UseSnapshotStmt) execAt(tx *SQLTx, params map[string]interface{}) (*SQLTx, error) {
	return nil, ErrNoSupported
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
		//{auto_incremental | nullable}{maxLen}{colNAME})
		v := make([]byte, 1+4+len(col.colName))

		if col.autoIncrement {
			if len(table.primaryIndex.cols) > 1 || col.id != table.primaryIndex.cols[0].id {
				return nil, ErrLimitedAutoIncrement
			}

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
			EncodeID(tx.currentDB.id),
			EncodeID(table.id),
			EncodeID(col.id),
			[]byte(col.colType),
		)

		err = tx.set(mappedKey, nil, v)
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
	return nil, ErrNoSupported
}

type UpsertIntoStmt struct {
	isInsert bool
	tableRef *tableRef
	cols     []string
	rows     []*RowSpec
}

type RowSpec struct {
	Values []ValueExp
}

func (stmt *UpsertIntoStmt) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	if tx.currentDB == nil {
		return ErrNoDatabaseSelected
	}

	for _, row := range stmt.rows {
		if len(stmt.cols) != len(row.Values) {
			return ErrIllegalArguments
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
			return nil, ErrDuplicatedColumn
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

		for colID, col := range table.colsByID {
			colPos, specified := selPosByColID[colID]
			if !specified {
				if col.notNull {
					return nil, ErrNotNullableColumnCannotBeNull
				}
				continue
			}

			if stmt.isInsert && col.autoIncrement {
				return nil, ErrNoValueForAutoIncrementalColumn
			}

			cVal := row.Values[colPos]

			val, err := cVal.substitute(params)
			if err != nil {
				return nil, err
			}

			rval, err := val.reduce(tx.catalog, nil, tx.currentDB.name, table.name)
			if err != nil {
				return nil, err
			}

			_, isNull := rval.(*NullValue)
			if isNull {
				if col.notNull {
					return nil, ErrNotNullableColumnCannotBeNull
				}

				continue
			}

			valuesByColID[colID] = rval
		}

		// inject auto-incremental pk value
		if stmt.isInsert && table.autoIncrementPK {
			table.maxPK++

			pkCol := table.primaryIndex.cols[0]

			valuesByColID[pkCol.id] = &Number{val: table.maxPK}

			tx.lastInsertedPKs[table.name] = table.maxPK
		}

		pkEncVals, err := encodedPK(table, valuesByColID)
		if err != nil {
			return nil, err
		}

		err = tx.doUpsert(pkEncVals, valuesByColID, table, stmt.isInsert)
		if err != nil {
			return nil, err
		}
	}

	return tx, nil
}

func (tx *SQLTx) doUpsert(pkEncVals []byte, valuesByColID map[uint32]TypedValue, table *Table, isInsert bool) error {
	var reusableIndexEntries map[uint32]struct{}

	if !isInsert && len(table.indexes) > 1 {
		currPKRow, err := tx.fetchPKRow(table, valuesByColID)
		if err != nil && err != ErrNoMoreRows {
			return err
		}

		if err == nil {
			currValuesByColID := make(map[uint32]TypedValue, len(currPKRow.Values))

			for _, col := range table.cols {
				encSel := EncodeSelector("", table.db.name, table.name, col.colName)
				currValuesByColID[col.id] = currPKRow.Values[encSel]
			}

			reusableIndexEntries, err = tx.deprecateIndexEntries(pkEncVals, currValuesByColID, valuesByColID, table)
			if err != nil {
				return err
			}
		}
	}

	// create primary index entry
	mkey := mapKey(tx.sqlPrefix(), PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(table.primaryIndex.id), pkEncVals)

	if isInsert && !table.autoIncrementPK {
		// mkey must not exist
		_, err := tx.get(mkey)
		if err == nil {
			return store.ErrKeyAlreadyExists
		}
		if err != store.ErrKeyNotFound {
			return err
		}
	}

	if !isInsert && table.autoIncrementPK {
		// mkey must exist
		_, err := tx.get(mkey)
		if err != nil {
			return err
		}
	}

	valbuf := bytes.Buffer{}

	b := make([]byte, EncLenLen)
	binary.BigEndian.PutUint32(b, uint32(len(valuesByColID)))

	_, err := valbuf.Write(b)
	if err != nil {
		return err
	}

	for _, col := range table.cols {
		rval, notNull := valuesByColID[col.id]
		if !notNull {
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
			if col.MaxLen() > maxKeyLen {
				return ErrMaxKeyLengthExceeded
			}

			rval, notNull := valuesByColID[col.id]
			if !notNull {
				return ErrIndexedColumnCanNotBeNull
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
		rval, notNull := valuesByColID[col.id]
		if !notNull {
			return nil, ErrPKCanNotBeNull
		}

		encVal, err := EncodeAsKey(rval.Value(), col.colType, col.MaxLen())
		if err != nil {
			return nil, err
		}

		if len(encVal) > maxKeyLen {
			return nil, ErrMaxKeyLengthExceeded
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
		index:         table.primaryIndex,
		rangesByColID: pkRanges,
	}

	r, err := newRawRowReader(tx, table, 0, table.name, scanSpecs)
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
			currVal, notNull := currValuesByColID[col.id]
			if !notNull {
				return nil, ErrCorruptedData
			}

			newVal, notNull := newValuesByColID[col.id]
			if notNull {
				r, err := currVal.Compare(newVal)
				if err != nil {
					return nil, err
				}

				sameIndexKey = sameIndexKey && r == 0
			}

			encVal, _ := EncodeAsKey(currVal.Value(), col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		// mark existent index entry as deleted
		if sameIndexKey {
			reusableIndexEntries[index.id] = struct{}{}
		} else {
			err = tx.set(mapKey(tx.sqlPrefix(), prefix, encodedValues...), store.NewKVMetadata().AsDeleted(true), nil)
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

	table := rowReader.ScanSpecs().index.table

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

		valuesByColID := make(map[uint32]TypedValue, len(row.Values))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.db.name, table.name, col.colName)
			valuesByColID[col.id] = row.Values[encSel]
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

		err = tx.doUpsert(pkEncVals, valuesByColID, table, false)
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

	table := rowReader.ScanSpecs().index.table

	for {
		row, err := rowReader.Read()
		if err == ErrNoMoreRows {
			break
		}

		valuesByColID := make(map[uint32]TypedValue, len(row.Values))

		for _, col := range table.cols {
			encSel := EncodeSelector("", table.db.name, table.name, col.colName)
			valuesByColID[col.id] = row.Values[encSel]
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

		// some rows might not indexed by every index
		indexed := true

		for i, col := range index.cols {
			val, notNull := valuesByColID[col.id]
			if !notNull {
				indexed = false
				break
			}

			encVal, _ := EncodeAsKey(val.Value(), col.colType, col.MaxLen())

			encodedValues[i+3] = encVal
		}

		if !indexed {
			continue
		}

		err := sqlTx.set(mapKey(sqlTx.sqlPrefix(), prefix, encodedValues...), store.NewKVMetadata().AsDeleted(true), nil)
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

func (v *Number) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return IntegerType, nil
}

func (v *Number) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != IntegerType {
		return ErrInvalidTypes
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
	_, isNull := val.(*NullValue)
	if isNull {
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

type Varchar struct {
	val string
}

func (v *Varchar) Type() SQLValueType {
	return VarcharType
}

func (v *Varchar) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return VarcharType, nil
}

func (v *Varchar) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != VarcharType {
		return ErrInvalidTypes
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

func (v *Bool) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return BooleanType, nil
}

func (v *Bool) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BooleanType {
		return ErrInvalidTypes
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

func (v *Blob) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	return BLOBType, nil
}

func (v *Blob) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if t != BLOBType {
		return ErrInvalidTypes
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

func (v *SysFn) inferType(cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) (SQLValueType, error) {
	if strings.ToUpper(v.fn) == "NOW" {
		return IntegerType, nil
	}

	return AnyType, ErrIllegalArguments
}

func (v *SysFn) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if strings.ToUpper(v.fn) == "NOW" {
		if t != IntegerType {
			return ErrInvalidTypes
		}

		return nil
	}

	return ErrIllegalArguments
}

func (v *SysFn) substitute(params map[string]interface{}) (ValueExp, error) {
	return v, nil
}

func (v *SysFn) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if strings.ToUpper(v.fn) == "NOW" {
		return &Number{val: time.Now().UnixNano()}, nil
	}

	return nil, errors.New("not yet supported")
}

func (v *SysFn) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	return v
}

func (v *SysFn) isConstant() bool {
	return false
}

func (v *SysFn) selectorRanges(table *Table, asTable string, params map[string]interface{}, rangesByColID map[uint32]*typedValueRange) error {
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
		return nil, ErrMissingParameter
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
	inferParameters(tx *SQLTx, params map[string]SQLValueType) error
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

type ScanSpecs struct {
	index         *Index
	rangesByColID map[uint32]*typedValueRange
	descOrder     bool
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
		rowReader, err = newJointRowReader(rowReader, stmt.joins, params)
		if err != nil {
			return nil, err
		}
	}

	if stmt.where != nil {
		rowReader, err = newConditionalRowReader(rowReader, stmt.where, params)
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
			rowReader, err = newConditionalRowReader(rowReader, stmt.having, params)
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

		index, ok := table.indexes[indexKeyFrom(cols)]
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
		index:         sortingIndex,
		rangesByColID: rangesByColID,
		descOrder:     descOrder,
	}, nil
}

type tableRef struct {
	db       string
	table    string
	asBefore uint64
	as       string
}

func (stmt *tableRef) referencedTable(tx *SQLTx) (*Table, error) {
	var db *Database

	if stmt.db != "" {
		rdb, err := tx.catalog.GetDatabaseByName(stmt.db)
		if err != nil {
			return nil, err
		}

		db = rdb
	}

	if db == nil {
		if tx.currentDB == nil {
			return nil, ErrNoDatabaseSelected
		}

		db = tx.currentDB
	}

	table, err := db.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (stmt *tableRef) inferParameters(tx *SQLTx, params map[string]SQLValueType) error {
	return nil
}

func (stmt *tableRef) Resolve(tx *SQLTx, params map[string]interface{}, scanSpecs *ScanSpecs) (RowReader, error) {
	if tx == nil {
		return nil, ErrIllegalArguments
	}

	table, err := stmt.referencedTable(tx)
	if err != nil {
		return nil, err
	}

	return newRawRowReader(tx, table, stmt.asBefore, stmt.as, scanSpecs)
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
		return ErrInvalidTypes
	}

	return nil
}

func (sel *ColSelector) substitute(params map[string]interface{}) (ValueExp, error) {
	return sel, nil
}

func (sel *ColSelector) reduce(catalog *Catalog, row *Row, implicitDB, implicitTable string) (TypedValue, error) {
	if row == nil {
		return nil, ErrInvalidValue
	}

	aggFn, db, table, col := sel.resolve(implicitDB, implicitTable)

	v, ok := row.Values[EncodeSelector(aggFn, db, table, col)]
	if !ok {
		return nil, ErrColumnDoesNotExist
	}

	return v, nil
}

func (sel *ColSelector) reduceSelectors(row *Row, implicitDB, implicitTable string) ValueExp {
	aggFn, db, table, col := sel.resolve(implicitDB, implicitTable)

	v, ok := row.Values[EncodeSelector(aggFn, db, table, col)]
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
			return AnyType, ErrInvalidTypes
		}

		return IntegerType, nil
	}

	return colSelector.inferType(cols, params, implicitDB, implicitTable)
}

func (sel *AggColSelector) requiresType(t SQLValueType, cols map[string]ColDescriptor, params map[string]SQLValueType, implicitDB, implicitTable string) error {
	if sel.aggFn == COUNT {
		if t != IntegerType {
			return ErrInvalidTypes
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
	v, ok := row.Values[EncodeSelector(sel.resolve(implicitDB, implicitTable))]
	if !ok {
		return nil, ErrColumnDoesNotExist
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
		return ErrInvalidTypes
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
		return ErrInvalidTypes
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
		return AnyType, ErrInvalidTypes
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
		return ErrInvalidTypes
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
	if err == ErrMissingParameter {
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
		return ErrInvalidTypes
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
