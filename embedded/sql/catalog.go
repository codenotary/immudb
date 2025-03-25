/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/google/uuid"
)

// Catalog represents a database catalog containing metadata for all tables in the database.
type Catalog struct {
	enginePrefix []byte

	tables       []*Table
	tablesByID   map[uint32]*Table
	tablesByName map[string]*Table

	maxTableID uint32 // The maxTableID variable is used to assign unique ids to new tables as they are created.
}

type Constraint interface{}

type PrimaryKeyConstraint []string

type CheckConstraint struct {
	id   uint32
	name string
	exp  ValueExp
}

type Table struct {
	catalog          *Catalog
	id               uint32
	name             string
	cols             []*Column
	colsByID         map[uint32]*Column
	colsByName       map[string]*Column
	indexes          []*Index
	indexesByName    map[string]*Index
	indexesByColID   map[uint32][]*Index
	checkConstraints map[string]CheckConstraint
	primaryIndex     *Index
	autoIncrementPK  bool
	maxPK            int64

	maxColID   uint32
	maxIndexID uint32
}

type Index struct {
	table    *Table
	id       uint32
	unique   bool
	cols     []*Column
	colsByID map[uint32]*Column
}

type Column struct {
	table         *Table
	id            uint32
	colName       string
	colType       SQLValueType
	maxLen        int
	autoIncrement bool
	notNull       bool
}

func newCatalog(enginePrefix []byte) *Catalog {
	ctlg := &Catalog{
		enginePrefix: enginePrefix,
		tablesByID:   make(map[uint32]*Table),
		tablesByName: make(map[string]*Table),
	}

	pgTypeTable := &Table{
		catalog: ctlg,
		name:    "pg_type",
		cols: []*Column{
			{
				colName: "oid",
				colType: VarcharType,
				maxLen:  10,
			},
			{
				colName: "typbasetype",
				colType: VarcharType,
				maxLen:  10,
			},
			{
				colName: "typname",
				colType: VarcharType,
				maxLen:  50,
			},
		},
	}

	pgTypeTable.colsByName = make(map[string]*Column, len(pgTypeTable.cols))

	for _, col := range pgTypeTable.cols {
		pgTypeTable.colsByName[col.colName] = col
	}

	pgTypeTable.indexes = []*Index{
		{
			unique: true,
			cols: []*Column{
				pgTypeTable.colsByName["oid"],
			},
			colsByID: map[uint32]*Column{
				0: pgTypeTable.colsByName["oid"],
			},
		},
	}

	pgTypeTable.primaryIndex = pgTypeTable.indexes[0]
	ctlg.tablesByName[pgTypeTable.name] = pgTypeTable

	return ctlg
}

func (catlg *Catalog) ExistTable(table string) bool {
	_, exists := catlg.tablesByName[table]
	return exists
}

func (catlg *Catalog) GetTables() []*Table {
	ts := make([]*Table, 0, len(catlg.tables))

	ts = append(ts, catlg.tables...)

	return ts
}

func (catlg *Catalog) GetTableByName(name string) (*Table, error) {
	table, exists := catlg.tablesByName[name]
	if !exists {
		return nil, fmt.Errorf("%w (%s)", ErrTableDoesNotExist, name)
	}
	return table, nil
}

func (catlg *Catalog) GetTableByID(id uint32) (*Table, error) {
	table, exists := catlg.tablesByID[id]
	if !exists {
		return nil, ErrTableDoesNotExist
	}
	return table, nil
}

func (t *Table) ID() uint32 {
	return t.id
}

func (t *Table) Cols() []*Column {
	cs := make([]*Column, 0, len(t.cols))

	cs = append(cs, t.cols...)

	return cs
}

func (t *Table) ColsByName() map[string]*Column {
	cs := make(map[string]*Column, len(t.cols))

	for _, c := range t.cols {
		cs[c.colName] = c
	}

	return cs
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) PrimaryIndex() *Index {
	return t.primaryIndex
}

func (t *Table) IsIndexed(colName string) (indexed bool, err error) {
	col, err := t.GetColumnByName(colName)
	if err != nil {
		return false, err
	}
	return len(t.indexesByColID[col.id]) > 0, nil
}

func (t *Table) GetColumnByName(name string) (*Column, error) {
	col, exists := t.colsByName[name]
	if !exists {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, name)
	}
	return col, nil
}

func (t *Table) GetColumnByID(id uint32) (*Column, error) {
	col, exists := t.colsByID[id]
	if !exists {
		return nil, ErrColumnDoesNotExist
	}
	return col, nil
}

func (t *Table) ColumnsByID() map[uint32]*Column {
	return t.colsByID
}

func (t *Table) GetIndexes() []*Index {
	idxs := make([]*Index, 0, len(t.indexes))

	idxs = append(idxs, t.indexes...)

	return idxs
}

func (t *Table) GetIndexesByColID(colID uint32) []*Index {
	idxs := make([]*Index, 0, len(t.indexes))

	idxs = append(idxs, t.indexesByColID[colID]...)

	return idxs
}

func (t *Table) GetMaxColID() uint32 {
	return t.maxColID
}

func (i *Index) IsPrimary() bool {
	return i.id == PKIndexID
}

func (i *Index) IsUnique() bool {
	return i.unique
}

func (i *Index) Cols() []*Column {
	return i.cols
}

func (i *Index) IncludesCol(colID uint32) bool {
	_, ok := i.colsByID[colID]
	return ok
}

func (i *Index) enginePrefix() []byte {
	return i.table.catalog.enginePrefix
}

func (i *Index) coversOrdCols(ordExps []*OrdExp, rangesByColID map[uint32]*typedValueRange) bool {
	if !ordExpsHaveSameDirection(ordExps) {
		return false
	}
	return i.hasPrefix(i.cols, ordExps) || i.sortableUsing(ordExps, rangesByColID)
}

func ordExpsHaveSameDirection(exps []*OrdExp) bool {
	if len(exps) == 0 {
		return true
	}

	desc := exps[0].descOrder
	for _, e := range exps[1:] {
		if e.descOrder != desc {
			return false
		}
	}
	return true
}

func (i *Index) hasPrefix(columns []*Column, ordExps []*OrdExp) bool {
	if len(ordExps) > len(columns) {
		return false
	}

	for j, ordCol := range ordExps {
		sel := ordCol.AsSelector()
		if sel == nil {
			return false
		}

		aggFn, _, colName := sel.resolve(i.table.Name())
		if len(aggFn) > 0 {
			return false
		}

		col, err := i.table.GetColumnByName(colName)
		if err != nil || col.id != columns[j].id {
			return false
		}
	}
	return true
}

func (i *Index) sortableUsing(columns []*OrdExp, rangesByColID map[uint32]*typedValueRange) bool {
	// all columns before colID must be fixedValues otherwise the index can not be used
	sel := columns[0].AsSelector()
	if sel == nil {
		return false
	}

	aggFn, _, colName := sel.resolve(i.table.Name())
	if len(aggFn) > 0 {
		return false
	}

	firstCol, err := i.table.GetColumnByName(colName)
	if err != nil {
		return false
	}

	for j, col := range i.cols {
		if col.id == firstCol.id {
			return i.hasPrefix(i.cols[j:], columns)
		}

		colRange, ok := rangesByColID[col.id]
		if ok && colRange.unitary() {
			continue
		}
		return false
	}
	return false
}

func (i *Index) Name() string {
	return indexName(i.table.name, i.cols)
}

func (i *Index) ID() uint32 {
	return i.id
}

func (t *Table) GetIndexByName(name string) (*Index, error) {
	idx, exists := t.indexesByName[name]
	if !exists {
		return nil, fmt.Errorf("%w (%s)", ErrIndexNotFound, name)
	}
	return idx, nil
}

func indexName(tableName string, cols []*Column) string {
	var buf strings.Builder

	buf.WriteString(tableName)

	buf.WriteString("(")

	for c, col := range cols {
		buf.WriteString(col.colName)

		if c < len(cols)-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString(")")

	return buf.String()
}

func (catlg *Catalog) newTable(name string, colsSpec map[uint32]*ColSpec, checkConstraints map[string]CheckConstraint, maxColID uint32) (table *Table, err error) {
	if len(name) == 0 || len(colsSpec) == 0 {
		return nil, ErrIllegalArguments
	}

	for id := range colsSpec {
		if id <= 0 || id > maxColID {
			return nil, ErrIllegalArguments
		}
	}

	exists := catlg.ExistTable(name)
	if exists {
		return nil, fmt.Errorf("%w (%s)", ErrTableAlreadyExists, name)
	}

	// Generate a new ID for the table by incrementing the 'maxTableID' variable of the 'catalog' instance.
	id := (catlg.maxTableID + 1)

	// This code is attempting to check if a table with the given id already exists in the Catalog.
	// If the function returns nil for err, it means that the table already exists and the function
	// should return an error indicating that the table cannot be created again.
	_, err = catlg.GetTableByID(id)
	if err == nil {
		return nil, fmt.Errorf("%w (%d)", ErrTableAlreadyExists, id)
	}

	table = &Table{
		id:               id,
		catalog:          catlg,
		name:             name,
		cols:             make([]*Column, 0, len(colsSpec)),
		colsByID:         make(map[uint32]*Column),
		colsByName:       make(map[string]*Column),
		indexesByName:    make(map[string]*Index),
		indexesByColID:   make(map[uint32][]*Index),
		checkConstraints: checkConstraints,
		maxColID:         maxColID,
	}

	for id := uint32(1); id <= maxColID; id++ {
		cs, found := colsSpec[id]
		if !found {
			// dropped column
			continue
		}

		if isReservedCol(cs.colName) {
			return nil, fmt.Errorf("%w(%s)", ErrReservedWord, cs.colName)
		}

		_, colExists := table.colsByName[cs.colName]
		if colExists {
			return nil, ErrDuplicatedColumn
		}

		if cs.autoIncrement && cs.colType != IntegerType {
			return nil, ErrLimitedAutoIncrement
		}

		if !validMaxLenForType(cs.maxLen, cs.colType) {
			return nil, ErrLimitedMaxLen
		}

		col := &Column{
			id:            uint32(id),
			table:         table,
			colName:       cs.colName,
			colType:       cs.colType,
			maxLen:        cs.maxLen,
			autoIncrement: cs.autoIncrement,
			notNull:       cs.notNull,
		}

		table.cols = append(table.cols, col)
		table.colsByID[col.id] = col
		table.colsByName[col.colName] = col
	}

	catlg.tables = append(catlg.tables, table)
	catlg.tablesByID[table.id] = table
	catlg.tablesByName[table.name] = table

	// increment table count on successfull table creation.
	// This ensures that each new table is assigned a unique ID
	// that has not been used before.
	catlg.maxTableID++

	return table, nil
}

func (catlg *Catalog) deleteTable(table *Table) error {
	_, exists := catlg.tablesByID[table.id]
	if !exists {
		return ErrTableDoesNotExist
	}

	newTables := make([]*Table, 0, len(catlg.tables)-1)

	for _, t := range catlg.tables {
		if t.id != table.id {
			newTables = append(newTables, t)
		}
	}

	catlg.tables = newTables
	delete(catlg.tablesByID, table.id)
	delete(catlg.tablesByName, table.name)

	return nil
}

func (t *Table) newIndex(unique bool, colIDs []uint32) (index *Index, err error) {
	if len(colIDs) < 1 {
		return nil, ErrIllegalArguments
	}

	// validate column ids
	cols := make([]*Column, len(colIDs))
	colsByID := make(map[uint32]*Column, len(colIDs))

	for i, colID := range colIDs {
		col, err := t.GetColumnByID(colID)
		if err != nil {
			return nil, err
		}

		_, ok := colsByID[colID]
		if ok {
			return nil, ErrDuplicatedColumn
		}

		cols[i] = col
		colsByID[colID] = col
	}

	index = &Index{
		id:       uint32(t.maxIndexID),
		table:    t,
		unique:   unique,
		cols:     cols,
		colsByID: colsByID,
	}

	_, exists := t.indexesByName[index.Name()]
	if exists {
		return nil, ErrIndexAlreadyExists
	}

	t.indexes = append(t.indexes, index)
	t.indexesByName[index.Name()] = index

	// having a direct way to get the indexes by colID
	for _, col := range index.cols {
		t.indexesByColID[col.id] = append(t.indexesByColID[col.id], index)
	}

	if index.id == PKIndexID {
		t.primaryIndex = index
		t.autoIncrementPK = len(index.cols) == 1 && index.cols[0].autoIncrement
	}

	// increment table count on successfull table creation.
	// This ensures that each new table is assigned a unique ID
	// that has not been used before.
	t.maxIndexID++

	return index, nil
}

func (t *Table) newColumn(spec *ColSpec) (*Column, error) {
	if isReservedCol(spec.colName) {
		return nil, fmt.Errorf("%w(%s)", ErrReservedWord, spec.colName)
	}

	if spec.autoIncrement {
		return nil, fmt.Errorf("%w (%s)", ErrLimitedAutoIncrement, spec.colName)
	}

	if spec.notNull {
		return nil, fmt.Errorf("%w (%s)", ErrNewColumnMustBeNullable, spec.colName)
	}

	if !validMaxLenForType(spec.maxLen, spec.colType) {
		return nil, fmt.Errorf("%w (%s)", ErrLimitedMaxLen, spec.colName)
	}

	_, exists := t.colsByName[spec.colName]
	if exists {
		return nil, fmt.Errorf("%w (%s)", ErrColumnAlreadyExists, spec.colName)
	}

	t.maxColID++

	col := &Column{
		id:            t.maxColID,
		table:         t,
		colName:       spec.colName,
		colType:       spec.colType,
		maxLen:        spec.maxLen,
		autoIncrement: spec.autoIncrement,
		notNull:       spec.notNull,
	}

	t.cols = append(t.cols, col)
	t.colsByID[col.id] = col
	t.colsByName[col.colName] = col

	return col, nil
}

func (ctlg *Catalog) renameTable(oldName, newName string) (*Table, error) {
	if oldName == newName {
		return nil, fmt.Errorf("%w (%s)", ErrSameOldAndNewNames, oldName)
	}

	t, err := ctlg.GetTableByName(oldName)
	if err != nil {
		return nil, err
	}

	_, err = ctlg.GetTableByName(newName)
	if err == nil {
		return nil, fmt.Errorf("%w (%s)", ErrTableAlreadyExists, newName)
	}

	t.name = newName

	delete(ctlg.tablesByName, oldName)
	ctlg.tablesByName[newName] = t

	return t, nil
}

func (t *Table) renameColumn(oldName, newName string) (*Column, error) {
	if isReservedCol(newName) {
		return nil, fmt.Errorf("%w(%s)", ErrReservedWord, newName)
	}

	if oldName == newName {
		return nil, fmt.Errorf("%w (%s)", ErrSameOldAndNewNames, oldName)
	}

	col, exists := t.colsByName[oldName]
	if !exists {
		return nil, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, oldName)
	}

	_, exists = t.colsByName[newName]
	if exists {
		return nil, fmt.Errorf("%w (%s)", ErrColumnAlreadyExists, newName)
	}

	col.colName = newName

	delete(t.colsByName, oldName)
	t.colsByName[newName] = col

	return col, nil
}

func (t *Table) deleteColumn(col *Column) error {
	isIndexed, err := t.IsIndexed(col.colName)
	if err != nil {
		return err
	}

	if isIndexed {
		return fmt.Errorf("%w %s because one or more indexes require it", ErrCannotDropColumn, col.colName)
	}

	newCols := make([]*Column, 0, len(t.cols)-1)

	for _, c := range t.cols {
		if c.id != col.id {
			newCols = append(newCols, c)
		}
	}

	t.cols = newCols
	delete(t.colsByName, col.colName)
	delete(t.colsByID, col.id)

	return nil
}

func (t *Table) deleteCheck(name string) (uint32, error) {
	c, exists := t.checkConstraints[name]
	if !exists {
		return 0, fmt.Errorf("%s.%s: %w", t.name, name, ErrConstraintNotFound)
	}

	delete(t.checkConstraints, name)
	return c.id, nil
}

func (t *Table) deleteIndex(index *Index) error {
	if index.IsPrimary() {
		return fmt.Errorf("%w: primary key index can NOT be deleted", ErrIllegalArguments)
	}

	newIndexes := make([]*Index, 0, len(t.indexes)-1)

	for _, i := range t.indexes {
		if i.id != index.id {
			newIndexes = append(newIndexes, i)
		}
	}

	t.indexes = newIndexes
	delete(t.indexesByColID, index.id)
	delete(t.indexesByName, index.Name())

	return nil
}

func (c *Column) ID() uint32 {
	return c.id
}

func (c *Column) Name() string {
	return c.colName
}

func (c *Column) Type() SQLValueType {
	return c.colType
}

func (c *Column) MaxLen() int {
	switch c.colType {
	case BooleanType:
		return 1
	case IntegerType:
		return 8
	case TimestampType:
		return 8
	case Float64Type:
		return 8
	case UUIDType:
		return 16
	}

	return c.maxLen
}

func (c *Column) IsNullable() bool {
	return !c.notNull
}

func (c *Column) IsAutoIncremental() bool {
	return c.autoIncrement
}

func validMaxLenForType(maxLen int, sqlType SQLValueType) bool {
	switch sqlType {
	case BooleanType:
		return maxLen <= 1
	case IntegerType:
		return maxLen == 0 || maxLen == 8
	case Float64Type:
		return maxLen == 0 || maxLen == 8
	case TimestampType:
		return maxLen == 0 || maxLen == 8
	case UUIDType:
		return maxLen == 0 || maxLen == 16
	}

	return maxLen >= 0
}

func (catlg *Catalog) load(ctx context.Context, tx *store.OngoingTx) error {
	return catlg.loadCatalog(ctx, tx, false)
}

func (catlg *Catalog) loadCatalog(ctx context.Context, tx *store.OngoingTx, copyToTx bool) error {
	prefix := MapKey(catlg.enginePrefix, catalogTablePrefix, EncodeID(1))

	return iteratePrefix(ctx, tx, prefix, func(key, value []byte, deleted bool) error {
		dbID, tableID, err := unmapTableID(catlg.enginePrefix, key)
		if err != nil {
			return err
		}

		if dbID != DatabaseID {
			return ErrCorruptedData
		}

		if deleted {
			catlg.maxTableID++
			return nil
		}

		colSpecs, maxColID, err := loadColSpecs(ctx, tableID, tx, catlg.enginePrefix, copyToTx)
		if err != nil {
			return err
		}

		checks, err := loadCheckConstraints(ctx, dbID, tableID, tx, catlg.enginePrefix, copyToTx)
		if err != nil {
			return err
		}

		table, err := catlg.newTable(string(value), colSpecs, checks, maxColID)
		if err != nil {
			return err
		}

		if tableID != table.id {
			return ErrCorruptedData
		}

		if copyToTx {
			if err := tx.Set(key, nil, value); err != nil {
				return err
			}
		}
		return table.loadIndexes(ctx, catlg.enginePrefix, tx, copyToTx)
	})
}

func loadMaxPK(ctx context.Context, sqlPrefix []byte, tx *store.OngoingTx, table *Table) ([]byte, error) {
	pkReaderSpec := store.KeyReaderSpec{
		Prefix:    MapKey(sqlPrefix, MappedPrefix, EncodeID(table.id), EncodeID(table.primaryIndex.id)),
		DescOrder: true,
	}

	pkReader, err := tx.NewKeyReader(pkReaderSpec)
	if err != nil {
		return nil, err
	}
	defer pkReader.Close()

	mkey, _, err := pkReader.Read(ctx)
	if err != nil {
		return nil, err
	}

	return unmapIndexEntry(table.primaryIndex, sqlPrefix, mkey)
}

func loadColSpecs(ctx context.Context, tableID uint32, tx *store.OngoingTx, sqlPrefix []byte, copyToTx bool) (map[uint32]*ColSpec, uint32, error) {
	prefix := MapKey(sqlPrefix, catalogColumnPrefix, EncodeID(1), EncodeID(tableID))

	var maxColID uint32
	specs := make(map[uint32]*ColSpec)

	err := iteratePrefix(ctx, tx, prefix, func(key, value []byte, deleted bool) error {
		if deleted {
			maxColID++
			return nil
		}

		colSpec, colID, err := loadColSpec(sqlPrefix, key, value, tableID)
		if err != nil {
			return err
		}

		maxColID++

		specs[colID] = colSpec

		if copyToTx {
			return tx.Set(key, nil, value)
		}
		return nil
	})
	return specs, maxColID, err
}

func loadColSpec(sqlPrefix, key, value []byte, tableID uint32) (*ColSpec, uint32, error) {
	if len(value) < 6 {
		return nil, 0, ErrCorruptedData
	}

	mdbID, mtableID, colID, colType, err := unmapColSpec(sqlPrefix, key)
	if err != nil {
		return nil, 0, err
	}

	if mdbID != 1 || tableID != mtableID {
		return nil, 0, ErrCorruptedData
	}

	return &ColSpec{
		colName:       string(value[5:]),
		colType:       colType,
		maxLen:        int(binary.BigEndian.Uint32(value[1:])),
		autoIncrement: value[0]&autoIncrementFlag != 0,
		notNull:       value[0]&nullableFlag != 0,
	}, colID, nil
}

func loadCheckConstraints(ctx context.Context, dbID, tableID uint32, tx *store.OngoingTx, sqlPrefix []byte, copyToTx bool) (map[string]CheckConstraint, error) {
	prefix := MapKey(sqlPrefix, catalogCheckPrefix, EncodeID(dbID), EncodeID(tableID))
	checks := make(map[string]CheckConstraint)

	err := iteratePrefix(ctx, tx, prefix, func(key, value []byte, deleted bool) error {
		if deleted {
			return nil
		}

		check, err := parseCheckConstraint(sqlPrefix, key, value)
		if err != nil {
			return err
		}
		checks[check.name] = *check

		if copyToTx {
			return tx.Set(key, nil, value)
		}
		return nil
	})
	return checks, err
}

func (table *Table) loadIndexes(ctx context.Context, sqlPrefix []byte, tx *store.OngoingTx, copyToTx bool) error {
	prefix := MapKey(sqlPrefix, catalogIndexPrefix, EncodeID(1), EncodeID(table.id))

	return iteratePrefix(ctx, tx, prefix, func(key, value []byte, deleted bool) error {
		dbID, tableID, indexID, err := unmapIndex(sqlPrefix, key)
		if err != nil {
			return err
		}

		if table.id != tableID || dbID != 1 {
			return ErrCorruptedData
		}

		if deleted {
			table.maxIndexID++
			return nil
		}

		if copyToTx {
			if err := tx.Set(key, nil, value); err != nil {
				return err
			}
		} else {
			// v={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)}
			colSpecLen := EncIDLen + 1
			if len(value) < 1+colSpecLen || len(value)%colSpecLen != 1 {
				return ErrCorruptedData
			}

			var colIDs []uint32
			for i := 1; i < len(value); i += colSpecLen {
				colID := binary.BigEndian.Uint32(value[i:])

				// TODO: currently only ASC order is supported
				if value[i+EncIDLen] != 0 {
					return ErrCorruptedData
				}
				colIDs = append(colIDs, colID)
			}

			index, err := table.newIndex(value[0] > 0, colIDs)
			if err != nil {
				return err
			}

			if indexID != index.id {
				return ErrCorruptedData
			}
		}
		return nil
	})
}

func trimPrefix(prefix, mkey []byte, mappingPrefix []byte) ([]byte, error) {
	if len(prefix)+len(mappingPrefix) > len(mkey) ||
		!bytes.Equal(prefix, mkey[:len(prefix)]) ||
		!bytes.Equal(mappingPrefix, mkey[len(prefix):len(prefix)+len(mappingPrefix)]) {
		return nil, ErrIllegalMappedKey
	}

	return mkey[len(prefix)+len(mappingPrefix):], nil
}

func unmapTableID(prefix, mkey []byte) (dbID, tableID uint32, err error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogTablePrefix))
	if err != nil {
		return 0, 0, err
	}

	if len(encID) != EncIDLen*2 {
		return 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])

	return
}

func unmapCheckID(prefix, mkey []byte) (uint32, error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogCheckPrefix))
	if err != nil {
		return 0, err
	}

	if len(encID) < 3*EncIDLen {
		return 0, ErrCorruptedData
	}
	return binary.BigEndian.Uint32(encID[2*EncIDLen:]), nil
}

func parseCheckConstraint(prefix, key, value []byte) (*CheckConstraint, error) {
	id, err := unmapCheckID(prefix, key)
	if err != nil {
		return nil, err
	}

	nameLen := value[0] + 1
	name := string(value[1 : 1+nameLen])

	exp, err := ParseExpFromString(string(value[1+nameLen:]))
	if err != nil {
		return nil, err
	}

	return &CheckConstraint{
		id:   id,
		name: name,
		exp:  exp,
	}, nil
}

func unmapColSpec(prefix, mkey []byte) (dbID, tableID, colID uint32, colType SQLValueType, err error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogColumnPrefix))
	if err != nil {
		return 0, 0, 0, "", err
	}

	if len(encID) < EncIDLen*3 {
		return 0, 0, 0, "", ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])
	colID = binary.BigEndian.Uint32(encID[2*EncIDLen:])

	colType, err = asType(string(encID[EncIDLen*3:]))
	if err != nil {
		return 0, 0, 0, "", ErrCorruptedData
	}

	return
}

func asType(t string) (SQLValueType, error) {
	switch t {
	case IntegerType,
		Float64Type,
		BooleanType,
		VarcharType,
		UUIDType,
		BLOBType,
		TimestampType,
		JSONType:
		return t, nil
	}
	return t, ErrCorruptedData
}

func unmapIndex(sqlPrefix, mkey []byte) (dbID, tableID, indexID uint32, err error) {
	encID, err := trimPrefix(sqlPrefix, mkey, []byte(catalogIndexPrefix))
	if err != nil {
		return 0, 0, 0, err
	}

	if len(encID) != EncIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])
	indexID = binary.BigEndian.Uint32(encID[EncIDLen*2:])

	return
}

func unmapIndexEntry(index *Index, sqlPrefix, mkey []byte) (encPKVals []byte, err error) {
	if index == nil {
		return nil, ErrIllegalArguments
	}

	enc, err := trimPrefix(sqlPrefix, mkey, []byte(MappedPrefix))
	if err != nil {
		return nil, ErrCorruptedData
	}

	if len(enc) <= EncIDLen*2 {
		return nil, ErrCorruptedData
	}

	off := 0

	tableID := binary.BigEndian.Uint32(enc[off:])
	off += EncIDLen

	indexID := binary.BigEndian.Uint32(enc[off:])
	off += EncIDLen

	if tableID != index.table.id || indexID != index.id {
		return nil, ErrCorruptedData
	}

	//read index values
	for _, col := range index.cols {
		if enc[off] == KeyValPrefixNull {
			off += 1
			continue
		}
		if enc[off] != KeyValPrefixNotNull {
			return nil, ErrCorruptedData
		}
		off += 1

		maxLen := col.MaxLen()
		if variableSizedType(col.colType) {
			maxLen += EncLenLen
		}
		if len(enc)-off < maxLen {
			return nil, ErrCorruptedData
		}

		off += maxLen
	}

	//PK cannot be nil
	if len(enc)-off < 1 {
		return nil, ErrCorruptedData
	}

	return enc[off:], nil
}

func variableSizedType(sqlType SQLValueType) bool {
	return sqlType == VarcharType || sqlType == BLOBType
}

func MapKey(prefix []byte, mappingPrefix string, encValues ...[]byte) []byte {
	mkeyLen := len(prefix) + len(mappingPrefix)

	for _, ev := range encValues {
		mkeyLen += len(ev)
	}

	mkey := make([]byte, mkeyLen)

	off := 0

	copy(mkey, prefix)
	off += len(prefix)

	copy(mkey[off:], []byte(mappingPrefix))
	off += len(mappingPrefix)

	for _, ev := range encValues {
		copy(mkey[off:], ev)
		off += len(ev)
	}

	return mkey
}

func EncodeID(id uint32) []byte {
	var encID [EncIDLen]byte
	binary.BigEndian.PutUint32(encID[:], id)
	return encID[:]
}

const (
	KeyValPrefixNull       byte = 0x20
	KeyValPrefixNotNull    byte = 0x80
	KeyValPrefixUpperBound byte = 0xFF
)

func EncodeValueAsKey(val TypedValue, colType SQLValueType, maxLen int) ([]byte, int, error) {
	return EncodeRawValueAsKey(val.RawValue(), colType, maxLen)
}

// EncodeRawValueAsKey encodes a value in a b-tree meaningful way.
func EncodeRawValueAsKey(val interface{}, colType SQLValueType, maxLen int) ([]byte, int, error) {
	if maxLen <= 0 {
		return nil, 0, ErrInvalidValue
	}
	if maxLen > MaxKeyLen {
		return nil, 0, ErrMaxKeyLengthExceeded
	}

	convVal, err := mayApplyImplicitConversion(val, colType)
	if err != nil {
		return nil, 0, err
	}

	if convVal == nil {
		return []byte{KeyValPrefixNull}, 0, nil
	}

	switch colType {
	case VarcharType:
		{
			strVal, ok := convVal.(string)
			if !ok {
				return nil, 0, fmt.Errorf("value is not a string: %w", ErrInvalidValue)
			}

			if len(strVal) > maxLen {
				return nil, 0, ErrMaxLengthExceeded
			}

			// notnull + value + padding + len(value)
			encv := make([]byte, 1+maxLen+EncLenLen)
			encv[0] = KeyValPrefixNotNull
			copy(encv[1:], []byte(strVal))
			binary.BigEndian.PutUint32(encv[len(encv)-EncLenLen:], uint32(len(strVal)))

			return encv, len(strVal), nil
		}
	case IntegerType:
		{
			if maxLen != 8 {
				return nil, 0, ErrCorruptedData
			}

			intVal, ok := convVal.(int64)
			if !ok {
				return nil, 0, fmt.Errorf("value is not an integer: %w", ErrInvalidValue)
			}

			// v
			var encv [9]byte
			encv[0] = KeyValPrefixNotNull
			binary.BigEndian.PutUint64(encv[1:], uint64(intVal))
			// map to unsigned integer space for lexical sorting order
			encv[1] ^= 0x80

			return encv[:], 8, nil
		}
	case BooleanType:
		{
			if maxLen != 1 {
				return nil, 0, ErrCorruptedData
			}

			boolVal, ok := convVal.(bool)
			if !ok {
				return nil, 0, fmt.Errorf("value is not a boolean: %w", ErrInvalidValue)
			}

			// v
			var encv [2]byte
			encv[0] = KeyValPrefixNotNull
			if boolVal {
				encv[1] = 1
			}

			return encv[:], 1, nil
		}
	case BLOBType:
		{
			blobVal, ok := convVal.([]byte)
			if !ok {
				return nil, 0, fmt.Errorf("value is not a blob: %w", ErrInvalidValue)
			}

			if len(blobVal) > maxLen {
				return nil, 0, ErrMaxLengthExceeded
			}

			// notnull + value + padding + len(value)
			encv := make([]byte, 1+maxLen+EncLenLen)
			encv[0] = KeyValPrefixNotNull
			copy(encv[1:], []byte(blobVal))
			binary.BigEndian.PutUint32(encv[len(encv)-EncLenLen:], uint32(len(blobVal)))

			return encv, len(blobVal), nil
		}
	case UUIDType:
		{
			uuidVal, ok := convVal.(uuid.UUID)
			if !ok {
				return nil, 0, fmt.Errorf("value is not an UUID: %w", ErrInvalidValue)
			}

			// notnull + value
			encv := make([]byte, 17)
			encv[0] = KeyValPrefixNotNull
			copy(encv[1:], uuidVal[:])

			return encv, 16, nil
		}
	case TimestampType:
		{
			if maxLen != 8 {
				return nil, 0, ErrCorruptedData
			}

			timeVal, ok := convVal.(time.Time)
			if !ok {
				return nil, 0, fmt.Errorf("value is not a timestamp: %w", ErrInvalidValue)
			}

			// v
			var encv [9]byte
			encv[0] = KeyValPrefixNotNull
			binary.BigEndian.PutUint64(encv[1:], uint64(timeVal.UnixNano()))
			// map to unsigned integer space for lexical sorting order
			encv[1] ^= 0x80

			return encv[:], 8, nil
		}
	case Float64Type:
		{
			floatVal, ok := convVal.(float64)
			if !ok {
				return nil, 0, fmt.Errorf("value is not a float: %w", ErrInvalidValue)
			}

			// Apart form the sign bit, bit representation of float64
			// can be sorted lexicographically
			floatBits := math.Float64bits(floatVal)

			var encv [9]byte
			encv[0] = KeyValPrefixNotNull
			binary.BigEndian.PutUint64(encv[1:], floatBits)

			if encv[1]&0x80 != 0 {
				// For negative numbers, the order must be reversed,
				// we also negate the sign bit so that all negative
				// numbers end up in the smaller half of values
				for i := 1; i < 9; i++ {
					encv[i] = ^encv[i]
				}
			} else {
				// For positive numbers, the order is already correct,
				// we only have to set the sign bit to 1 to ensure that
				// positive numbers end in the larger half of values
				encv[1] ^= 0x80
			}

			return encv[:], 8, nil
		}
	}

	return nil, 0, ErrInvalidValue
}

func getEncodeRawValue(val TypedValue, colType SQLValueType) (interface{}, error) {
	if colType != JSONType || val.Type() == JSONType {
		return val.RawValue(), nil
	}

	if val.Type() != VarcharType {
		return nil, fmt.Errorf("%w: invalid json value", ErrInvalidValue)
	}
	s, _ := val.RawValue().(string)

	raw := json.RawMessage(s)
	if !json.Valid(raw) {
		return nil, fmt.Errorf("%w: invalid json value", ErrInvalidValue)
	}
	return raw, nil
}

func EncodeValue(val TypedValue, colType SQLValueType, maxLen int) ([]byte, error) {
	v, err := getEncodeRawValue(val, colType)
	if err != nil {
		return nil, err
	}
	return EncodeRawValue(v, colType, maxLen, false)
}

func EncodeNullableValue(val TypedValue, colType SQLValueType, maxLen int) ([]byte, error) {
	v, err := getEncodeRawValue(val, colType)
	if err != nil {
		return nil, err
	}
	return EncodeRawValue(v, colType, maxLen, true)
}

// EncodeRawValue encode a value in a byte format. This is the internal binary representation of a value. Can be decoded with DecodeValue.
func EncodeRawValue(val interface{}, colType SQLValueType, maxLen int, nullable bool) ([]byte, error) {
	convVal, err := mayApplyImplicitConversion(val, colType)
	if err != nil {
		return nil, err
	}

	if convVal == nil && !nullable {
		return nil, ErrInvalidValue
	}

	if convVal == nil {
		encv := make([]byte, EncLenLen)
		binary.BigEndian.PutUint32(encv[:], uint32(0))
		return encv, nil
	}

	switch colType {
	case VarcharType:
		{
			strVal, ok := convVal.(string)
			if !ok {
				return nil, fmt.Errorf("value is not a string: %w", ErrInvalidValue)
			}

			if maxLen > 0 && len(strVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(strVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal)))
			copy(encv[EncLenLen:], []byte(strVal))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := convVal.(int64)
			if !ok {
				return nil, fmt.Errorf("value is not an integer: %w", ErrInvalidValue)
			}

			// map to unsigned integer space
			// len(v) + v
			var encv [EncLenLen + 8]byte
			binary.BigEndian.PutUint32(encv[:], uint32(8))
			binary.BigEndian.PutUint64(encv[EncLenLen:], uint64(intVal))

			return encv[:], nil
		}
	case BooleanType:
		{
			boolVal, ok := convVal.(bool)
			if !ok {
				return nil, fmt.Errorf("value is not a boolean: %w", ErrInvalidValue)
			}

			// len(v) + v
			var encv [EncLenLen + 1]byte
			binary.BigEndian.PutUint32(encv[:], uint32(1))
			if boolVal {
				encv[EncLenLen] = 1
			}

			return encv[:], nil
		}
	case BLOBType:
		{
			var blobVal []byte

			if val != nil {
				v, ok := convVal.([]byte)
				if !ok {
					return nil, fmt.Errorf("value is not a blob: %w", ErrInvalidValue)
				}
				blobVal = v
			}

			if maxLen > 0 && len(blobVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(blobVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(blobVal)))
			copy(encv[EncLenLen:], blobVal)

			return encv[:], nil
		}
	case JSONType:
		rawJson, ok := val.(json.RawMessage)
		if !ok {
			data, err := json.Marshal(val)
			if err != nil {
				return nil, err
			}
			rawJson = data
		}

		// len(v) + v
		encv := make([]byte, EncLenLen+len(rawJson))
		binary.BigEndian.PutUint32(encv[:], uint32(len(rawJson)))
		copy(encv[EncLenLen:], rawJson)

		return encv[:], nil
	case UUIDType:
		{
			uuidVal, ok := convVal.(uuid.UUID)
			if !ok {
				return nil, fmt.Errorf("value is not an UUID: %w", ErrInvalidValue)
			}

			// len(v) + v
			var encv [EncLenLen + 16]byte
			binary.BigEndian.PutUint32(encv[:], uint32(16))
			copy(encv[EncLenLen:], uuidVal[:])

			return encv[:], nil
		}
	case TimestampType:
		{
			timeVal, ok := convVal.(time.Time)
			if !ok {
				return nil, fmt.Errorf("value is not a timestamp: %w", ErrInvalidValue)
			}

			// len(v) + v
			var encv [EncLenLen + 8]byte
			binary.BigEndian.PutUint32(encv[:], uint32(8))
			binary.BigEndian.PutUint64(encv[EncLenLen:], uint64(TimeToInt64(timeVal)))

			return encv[:], nil
		}
	case Float64Type:
		{
			floatVal, ok := convVal.(float64)
			if !ok {
				return nil, fmt.Errorf("value is not a float: %w", ErrInvalidValue)
			}

			var encv [EncLenLen + 8]byte
			floatBits := math.Float64bits(floatVal)
			binary.BigEndian.PutUint32(encv[:], uint32(8))
			binary.BigEndian.PutUint64(encv[EncLenLen:], floatBits)

			return encv[:], nil
		}
	}

	return nil, ErrInvalidValue
}

func DecodeValueLength(b []byte) (int, int, error) {
	if len(b) < EncLenLen {
		return 0, 0, ErrCorruptedData
	}

	vlen := int(binary.BigEndian.Uint32(b[:]))
	voff := EncLenLen

	if vlen < 0 || len(b) < voff+vlen {
		return 0, 0, ErrCorruptedData
	}

	return vlen, EncLenLen, nil
}

func DecodeValue(b []byte, colType SQLValueType) (TypedValue, int, error) {
	return decodeValue(b, colType, false)
}

func DecodeNullableValue(b []byte, colType SQLValueType) (TypedValue, int, error) {
	return decodeValue(b, colType, true)
}

func decodeValue(b []byte, colType SQLValueType, nullable bool) (TypedValue, int, error) {
	vlen, voff, err := DecodeValueLength(b)
	if err != nil {
		return nil, 0, err
	}

	if vlen == 0 && nullable {
		return &NullValue{t: colType}, voff, nil
	}

	switch colType {
	case VarcharType:
		{
			v := string(b[voff : voff+vlen])
			voff += vlen

			return &Varchar{val: v}, voff, nil
		}
	case IntegerType:
		{
			if vlen != 8 {
				return nil, 0, ErrCorruptedData
			}

			v := binary.BigEndian.Uint64(b[voff:])
			voff += vlen

			return &Integer{val: int64(v)}, voff, nil
		}
	case BooleanType:
		{
			if vlen != 1 {
				return nil, 0, ErrCorruptedData
			}

			v := b[voff] == 1
			voff += 1

			return &Bool{val: v}, voff, nil
		}
	case BLOBType:
		{
			v := b[voff : voff+vlen]
			voff += vlen

			return &Blob{val: v}, voff, nil
		}
	case JSONType:
		{
			v := b[voff : voff+vlen]
			voff += vlen

			var val interface{}
			err = json.Unmarshal(v, &val)

			return &JSON{val: val}, voff, err
		}
	case UUIDType:
		{
			if vlen != 16 {
				return nil, 0, ErrCorruptedData
			}

			u, err := uuid.FromBytes(b[voff : voff+16])
			if err != nil {
				return nil, 0, fmt.Errorf("%w: %s", ErrCorruptedData, err.Error())
			}

			voff += vlen

			return &UUID{val: u}, voff, nil
		}
	case TimestampType:
		{
			if vlen != 8 {
				return nil, 0, ErrCorruptedData
			}

			v := binary.BigEndian.Uint64(b[voff:])
			voff += vlen

			return &Timestamp{val: TimeFromInt64(int64(v))}, voff, nil
		}
	case Float64Type:
		{
			if vlen != 8 {
				return nil, 0, ErrCorruptedData
			}
			v := binary.BigEndian.Uint64(b[voff:])
			voff += vlen
			return &Float64{val: math.Float64frombits(v)}, voff, nil
		}
	}

	return nil, 0, ErrCorruptedData
}

// addSchemaToTx adds the schema of the catalog to the given transaction.
func (catlg *Catalog) addSchemaToTx(ctx context.Context, tx *store.OngoingTx) error {
	return catlg.loadCatalog(ctx, tx, true)
}

func iteratePrefix(ctx context.Context, tx *store.OngoingTx, prefix []byte, onSpec func(key, value []byte, deleted bool) error) error {
	dbReaderSpec := store.KeyReaderSpec{
		Prefix: prefix,
	}

	colSpecReader, err := tx.NewKeyReader(dbReaderSpec)
	if err != nil {
		return err
	}
	defer colSpecReader.Close()

	for {
		mkey, vref, err := colSpecReader.Read(ctx)
		if errors.Is(err, store.ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return err
		}

		md := vref.KVMetadata()
		if md != nil && md.IsExpirable() {
			return ErrBrokenCatalogColSpecExpirable
		}

		deleted := md != nil && md.Deleted()
		var v []byte
		if !deleted {
			v, err = vref.Resolve()
			if err != nil {
				return err
			}
		}

		err = onSpec(mkey, v, deleted)
		if err != nil {
			return err
		}
	}
	return nil
}
