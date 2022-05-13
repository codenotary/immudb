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
	"fmt"
	"strings"
)

type Catalog struct {
	dbsByID   map[uint32]*Database
	dbsByName map[string]*Database
}

type Database struct {
	id           uint32
	catalog      *Catalog
	name         string
	tables       []*Table
	tablesByID   map[uint32]*Table
	tablesByName map[string]*Table
}

type Table struct {
	db              *Database
	id              uint32
	name            string
	cols            []*Column
	colsByID        map[uint32]*Column
	colsByName      map[string]*Column
	indexes         []*Index
	indexesByName   map[string]*Index
	indexesByColID  map[uint32][]*Index
	primaryIndex    *Index
	autoIncrementPK bool
	maxPK           int64
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

func newCatalog() *Catalog {
	return &Catalog{
		dbsByID:   map[uint32]*Database{},
		dbsByName: map[string]*Database{},
	}
}

func (c *Catalog) ExistDatabase(db string) bool {
	_, exists := c.dbsByName[db]
	return exists
}

func (c *Catalog) newDatabase(id uint32, name string) (*Database, error) {
	exists := c.ExistDatabase(name)
	if exists {
		return nil, ErrDatabaseAlreadyExists
	}

	db := &Database{
		id:           id,
		catalog:      c,
		name:         name,
		tablesByID:   map[uint32]*Table{},
		tablesByName: map[string]*Table{},
	}

	c.dbsByID[db.id] = db
	c.dbsByName[db.name] = db

	return db, nil
}

func (c *Catalog) Databases() []*Database {
	dbs := make([]*Database, len(c.dbsByID))

	i := 0
	for _, db := range c.dbsByID {
		dbs[i] = db
		i++
	}

	return dbs
}

func (c *Catalog) GetDatabaseByName(name string) (*Database, error) {
	db, exists := c.dbsByName[name]
	if !exists {
		return nil, ErrDatabaseDoesNotExist
	}
	return db, nil
}

func (c *Catalog) GetDatabaseByID(id uint32) (*Database, error) {
	db, exists := c.dbsByID[id]
	if !exists {
		return nil, ErrDatabaseDoesNotExist
	}
	return db, nil
}

func (db *Database) ID() uint32 {
	return db.id
}

func (db *Database) Name() string {
	return db.name
}

func (db *Database) ExistTable(table string) bool {
	_, exists := db.tablesByName[table]
	return exists
}

func (c *Catalog) GetTableByName(dbName, tableName string) (*Table, error) {
	db, err := c.GetDatabaseByName(dbName)
	if err != nil {
		return nil, err
	}
	return db.GetTableByName(tableName)
}

func (db *Database) GetTables() []*Table {
	return db.tables
}

func (db *Database) GetTableByName(name string) (*Table, error) {
	table, exists := db.tablesByName[name]
	if !exists {
		return nil, fmt.Errorf("%w (%s)", ErrTableDoesNotExist, name)
	}
	return table, nil
}

func (db *Database) GetTableByID(id uint32) (*Table, error) {
	table, exists := db.tablesByID[id]
	if !exists {
		return nil, ErrTableDoesNotExist
	}
	return table, nil
}

func (t *Table) ID() uint32 {
	return t.id
}

func (t *Table) Database() *Database {
	return t.db
}

func (t *Table) Cols() []*Column {
	return t.cols
}

func (t *Table) ColsByName() map[string]*Column {
	return t.colsByName
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) PrimaryIndex() *Index {
	return t.primaryIndex
}

func (t *Table) IsIndexed(colName string) (indexed bool, err error) {
	c, exists := t.colsByName[colName]
	if !exists {
		return false, fmt.Errorf("%w (%s)", ErrColumnDoesNotExist, colName)
	}

	_, ok := t.indexesByColID[c.id]

	return ok, nil
}

func (t *Table) IndexesByColID(colID uint32) []*Index {
	return t.indexesByColID[colID]
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

func (i *Index) sortableUsing(colID uint32, rangesByColID map[uint32]*typedValueRange) bool {
	// all columns before colID must be fixedValues otherwise the index can not be used
	for _, col := range i.cols {
		if col.id == colID {
			return true
		}

		colRange, ok := rangesByColID[col.id]
		if ok && colRange.unitary() {
			continue
		}

		return false
	}
	return false
}

func (i *Index) prefix() string {
	if i.IsPrimary() {
		return PIndexPrefix
	}

	if i.IsUnique() {
		return UIndexPrefix
	}

	return SIndexPrefix
}

func (i *Index) Name() string {
	return indexName(i.table.name, i.cols)
}

func indexName(tableName string, cols []*Column) string {
	var buf strings.Builder

	buf.WriteString(tableName)

	buf.WriteString("[")

	for c, col := range cols {
		buf.WriteString(col.colName)

		if c < len(cols)-1 {
			buf.WriteString(",")
		}
	}

	buf.WriteString("]")

	return buf.String()
}

func (db *Database) newTable(name string, colsSpec []*ColSpec) (table *Table, err error) {
	if len(name) == 0 || len(colsSpec) == 0 {
		return nil, ErrIllegalArguments
	}

	exists := db.ExistTable(name)
	if exists {
		return nil, fmt.Errorf("%w (%s)", ErrTableAlreadyExists, name)
	}

	id := len(db.tables) + 1

	table = &Table{
		id:             uint32(id),
		db:             db,
		name:           name,
		cols:           make([]*Column, len(colsSpec)),
		colsByID:       make(map[uint32]*Column),
		colsByName:     make(map[string]*Column),
		indexesByName:  make(map[string]*Index),
		indexesByColID: make(map[uint32][]*Index),
	}

	for i, cs := range colsSpec {
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

		id := len(table.colsByID) + 1

		col := &Column{
			id:            uint32(id),
			table:         table,
			colName:       cs.colName,
			colType:       cs.colType,
			maxLen:        cs.maxLen,
			autoIncrement: cs.autoIncrement,
			notNull:       cs.notNull,
		}

		table.cols[i] = col
		table.colsByID[col.id] = col
		table.colsByName[col.colName] = col
	}

	db.tables = append(db.tables, table)
	db.tablesByID[table.id] = table
	db.tablesByName[table.name] = table

	return table, nil
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
		id:       uint32(len(t.indexes)),
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

	return index, nil
}

func (t *Table) newColumn(spec *ColSpec) (*Column, error) {
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

	id := len(t.cols) + 1

	col := &Column{
		id:            uint32(id),
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

func (t *Table) renameColumn(oldName, newName string) (*Column, error) {
	if oldName == newName {
		return nil, fmt.Errorf("%w (%s)", ErrSameOldAndNewColumnName, oldName)
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
	case TimestampType:
		return maxLen == 0 || maxLen == 8
	}

	return maxLen >= 0
}
