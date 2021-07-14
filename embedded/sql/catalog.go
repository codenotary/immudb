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

type Catalog struct {
	dbsByID   map[uint64]*Database
	dbsByName map[string]*Database
}

type Database struct {
	id           uint64
	name         string
	tablesByID   map[uint64]*Table
	tablesByName map[string]*Table
}

type Table struct {
	db         *Database
	id         uint64
	name       string
	colsByID   map[uint64]*Column
	colsByName map[string]*Column
	pk         *Column
	indexes    map[uint64]struct{}
}

type Column struct {
	table   *Table
	id      uint64
	colName string
	colType SQLValueType
	notNull bool
}

func newCatalog() *Catalog {
	return &Catalog{
		dbsByID:   map[uint64]*Database{},
		dbsByName: map[string]*Database{},
	}
}

func (c *Catalog) ExistDatabase(db string) bool {
	_, exists := c.dbsByName[db]
	return exists
}

func (c *Catalog) newDatabase(id uint64, name string) (*Database, error) {
	exists := c.ExistDatabase(name)
	if exists {
		return nil, ErrDatabaseAlreadyExists
	}

	db := &Database{
		id:           id,
		name:         name,
		tablesByID:   map[uint64]*Table{},
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

func (c *Catalog) GetDatabaseByID(id uint64) (*Database, error) {
	db, exists := c.dbsByID[id]
	if !exists {
		return nil, ErrDatabaseDoesNotExist
	}
	return db, nil
}

func (db *Database) ID() uint64 {
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
	ts := make([]*Table, len(db.tablesByName))

	i := 0
	for _, t := range db.tablesByID {
		ts[i] = t
		i++
	}

	return ts
}

func (db *Database) GetTableByName(name string) (*Table, error) {
	table, exists := db.tablesByName[name]
	if !exists {
		return nil, ErrTableDoesNotExist
	}
	return table, nil
}

func (db *Database) GetTableByID(id uint64) (*Table, error) {
	table, exists := db.tablesByID[id]
	if !exists {
		return nil, ErrTableDoesNotExist
	}
	return table, nil
}

func (t *Table) ID() uint64 {
	return t.id
}

func (t *Table) Database() *Database {
	return t.db
}

func (t *Table) ColsByID() map[uint64]*Column {
	return t.colsByID
}

func (t *Table) ColsByName() map[string]*Column {
	return t.colsByName
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) PrimaryKey() *Column {
	return t.pk
}

func (t *Table) IsIndexed(colName string) (bool, error) {
	c, exists := t.colsByName[colName]
	if !exists {
		return false, ErrColumnDoesNotExist
	}

	_, indexed := t.indexes[c.id]
	return indexed, nil
}

func (t *Table) GetColumnByName(name string) (*Column, error) {
	col, exists := t.colsByName[name]
	if !exists {
		return nil, ErrColumnDoesNotExist
	}
	return col, nil
}

func (t *Table) GetColumnByID(id uint64) (*Column, error) {
	col, exists := t.colsByID[id]
	if !exists {
		return nil, ErrColumnDoesNotExist
	}
	return col, nil
}

func (db *Database) newTable(name string, colsSpec []*ColSpec, pk string) (*Table, error) {
	if len(name) == 0 || len(colsSpec) == 0 || len(pk) == 0 {
		return nil, ErrIllegalArguments
	}

	exists := db.ExistTable(name)
	if exists {
		return nil, ErrTableAlreadyExists
	}

	id := len(db.tablesByID) + 1

	table := &Table{
		id:         uint64(id),
		db:         db,
		name:       name,
		colsByID:   make(map[uint64]*Column, 0),
		colsByName: make(map[string]*Column, 0),
		indexes:    make(map[uint64]struct{}, 0),
	}

	for _, cs := range colsSpec {
		_, colExists := table.colsByName[cs.colName]
		if colExists {
			return nil, ErrDuplicatedColumn
		}

		id := len(table.colsByID) + 1

		col := &Column{
			id:      uint64(id),
			table:   table,
			colName: cs.colName,
			colType: cs.colType,
			notNull: cs.notNull || cs.colName == pk,
		}

		table.colsByID[col.id] = col
		table.colsByName[col.colName] = col

		if pk == col.colName {
			table.pk = col
		}
	}

	if table.pk == nil {
		return nil, ErrInvalidPK
	}

	db.tablesByID[table.id] = table
	db.tablesByName[table.name] = table

	return table, nil
}

func (c *Column) ID() uint64 {
	return c.id
}

func (c *Column) Name() string {
	return c.colName
}

func (c *Column) Type() SQLValueType {
	return c.colType
}

func (c *Column) IsNullable() bool {
	return !c.notNull
}
