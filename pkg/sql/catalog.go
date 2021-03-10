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
}

func newCatalog() *Catalog {
	return &Catalog{
		dbsByID:   map[uint64]*Database{},
		dbsByName: map[string]*Database{},
	}
}

func (c *Catalog) newDatabase(name string) (*Database, error) {
	exists := c.ExistDatabase(name)
	if exists {
		return nil, ErrDatabaseAlreadyExists
	}

	id := len(c.dbsByID) + 1

	db := &Database{
		id:           uint64(id),
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

func (c *Catalog) ExistDatabase(db string) bool {
	_, exists := c.dbsByName[db]
	return exists
}

func (db *Database) ExistTable(table string) bool {
	_, exists := db.tablesByName[table]
	return exists
}

func (db *Database) newTable(name string, colsSpec []*ColSpec, pk string) (*Table, error) {
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
