/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package database

import (
	"sync"
)

// DatabaseList interface
type DatabaseList interface {
	Put(database DB)
	Delete(string) (DB, error)
	GetByIndex(index int) (DB, error)
	GetByName(string) (DB, error)
	GetId(dbname string) int
	Length() int
}

type databaseList struct {
	databases      []DB
	databaseByName map[string]*dbRef
	sync.RWMutex
}

type dbRef struct {
	index   int
	deleted bool
}

// NewDatabaseList constructs a new database list
func NewDatabaseList() DatabaseList {
	return &databaseList{
		databases:      make([]DB, 0),
		databaseByName: make(map[string]*dbRef),
	}
}

func (d *databaseList) Put(database DB) {
	d.Lock()
	defer d.Unlock()

	ref, exists := d.databaseByName[database.GetName()]
	if exists {
		d.databases[ref.index] = database
		ref.deleted = false
		return
	}

	d.databases = append(d.databases, database)
	d.databaseByName[database.GetName()] = &dbRef{index: len(d.databases) - 1}
}

func (d *databaseList) Delete(dbname string) (DB, error) {
	d.Lock()
	defer d.Unlock()

	dbRef, exists := d.databaseByName[dbname]
	if !exists || dbRef.deleted {
		return nil, ErrDatabaseNotExists
	}

	db := d.databases[dbRef.index]

	if !db.IsClosed() {
		return nil, ErrCannotDeleteAnOpenDatabase
	}

	dbRef.deleted = true

	return db, nil
}

func (d *databaseList) GetByIndex(index int) (DB, error) {
	d.RLock()
	defer d.RUnlock()

	if index < 0 || index >= len(d.databases) {
		return nil, ErrDatabaseNotExists
	}

	db := d.databases[index]

	dbRef := d.databaseByName[db.GetName()]
	if dbRef.deleted {
		return nil, ErrDatabaseNotExists
	}

	return db, nil
}

func (d *databaseList) GetByName(dbname string) (DB, error) {
	d.RLock()
	defer d.RUnlock()

	if dbRef, ok := d.databaseByName[dbname]; !ok || dbRef.deleted {
		return nil, ErrDatabaseNotExists
	}

	return d.databases[d.databaseByName[dbname].index], nil
}

func (d *databaseList) Length() int {
	d.RLock()
	defer d.RUnlock()

	return len(d.databases)
}

// GetById returns the database id number. -1 if database is not present
func (d *databaseList) GetId(dbname string) int {
	d.RLock()
	defer d.RUnlock()

	if dbRef, ok := d.databaseByName[dbname]; ok && !dbRef.deleted {
		return dbRef.index
	}

	return -1
}
