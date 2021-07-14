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

package database

import "sync"

// DatabaseList interface
type DatabaseList interface {
	Append(database DB)
	GetByIndex(index int64) DB
	GetByName(string) (DB, error)
	GetId(dbname string) int64
	Length() int
}

type databaseList struct {
	databases           []DB
	databasenameToIndex map[string]int64
	sync.RWMutex
}

//NewDatabaseList constructs a new database list
func NewDatabaseList() DatabaseList {
	return &databaseList{
		databasenameToIndex: make(map[string]int64),
		databases:           make([]DB, 0),
	}
}

func (d *databaseList) Append(database DB) {
	d.Lock()
	defer d.Unlock()

	d.databasenameToIndex[database.GetName()] = int64(len(d.databases))
	d.databases = append(d.databases, database)
}

func (d *databaseList) GetByIndex(index int64) DB {
	d.RLock()
	defer d.RUnlock()

	return d.databases[index]
}

func (d *databaseList) GetByName(dbname string) (DB, error) {
	d.RLock()
	defer d.RUnlock()

	if _, ok := d.databasenameToIndex[dbname]; !ok {
		return nil, ErrDatabaseNotExists
	}

	return d.databases[d.databasenameToIndex[dbname]], nil
}

func (d *databaseList) Length() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.databases)
}

// GetById returns the database id number. -1 if database is not present
func (d *databaseList) GetId(dbname string) int64 {
	d.RLock()
	defer d.RUnlock()

	if id, ok := d.databasenameToIndex[dbname]; ok {
		return id
	}

	return -1
}
