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

package server

import (
	"sync"

	"github.com/codenotary/immudb/pkg/database"
)

type databaseList struct {
	databases           []database.DB
	databasenameToIndex map[string]int64
	sync.RWMutex
}

//NewDatabaseList constructs a new database list
func NewDatabaseList() database.DatabaseList {
	return &databaseList{
		databasenameToIndex: make(map[string]int64),
		databases:           make([]database.DB, 0),
	}
}
func (d *databaseList) Append(database database.DB) {
	d.Lock()
	defer d.Unlock()
	d.databasenameToIndex[database.GetName()] = int64(d.Length())
	d.databases = append(d.databases, database)
}
func (d *databaseList) GetByIndex(index int64) database.DB {
	d.RLock()
	defer d.RUnlock()
	return d.databases[index]
}
func (d *databaseList) GetByName(dbname string) database.DB {
	d.RLock()
	defer d.RUnlock()
	return d.databases[d.databasenameToIndex[dbname]]
}
func (d *databaseList) Length() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.databases)
}

// GetById returns the database id number. 0 if database is not present
func (d *databaseList) GetId(dbname string) int64 {
	d.RLock()
	defer d.RUnlock()
	if id, ok := d.databasenameToIndex[dbname]; ok {
		return id
	}
	return 0
}
