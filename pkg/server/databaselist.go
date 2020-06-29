/*
Copyright 2019-2020 vChain, Inc.

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
)

type databaseList struct {
	databases []*Db
	sync.RWMutex
}

//NewDatabaseList constructs a new database list
func NewDatabaseList() DatabaseList {
	return &databaseList{
		databases: make([]*Db, 0),
	}
}
func (d *databaseList) Append(database *Db) {
	d.Lock()
	defer d.Unlock()
	d.databases = append(d.databases, database)
}
func (d *databaseList) GetByIndex(index int64) *Db {
	d.RLock()
	defer d.RUnlock()
	return d.databases[index]
}
func (d *databaseList) Length() int {
	d.RLock()
	defer d.RUnlock()
	return len(d.databases)
}
