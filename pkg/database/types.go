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

import "context"

// DatabaseList interface
type DatabaseList interface {
	Put(name string, opts *Options) DB
	PutClosed(name string, opts *Options) DB
	Delete(string) (DB, error)
	GetByIndex(index int) (DB, error)
	GetByName(string) (DB, error)
	GetId(dbname string) int
	Length() int
	Resize(n int)
	CloseAll(ctx context.Context) error
}

type databaseList struct {
	m *DBManager
}

// NewDatabaseList constructs a new database list
func NewDatabaseList(m *DBManager) DatabaseList {
	return &databaseList{
		m: m,
	}
}

func (d *databaseList) Put(dbName string, opts *Options) DB {
	return d.put(dbName, opts, false)
}

func (d *databaseList) PutClosed(dbName string, opts *Options) DB {
	return d.put(dbName, opts, true)
}

func (d *databaseList) put(dbName string, opts *Options, closed bool) DB {
	var newOpts Options = *opts

	idx := d.m.Put(dbName, &newOpts, closed)

	return &lazyDB{
		m:   d.m,
		idx: idx,
	}
}

func (d *databaseList) Delete(dbname string) (DB, error) {
	if err := d.m.Delete(dbname); err != nil {
		return nil, err
	}
	idx := d.m.GetIndexByName(dbname)
	return &lazyDB{m: d.m, idx: idx}, nil
}

func (d *databaseList) GetByIndex(index int) (DB, error) {
	if !d.m.HasIndex(index) {
		return nil, ErrDatabaseNotExists
	}
	return &lazyDB{m: d.m, idx: index}, nil
}

func (d *databaseList) GetByName(dbname string) (DB, error) {
	idx := d.m.GetIndexByName(dbname)
	if idx < 0 {
		return nil, ErrDatabaseNotExists
	}
	return &lazyDB{m: d.m, idx: idx}, nil
}

func (d *databaseList) Length() int {
	return d.m.Length()
}

// GetById returns the database id number. -1 if database is not present
func (d *databaseList) GetId(dbname string) int {
	return d.m.GetIndexByName(dbname)
}

func (d *databaseList) CloseAll(ctx context.Context) error {
	return d.m.CloseAll(ctx)
}

func (d *databaseList) Resize(n int) {
	d.m.Resize(n)
}
