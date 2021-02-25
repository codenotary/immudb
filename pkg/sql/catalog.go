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
	"errors"
	"math"
	"strings"
	"sync"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

const patternSeparator = "/"

const catalogDatabasePrefix = "CATALOG/DATABASE/"
const catalogDatabase = catalogDatabasePrefix + "%s" // e.g. CATALOG/DATABASE/db1

const catalogTablePrefix = "CATALOG/TABLE/"
const catalogTable = catalogTablePrefix + "%s/%s" // e.g. CATALOG/TABLE/db1/table1

const catalogColumnPrefix = "CATALOG/COLUMN/"
const catalogColumn = catalogColumnPrefix + "%s/%s/%s/%s" // e.g. "CATALOG/COLUMN/db1/table1/col1/INTEGER"

const catalogPKPrefix = "CATALOG/PK/"
const catalogPK = catalogPKPrefix + "%s/%s/%s" // e.g. CATALOG/PK/db1/table1/col1

const catalogIndexPrefix = "CATALOG/INDEX/"
const catalogIndex = catalogIndexPrefix + "%s/%s/%s" // e.g. CATALOG/INDEX/db1/table1/col1

type Catalog interface {
	UseSnapshot(stmt *UseSnapshotStmt) error
	Databases() ([]*Database, error)
	DatabaseExist(db string) (bool, error)
}

type Database struct {
	name    string
	tables  []*Table
	indexes []string
}

type Table struct {
	name string
	cols []*Column
	pk   *Column
}

type Column struct {
	colName string
	colType SQLValueType
}

type sqlCatalog struct {
	st     *store.ImmuStore
	prefix []byte

	databases []*Database

	rwmutex sync.RWMutex

	writeLocked bool
}

// think on how to manage the catalog as per different snapshots.
// given the snapshot may be affected as well with the snapshot, should use the old catalog based on the snapshot or always the latest catalog
// but with only affecting data?

func openCatalog(st *store.ImmuStore, prefix []byte) (Catalog, error) {
	if st == nil {
		return nil, ErrIllegalArguments
	}

	catalog := &sqlCatalog{
		st:          st,
		prefix:      prefix,
		databases:   nil,
		writeLocked: false,
	}

	return catalog, nil
}

func (c *sqlCatalog) UseSnapshot(stmt *UseSnapshotStmt) error {
	if stmt == nil {
		return ErrIllegalArguments
	}

	c.rwmutex.Lock()
	defer c.rwmutex.Unlock()

	// TODO: get a snapshot from the store, load databases
	snap, err := c.st.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}

	dbReaderSpec := &tbtree.ReaderSpec{
		Prefix: []byte(catalogDatabasePrefix),
	}
	dbReader, err := snap.NewReader(dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		c.databases = nil
		return nil
	}
	if err != nil {
		return err
	}

	dbs := make([]*Database, 0)

	for {
		mkey, _, _, _, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		db := &Database{
			name: c.unmapDatabase(mkey),
		}

		dbs = append(dbs, db)
	}

	c.databases = dbs

	return nil
}

func (c *sqlCatalog) unmap(mkey []byte, patternPrefix string) []string {
	return strings.Split(strings.Trim(string(mkey[len(c.prefix):]), patternPrefix), patternSeparator)
}

func (c *sqlCatalog) unmapDatabase(mkey []byte) string {
	args := c.unmap(mkey, catalogDatabasePrefix)
	return args[0]
}

func (c *sqlCatalog) Databases() ([]*Database, error) {
	c.rwmutex.RLock()
	defer c.rwmutex.RUnlock()

	return c.databases, nil
}

func (c *sqlCatalog) DatabaseExist(db string) (bool, error) {
	c.rwmutex.RLock()
	defer c.rwmutex.RUnlock()

	for _, database := range c.databases {
		if database.name == db {
			return true, nil
		}
	}

	return false, nil
}

// for writes, always needs to be up the date, doesn't matter the snapshot...
// for reading, a snapshot is created. It will wait until such tx is indexed.
// still writing to the catalog will wait the index to be up to date and locked
// conditional lock on writeLocked
func (c *sqlCatalog) CreateDatabase(db string) error {
	return errors.New("not yet supported")
}
