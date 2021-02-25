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
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrInvalidPK = errors.New("invalid primary key")
var ErrInvalidPKType = errors.New("primary key of invalid type. Only INTEGER type is supported")

type Engine struct {
	catalogStore *store.ImmuStore
	dataStore    *store.ImmuStore

	prefix []byte

	catalog *Catalog // in-mem current catalog (used for INSERT, DDL statements and SELECT statements without UseSnapshotStmt)

	catalogRWMux sync.RWMutex

	implicitDatabase string

	snapSinceTx, snapUptoTx uint64
}

func NewEngine(catalogStore, dataStore *store.ImmuStore, prefix []byte) (*Engine, error) {
	e := &Engine{
		catalogStore: catalogStore,
		dataStore:    dataStore,
		prefix:       make([]byte, len(prefix)),
	}

	copy(e.prefix, prefix)

	err := e.loadCatalog()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) loadCatalog() error {
	e.catalog = nil

	lastTxID, _ := e.catalogStore.Alh()
	waitForIndexingUpto(e.catalogStore, lastTxID)

	latestSnapshot, err := e.catalogStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}

	c, err := catalogFrom(latestSnapshot, e.prefix)
	if err != nil {
		return err
	}

	e.catalog = c
	return nil
}

func waitForIndexingUpto(st *store.ImmuStore, txID uint64) error {
	if txID == 0 {
		return nil
	}

	for {
		its := st.IndexInfo()

		if its >= txID {
			return nil
		}

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

func catalogFrom(snap *tbtree.Snapshot, prefix []byte) (*Catalog, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	catalog := &Catalog{
		databases: map[string]*Database{},
	}

	dbReaderSpec := &tbtree.ReaderSpec{
		Prefix: []byte(catalogDatabasePrefix),
	}

	dbReader, err := snap.NewReader(dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		return catalog, nil
	}
	if err != nil {
		return nil, err
	}

	for {
		mkey, _, _, _, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		db := &Database{
			name: unmapDatabase(mkey, prefix),
		}

		catalog.databases[db.name] = db
	}

	return catalog, nil
}

func unmap(mkey []byte, prefix []byte, patternPrefix string) []string {
	return strings.Split(strings.Trim(string(mkey[len(prefix):]), patternPrefix), patternSeparator)
}

func unmapDatabase(mkey []byte, prefix []byte) string {
	args := unmap(mkey, prefix, catalogDatabasePrefix)
	return args[0]
}

func existKey(key []byte, st *store.ImmuStore) (bool, error) {
	_, _, _, err := st.Get([]byte(key))
	if err == nil {
		return true, nil
	}
	if err != store.ErrKeyNotFound {
		return false, err
	}
	return false, nil
}

func (e *Engine) mapKey(pattern string, keys ...string) []byte {
	mk := fmt.Sprintf(pattern, keys)

	pk := make([]byte, len(e.prefix)+len(mk))
	copy(pk, e.prefix)
	copy(pk[len(e.prefix):], mk)

	return pk
}

func (e *Engine) Catalog() *Catalog {
	return e.catalog
}

// exist database directly on catalogStore: // existKey(e.mapKey(catalogDatabase, db), e.catalogStore)

//TODO: will return a list of rows
func (e *Engine) Query(sql io.ByteReader) error {
	if e.catalog == nil {
		err := e.loadCatalog()
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) ExecStmt(sql string) (*store.TxMetadata, error) {
	return e.Exec(strings.NewReader(sql))
}

func (e *Engine) Exec(sql io.ByteReader) (*store.TxMetadata, error) {
	if e.catalog == nil {
		err := e.loadCatalog()
		if err != nil {
			return nil, err
		}
	}

	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	if includesDDL(stmts) {
		e.catalogRWMux.Lock()
		defer e.catalogRWMux.Unlock()
	} else {
		e.catalogRWMux.RLock()
		defer e.catalogRWMux.RUnlock()
	}

	centries, dentries, err := e.ValidateAndCompile(stmts)
	if err != nil {
		return nil, err
	}

	if len(centries) > 0 && len(dentries) > 0 {
		return nil, ErrDDLorDMLTxOnly
	}

	if len(centries) > 0 {
		txmd, err := e.catalogStore.Commit(centries)
		if err != nil {
			return nil, e.loadCatalog()
		}

		return txmd, nil
	}

	if len(dentries) > 0 {
		return e.dataStore.Commit(dentries)
	}

	return nil, nil
}

func includesDDL(stmts []SQLStmt) bool {
	for _, stmt := range stmts {
		if stmt.isDDL() {
			return true
		}
	}
	return false
}

// Porque va a validar contra el catalogo antes de agregar
// Y luego es el commit del resultado si no hubo error

func (e *Engine) ValidateAndCompile(stmts []SQLStmt) (centries []*store.KV, dentries []*store.KV, err error) {
	for _, stmt := range stmts {
		// TODO: semantic analysis

		ces, des, err := stmt.ValidateAndCompileUsing(e)
		if err != nil {
			return nil, nil, err
		}

		centries = append(centries, ces...)
		dentries = append(dentries, des...)
	}

	return
}
