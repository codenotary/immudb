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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var ErrIllegalArguments = store.ErrIllegalArguments
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrInvalidPK = errors.New("primary key of invalid type. Supported types are: INTEGER, STRING[256], TIMESTAMP OR BLOB[256]")
var ErrDuplicatedColumn = errors.New("duplicated column")
var ErrInvalidColumn = errors.New("invalid column")
var ErrPKCanNotBeNull = errors.New("primary key can not be null")
var ErrInvalidNumberOfValues = errors.New("invalid number of values provided")
var ErrInvalidValue = errors.New("invalid value provided")
var ErrExpectingDQLStmt = errors.New("illegal statement. DQL statement expected")
var ErrLimitedOrderBy = errors.New("order is limit to one indexed column")
var ErrIllegelMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrNoMoreEntries = store.ErrNoMoreEntries

const MaxPKStringValue = 256

const asPK = true

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

	c, err := catalogFrom(e, latestSnapshot)
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
		its, err := st.IndexInfo()
		if err != nil {
			return err
		}

		if its >= txID {
			return nil
		}

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

func catalogFrom(e *Engine, snap *tbtree.Snapshot) (*Catalog, error) {
	if snap == nil {
		return nil, ErrIllegalArguments
	}

	catalog := newCatalog()

	dbReaderSpec := &tbtree.ReaderSpec{
		Prefix: []byte(catalogDatabasePrefix),
	}

	dbReader, err := e.dataStore.NewKeyReader(snap, dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		return catalog, nil
	}
	if err != nil {
		return nil, err
	}

	for {
		mkey, vref, _, _, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		id, err := e.unmapDatabaseID(mkey)
		if err != nil {
			return nil, err
		}

		v, err := vref.Resolve()
		if err != nil {
			return nil, err
		}

		db, err := catalog.newDatabase(string(v))
		if err != nil {
			return nil, err
		}

		if id != db.id {
			return nil, ErrCorruptedData
		}
	}

	return catalog, nil
}

func (e *Engine) trimPrefix(mkey []byte, mappingPrefix []byte) ([]byte, error) {
	if len(e.prefix) > len(mkey) || !bytes.Equal(mkey[len(e.prefix):], mappingPrefix) {
		return nil, ErrIllegelMappedKey
	}

	return mkey[len(e.prefix)+len(mappingPrefix):], nil
}

func (e *Engine) unmapDatabaseID(mkey []byte) (uint64, error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogDatabasePrefix))
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(encID), nil
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

func (e *Engine) mapKey(mappingPrefix string, encValues ...[]byte) []byte {
	mkeyLen := len(e.prefix) + len(mappingPrefix)

	for _, ev := range encValues {
		mkeyLen += len(ev)
	}

	mkey := make([]byte, mkeyLen)

	off := 0

	copy(mkey[off:], e.prefix)
	off += len(e.prefix)

	copy(mkey[off:], []byte(mappingPrefix))
	off += len(mappingPrefix)

	for _, ev := range encValues {
		copy(mkey[off:], ev)
		off += len(ev)
	}

	return mkey
}

func encodeID(id uint64) []byte {
	var encID [8]byte
	binary.BigEndian.PutUint64(encID[:], id)
	return encID[:]
}

func encodeValue(val interface{}, colType SQLValueType, isPK bool) ([]byte, error) {
	switch colType {
	case StringType:
		{
			strVal, ok := val.(string)
			if !ok {
				return nil, ErrInvalidValue
			}

			if len(strVal) > MaxPKStringValue {
				return nil, ErrInvalidPK
			}

			encv := make([]byte, 4+len(strVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal)))
			copy(encv[4:], []byte(strVal))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := val.(uint64)
			if !ok {
				return nil, ErrInvalidValue
			}

			var encv [8]byte
			binary.BigEndian.PutUint64(encv[:], intVal)

			return encv[:], nil
		}
	}

	return nil, ErrInvalidValue
}

func (e *Engine) Catalog() *Catalog {
	return e.catalog
}

// exist database directly on catalogStore: // existKey(e.mapKey(catalogDatabase, db), e.catalogStore)
func (e *Engine) QueryStmt(sql string) (RowReader, error) {
	return e.Query(strings.NewReader(sql))
}

func (e *Engine) Query(sql io.ByteReader) (RowReader, error) {
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
	if len(stmts) > 1 {
		return nil, ErrExpectingDQLStmt
	}

	stmt, ok := stmts[0].(*SelectStmt)
	if !ok {
		return nil, ErrExpectingDQLStmt
	}

	snap, err := e.dataStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return nil, err
	}

	return stmt.Resolve(e, snap, nil)
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

	for _, stmt := range stmts {
		centries, dentries, err := stmt.CompileUsing(e)
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
