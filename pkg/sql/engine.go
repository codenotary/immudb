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
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseNoExists = errors.New("database no exists")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrInvalidPK = errors.New("invalid primary key")

type Engine struct {
	catalogStore *store.ImmuStore
	dataStore    *store.ImmuStore

	prefix []byte

	cmux sync.Mutex

	implicitDatabase string
}

func NewEngine(catalogStore, dataStore *store.ImmuStore, prefix []byte) (*Engine, error) {
	e := &Engine{
		catalogStore: catalogStore,
		dataStore:    dataStore,
		prefix:       make([]byte, len(prefix)),
	}

	copy(e.prefix, prefix)

	return e, nil
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

func (e *Engine) ExistDatabase(db string) (bool, error) {
	return existKey(e.mapKey(catalogDatabase, db), e.catalogStore)
}

//TODO: will return a list of rows
func (e *Engine) Query(sql io.ByteReader) error {
	return nil
}

func (e *Engine) ExecStmt(sql string) (*store.TxMetadata, error) {
	return e.Exec(strings.NewReader(sql))
}

func (e *Engine) WaitForIndexingUpto(txID uint64) error {
	if txID == 0 {
		return nil
	}

	for {
		its := e.catalogStore.IndexInfo()

		if its >= txID {
			return nil
		}

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

func (e *Engine) Exec(sql io.ByteReader) (*store.TxMetadata, error) {
	// TODO: only needs to lock insertions in catalog store
	e.cmux.Lock()
	defer e.cmux.Unlock()

	// actually if it's a query, should just wait until specified SNAPSHOT
	// wait for catalogStore indexing up to date
	lastTxID, _ := e.catalogStore.Alh()
	e.WaitForIndexingUpto(lastTxID)

	centries, dentries, err := e.ValidateAndCompile(sql)
	if err != nil {
		return nil, err
	}

	if len(centries) > 0 && len(dentries) > 0 {
		return nil, ErrDDLorDMLTxOnly
	}

	if len(centries) > 0 {
		return e.catalogStore.Commit(centries)
	}

	if len(dentries) > 0 {
		return e.dataStore.Commit(dentries)
	}

	return nil, nil
}

// Porque va a validar contra el catalogo antes de agregar
// Y luego es el commit del resultado si no hubo error

func (e *Engine) ValidateAndCompile(sql io.ByteReader) ([]*store.KV, []*store.KV, error) {
	stmts, err := Parse(sql)
	if err != nil {
		return nil, nil, err
	}

	centries := make([]*store.KV, 0)
	dentries := make([]*store.KV, 0)

	for _, stmt := range stmts {
		// TODO: semantic analysis

		ces, des, err := stmt.ValidateAndCompileUsing(e)
		if err != nil {
			return nil, nil, err
		}

		centries = append(centries, ces...)
		dentries = append(dentries, des...)
	}

	return centries, dentries, nil
}
