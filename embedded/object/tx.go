/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package object

import (
	"context"
	"errors"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

// Tx is a database transaction interface - it holds the primary methods used while using a transaction
type Tx interface {
	Catalog() *sql.Catalog
}

// ObjectTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW
type ObjectTx struct {
	engine *Engine

	opts *sql.TxOptions

	tx *store.OngoingTx

	currentDB *sql.Database
	catalog   *sql.Catalog // in-mem catalog

	updatedRows      int
	lastInsertedPKs  map[string]int64 // last inserted PK by table name
	firstInsertedPKs map[string]int64 // first inserted PK by table name

	txHeader *store.TxHeader // header is set once tx is committed

	committed bool
	closed    bool
}

func (otx *ObjectTx) Catalog() *sql.Catalog {
	return otx.catalog
}

func (otx *ObjectTx) IsExplicitCloseRequired() bool {
	return otx.opts.ExplicitClose
}

func (otx *ObjectTx) RequireExplicitClose() error {
	if otx.updatedRows != 0 {
		return store.ErrIllegalState
	}

	otx.opts.ExplicitClose = true

	return nil
}

func (otx *ObjectTx) useDatabase(dbName string) error {
	db, err := otx.catalog.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	otx.currentDB = db

	return nil
}

func (otx *ObjectTx) Database() *sql.Database {
	return otx.currentDB
}

func (otx *ObjectTx) Timestamp() time.Time {
	return otx.tx.Timestamp()
}

func (otx *ObjectTx) UpdatedRows() int {
	return otx.updatedRows
}

func (otx *ObjectTx) LastInsertedPKs() map[string]int64 {
	return otx.lastInsertedPKs
}

func (otx *ObjectTx) FirstInsertedPKs() map[string]int64 {
	return otx.firstInsertedPKs
}

func (otx *ObjectTx) TxHeader() *store.TxHeader {
	return otx.txHeader
}

func (otx *ObjectTx) sqlPrefix() []byte {
	return otx.engine.prefix
}

func (otx *ObjectTx) distinctLimit() int {
	return otx.engine.distinctLimit
}

func (otx *ObjectTx) newKeyReader(rSpec store.KeyReaderSpec) (store.KeyReader, error) {
	return otx.tx.NewKeyReader(rSpec)
}

func (otx *ObjectTx) get(key []byte) (store.ValueRef, error) {
	return otx.tx.Get(key)
}

func (otx *ObjectTx) set(key []byte, metadata *store.KVMetadata, value []byte) error {
	return otx.tx.Set(key, metadata, value)
}

func (otx *ObjectTx) existKeyWith(prefix, neq []byte) (bool, error) {
	_, _, err := otx.tx.GetWithPrefix(prefix, neq)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}

	return err == nil, err
}

func (otx *ObjectTx) Cancel() error {
	if otx.closed {
		return sql.ErrAlreadyClosed
	}

	otx.closed = true

	return otx.tx.Cancel()
}

func (otx *ObjectTx) Commit(ctx context.Context) error {
	otx.committed = true
	otx.closed = true

	// no need to wait for indexing to be up to date during commit phase
	hdr, err := otx.tx.AsyncCommit(ctx)
	if err != nil && err != store.ErrorNoEntriesProvided {
		return err
	}

	otx.txHeader = hdr

	return nil
}

func (otx *ObjectTx) Closed() bool {
	return otx.closed
}
