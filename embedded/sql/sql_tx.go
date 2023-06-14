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

package sql

import (
	"context"
	"errors"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

// SQLTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW
type SQLTx struct {
	engine *Engine

	opts *TxOptions

	tx *store.OngoingTx

	catalog *Catalog // in-mem catalog

	mutatedCatalog bool // set when a DDL stmt was executed within the current tx

	updatedRows      int
	lastInsertedPKs  map[string]int64 // last inserted PK by table name
	firstInsertedPKs map[string]int64 // first inserted PK by table name

	txHeader *store.TxHeader // header is set once tx is committed
}

func (sqlTx *SQLTx) Catalog() *Catalog {
	return sqlTx.catalog
}

func (sqlTx *SQLTx) IsExplicitCloseRequired() bool {
	return sqlTx.opts.ExplicitClose
}

func (sqlTx *SQLTx) RequireExplicitClose() error {
	if sqlTx.updatedRows != 0 {
		return store.ErrIllegalState
	}

	sqlTx.opts.ExplicitClose = true

	return nil
}

func (sqlTx *SQLTx) Timestamp() time.Time {
	return sqlTx.tx.Timestamp()
}

func (sqlTx *SQLTx) UpdatedRows() int {
	return sqlTx.updatedRows
}

func (sqlTx *SQLTx) LastInsertedPKs() map[string]int64 {
	return sqlTx.lastInsertedPKs
}

func (sqlTx *SQLTx) FirstInsertedPKs() map[string]int64 {
	return sqlTx.firstInsertedPKs
}

func (sqlTx *SQLTx) TxHeader() *store.TxHeader {
	return sqlTx.txHeader
}

func (sqlTx *SQLTx) sqlPrefix() []byte {
	return sqlTx.engine.prefix
}

func (sqlTx *SQLTx) distinctLimit() int {
	return sqlTx.engine.distinctLimit
}

func (sqlTx *SQLTx) newKeyReader(rSpec store.KeyReaderSpec) (store.KeyReader, error) {
	return sqlTx.tx.NewKeyReader(rSpec)
}

func (sqlTx *SQLTx) get(key []byte) (store.ValueRef, error) {
	return sqlTx.tx.Get(key)
}

func (sqlTx *SQLTx) set(key []byte, metadata *store.KVMetadata, value []byte) error {
	return sqlTx.tx.Set(key, metadata, value)
}

func (sqlTx *SQLTx) existKeyWith(prefix, neq []byte) (bool, error) {
	_, _, err := sqlTx.tx.GetWithPrefix(prefix, neq)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}

	return err == nil, err
}

func (sqlTx *SQLTx) Cancel() error {
	return sqlTx.tx.Cancel()
}

func (sqlTx *SQLTx) Commit(ctx context.Context) error {
	err := sqlTx.tx.RequireMVCCOnFollowingTxs(sqlTx.mutatedCatalog)
	if err != nil {
		return err
	}

	// no need to wait for indexing to be up to date during commit phase
	sqlTx.txHeader, err = sqlTx.tx.AsyncCommit(ctx)
	if err != nil && !errors.Is(err, store.ErrNoEntriesProvided) {
		return err
	}

	return nil
}

func (sqlTx *SQLTx) Closed() bool {
	return sqlTx.tx.Closed()
}

func (sqlTx *SQLTx) Committed() bool {
	return sqlTx.txHeader != nil
}

func (sqlTx *SQLTx) delete(key []byte) error {
	return sqlTx.tx.Delete(key)
}
