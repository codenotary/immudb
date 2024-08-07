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

package sql

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/store"
)

// SQLTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW
type SQLTx struct {
	engine *Engine

	opts *TxOptions

	tx        *store.OngoingTx
	tempFiles []*os.File

	catalog *Catalog // in-mem catalog

	mutatedCatalog bool // set when a DDL stmt was executed within the current tx

	updatedRows      int
	lastInsertedPKs  map[string]int64 // last inserted PK by table name
	firstInsertedPKs map[string]int64 // first inserted PK by table name

	txHeader *store.TxHeader // header is set once tx is committed

	onCommittedCallbacks []onCommittedCallback
}

type onCommittedCallback = func(sqlTx *SQLTx) error

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

func (sqlTx *SQLTx) get(ctx context.Context, key []byte) (store.ValueRef, error) {
	return sqlTx.tx.Get(ctx, key)
}

func (sqlTx *SQLTx) set(key []byte, metadata *store.KVMetadata, value []byte) error {
	return sqlTx.tx.Set(key, metadata, value)
}

func (sqlTx *SQLTx) setTransient(key []byte, metadata *store.KVMetadata, value []byte) error {
	return sqlTx.tx.SetTransient(key, metadata, value)
}

func (sqlTx *SQLTx) getWithPrefix(ctx context.Context, prefix, neq []byte) (key []byte, valRef store.ValueRef, err error) {
	return sqlTx.tx.GetWithPrefix(ctx, prefix, neq)
}

func (sqlTx *SQLTx) Cancel() error {
	defer sqlTx.removeTempFiles()

	return sqlTx.tx.Cancel()
}

func (sqlTx *SQLTx) Commit(ctx context.Context) error {
	defer sqlTx.removeTempFiles()

	err := sqlTx.tx.RequireMVCCOnFollowingTxs(sqlTx.mutatedCatalog)
	if err != nil {
		return err
	}

	// no need to wait for indexing to be up to date during commit phase
	sqlTx.txHeader, err = sqlTx.tx.AsyncCommit(ctx)
	if err != nil && !errors.Is(err, store.ErrNoEntriesProvided) {
		return err
	}

	merr := multierr.NewMultiErr()

	for _, onCommitCallback := range sqlTx.onCommittedCallbacks {
		err := onCommitCallback(sqlTx)
		merr.Append(err)
	}

	return merr.Reduce()
}

func (sqlTx *SQLTx) Closed() bool {
	return sqlTx.tx.Closed()
}

func (sqlTx *SQLTx) delete(ctx context.Context, key []byte) error {
	return sqlTx.tx.Delete(ctx, key)
}

func (sqlTx *SQLTx) addOnCommittedCallback(callback onCommittedCallback) error {
	if callback == nil {
		return ErrIllegalArguments
	}

	sqlTx.onCommittedCallbacks = append(sqlTx.onCommittedCallbacks, callback)

	return nil
}

func (sqlTx *SQLTx) createTempFile() (*os.File, error) {
	tempFile, err := os.CreateTemp("", "immudb")
	if err == nil {
		sqlTx.tempFiles = append(sqlTx.tempFiles, tempFile)
	}
	return tempFile, err
}

func (sqlTx *SQLTx) removeTempFiles() error {
	for _, file := range sqlTx.tempFiles {
		err := file.Close()
		if err != nil {
			return err
		}

		err = os.Remove(file.Name())
		if err != nil {
			return err
		}
	}
	return nil
}
