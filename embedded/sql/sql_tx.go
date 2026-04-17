/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"fmt"
	"os"
	"time"

	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/store"
)

// savepointState captures SQLTx state at a SAVEPOINT for later rollback.
type savepointState struct {
	updatedRows      int
	lastInsertedPKs  map[string]int64
	firstInsertedPKs map[string]int64
	mutatedCatalog   bool
}

// SQLTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW
type SQLTx struct {
	engine *Engine

	opts *TxOptions

	tx        *store.OngoingTx
	tempFiles []*os.File

	catalog *Catalog // in-mem catalog

	// openCatalogVersion is engine.cachedCatalogVersion at the time this tx
	// was opened. Compared on commit (D2 Phase 2) to determine whether this
	// tx's catalog can safely populate the engine cache: if the version has
	// changed, an invalidation happened concurrently and our view is stale.
	openCatalogVersion uint64

	mutatedCatalog bool // set when a DDL stmt was executed within the current tx

	updatedRows      int
	lastInsertedPKs  map[string]int64 // last inserted PK by table name
	firstInsertedPKs map[string]int64 // first inserted PK by table name

	txHeader *store.TxHeader // header is set once tx is committed

	onCommittedCallbacks []onCommittedCallback

	savepoints map[string]*savepointState
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

func (sqlTx *SQLTx) distinctSpillThreshold() int {
	return sqlTx.engine.distinctSpillThreshold
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

func (sqlTx *SQLTx) Savepoint(name string) {
	if sqlTx.savepoints == nil {
		sqlTx.savepoints = make(map[string]*savepointState)
	}

	// Copy current state
	lastPKs := make(map[string]int64)
	for k, v := range sqlTx.lastInsertedPKs {
		lastPKs[k] = v
	}
	firstPKs := make(map[string]int64)
	for k, v := range sqlTx.firstInsertedPKs {
		firstPKs[k] = v
	}

	sqlTx.savepoints[name] = &savepointState{
		updatedRows:      sqlTx.updatedRows,
		lastInsertedPKs:  lastPKs,
		firstInsertedPKs: firstPKs,
		mutatedCatalog:   sqlTx.mutatedCatalog,
	}
}

func (sqlTx *SQLTx) RollbackToSavepoint(name string) error {
	if sqlTx.savepoints == nil {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	sp, ok := sqlTx.savepoints[name]
	if !ok {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	// Restore state
	sqlTx.updatedRows = sp.updatedRows
	sqlTx.lastInsertedPKs = sp.lastInsertedPKs
	sqlTx.firstInsertedPKs = sp.firstInsertedPKs
	sqlTx.mutatedCatalog = sp.mutatedCatalog

	// Remove this savepoint and any created after it
	// (PostgreSQL behavior: ROLLBACK TO destroys savepoints created after the named one)
	delete(sqlTx.savepoints, name)

	return nil
}

func (sqlTx *SQLTx) ReleaseSavepoint(name string) error {
	if sqlTx.savepoints == nil {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	if _, ok := sqlTx.savepoints[name]; !ok {
		return fmt.Errorf("savepoint %s does not exist", name)
	}

	delete(sqlTx.savepoints, name)
	return nil
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

	// DDL committed: the cached catalog is now stale; clear it so the next
	// read-only transaction reloads the schema from the store.
	if sqlTx.mutatedCatalog {
		sqlTx.engine.invalidateCatalogCache()
	} else {
		// D2 Phase 2: opportunistically populate the engine catalog cache
		// from this RW tx's view. Subsequent RW txs then take the Clone
		// fast path (saves the per-NewTx catalog.load); option (a) from
		// the prior comment is now satisfied because NewTx's Clone branch
		// calls seedCatalogReadSet, restoring the MVCC read-set entries
		// that catalog.load would have produced.
		//
		// tryPopulateCatalogCache is no-op when the cache is already
		// warm or when an invalidation happened during this tx's
		// lifetime (cachedCatalogVersion mismatch).
		sqlTx.engine.tryPopulateCatalogCache(sqlTx.catalog, sqlTx.openCatalogVersion)
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

// createTempFile returns an os.CreateTemp("", "immudb") file and registers
// it with the SQLTx for observability + defensive cleanup. Ownership of
// the close/remove sits with the caller (e.g. fileSorter.Close), which
// must call deregisterTempFile to drop the registration before the
// surrounding tx Cancel/Commit closes it. This split avoids a race where
// the tx's deferred removeTempFiles would force-close a file the caller's
// row reader is still consuming — see embedded/sql/file_sort.go and the
// JOIN+GROUP+ORDER regression in joint_row_reader.go:198.
func (sqlTx *SQLTx) createTempFile() (*os.File, error) {
	tempFile, err := os.CreateTemp("", "immudb")
	if err == nil {
		sqlTx.tempFiles = append(sqlTx.tempFiles, tempFile)
	}
	return tempFile, err
}

// deregisterTempFile removes f from sqlTx.tempFiles so the deferred
// removeTempFiles in Cancel/Commit will not touch it. The caller is then
// responsible for closing and removing the file. Safe to call with a file
// that was never registered.
func (sqlTx *SQLTx) deregisterTempFile(f *os.File) {
	for i, tf := range sqlTx.tempFiles {
		if tf == f {
			sqlTx.tempFiles = append(sqlTx.tempFiles[:i], sqlTx.tempFiles[i+1:]...)
			return
		}
	}
}

// removeTempFiles closes and removes any temp files still registered with
// the tx. Run from Cancel/Commit as a defensive safety net for files the
// caller never explicitly closed.
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
	sqlTx.tempFiles = nil
	return nil
}

func (sqlTx *SQLTx) ListUsers(ctx context.Context) ([]User, error) {
	if sqlTx.engine.multidbHandler == nil {
		return nil, ErrUnspecifiedMultiDBHandler
	}
	return sqlTx.engine.multidbHandler.ListUsers(ctx)
}
