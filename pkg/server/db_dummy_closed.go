/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"context"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

// work-around until a DBManager is in-place, taking care of all db-related stuff
type closedDB struct {
	name string
	opts *database.Options
}

func (db *closedDB) GetName() string {
	return db.name
}

func (db *closedDB) GetOptions() *database.Options {
	return db.opts
}

func (db *closedDB) Path() string {
	return filepath.Join(db.opts.GetDBRootPath(), db.GetName())
}

func (db *closedDB) AsReplica(asReplica bool) {
}

func (db *closedDB) IsReplica() bool {
	return false
}

func (db *closedDB) MaxResultSize() int {
	return 1000
}

func (db *closedDB) UseTimeFunc(timeFunc store.TimeFunc) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) Health() (waitingCount int, lastReleaseAt time.Time) {
	return
}

func (db *closedDB) CurrentState() (*schema.ImmutableState, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Size() (uint64, error) {
	return 0, store.ErrAlreadyClosed
}

func (db *closedDB) Set(req *schema.SetRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Get(req *schema.KeyRequest) (*schema.Entry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) GetAll(req *schema.KeyListRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Delete(req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SetReference(req *schema.ReferenceRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Scan(req *schema.ScanRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) History(req *schema.HistoryRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ExecAll(operations *schema.ExecAllRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) CountAll() (*schema.EntryCount, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ZAdd(req *schema.ZAddRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) NewSQLTx(ctx context.Context) (*sql.SQLTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLExec(req *schema.SQLExecRequest, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLExecPrepared(stmts []sql.SQLStmt, params map[string]interface{}, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, store.ErrAlreadyClosed
}

func (db *closedDB) InferParameters(sql string, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) InferParametersPrepared(stmt sql.SQLStmt, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQuery(req *schema.SQLQueryRequest, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQueryPrepared(stmt sql.DataSource, namedParams []*schema.NamedParam, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQueryRowReader(stmt sql.DataSource, params map[string]interface{}, tx *sql.SQLTx) (sql.RowReader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ListTables(tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) DescribeTable(table string, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) WaitForTx(txID uint64, cancellation <-chan struct{}) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) TxByID(req *schema.TxRequest) (*schema.Tx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ExportTxByID(req *schema.ExportTxRequest) ([]byte, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ReplicateTx(exportedTx []byte) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) TxScan(req *schema.TxScanRequest) (*schema.TxList, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) FlushIndex(req *schema.FlushIndexRequest) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) CompactIndex() error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) IsClosed() bool {
	return true
}

func (db *closedDB) Close() error {
	return store.ErrAlreadyClosed
}
