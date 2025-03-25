/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
package main

import (
	"context"
	"errors"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server"
)

type customDbList struct {
	dbList database.DatabaseList
}

func (l *customDbList) Put(db database.DB) {
	l.dbList.Put(db)
}

func (l *customDbList) Delete(dbName string) (database.DB, error) {
	return l.dbList.Delete(dbName)
}

func (l *customDbList) GetByIndex(index int) (database.DB, error) {
	return wrapDb(l.dbList.GetByIndex(index))
}

func (l *customDbList) GetByName(dbName string) (database.DB, error) {
	return wrapDb(l.dbList.GetByName(dbName))
}

func (l *customDbList) GetId(dbName string) int {
	return l.dbList.GetId(dbName)
}

func (l *customDbList) Length() int {
	return l.dbList.Length()
}

func wrapDb(db database.DB, err error) (database.DB, error) {
	if err != nil {
		return nil, err
	}

	return &dbWrapper{
		DB: db,
	}, nil
}

type dbWrapper struct {
	database.DB
}

func (db *dbWrapper) unsupported() error {
	return errors.New(
		`unsupported operation for this test, please do the test using the following steps:
  1. fetch transaction 2 with proof without prior state or with state at tx 2,
     ensure transaction 2 contains key "valid-key-0"
  2. get consistency proof between Tx 2 and Tx 3, store Tx 3 state
  3. get consistency proof between Tx 3 and Tx 5, store Tx 5 state
  4. fetch Tx 2 with consistency proof against Tx 5 again, server will respond with
     a transaction that will contain "fake-key" entry
valid client should reject this sequence of actions`,
	)
}

func (db *dbWrapper) CurrentState() (*schema.ImmutableState, error) {
	// This call may happen when the client does not yet have a valid state
	return stateQueryResult, nil
}

func (db *dbWrapper) Set(req *schema.SetRequest) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) Get(req *schema.KeyRequest) (*schema.Entry, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) GetAll(req *schema.KeyListRequest) (*schema.Entries, error) {
	return nil, db.unsupported()
}
func (db *dbWrapper) Delete(req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) SetReference(req *schema.ReferenceRequest) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) Scan(req *schema.ScanRequest) (*schema.Entries, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) History(req *schema.HistoryRequest) (*schema.Entries, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ExecAll(operations *schema.ExecAllRequest) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) CountAll() (*schema.EntryCount, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ZAdd(req *schema.ZAddRequest) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) NewSQLTx(ctx context.Context) (*sql.SQLTx, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) SQLExec(req *schema.SQLExecRequest, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, db.unsupported()
}

func (db *dbWrapper) SQLExecPrepared(stmts []sql.SQLStmt, params map[string]interface{}, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, db.unsupported()
}

func (db *dbWrapper) InferParameters(sql string, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) InferParametersPrepared(stmt sql.SQLStmt, tx *sql.SQLTx) (map[string]sql.SQLValueType, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) SQLQuery(req *schema.SQLQueryRequest, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) SQLQueryPrepared(stmt sql.DataSource, namedParams []*schema.NamedParam, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) SQLQueryRowReader(stmt sql.DataSource, params map[string]interface{}, tx *sql.SQLTx) (sql.RowReader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ListTables(tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) DescribeTable(table string, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) WaitForTx(txID uint64, cancellation <-chan struct{}) error {
	return db.unsupported()
}

func (db *dbWrapper) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	return db.unsupported()
}

func (db *dbWrapper) TxByID(req *schema.TxRequest) (*schema.Tx, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ExportTxByID(req *schema.ExportTxRequest) ([]byte, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) ReplicateTx(exportedTx []byte) (*schema.TxHeader, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	if req.Tx == 2 && req.ProveSinceTx == 2 {
		return verifiableTxById_2_2, nil
	}

	if req.Tx == 3 && req.ProveSinceTx == 2 {
		return verifiableTxById_3_2, nil
	}

	if req.Tx == 5 && req.ProveSinceTx == 3 {
		return verifiableTxById_5_3, nil
	}

	if req.Tx == 2 && req.ProveSinceTx == 5 {
		return verifiableTxById_5_2_fake, nil
	}

	return nil, db.unsupported()
}

func (db *dbWrapper) TxScan(req *schema.TxScanRequest) (*schema.TxList, error) {
	return nil, db.unsupported()
}

func (db *dbWrapper) FlushIndex(req *schema.FlushIndexRequest) error {
	return db.unsupported()
}

func (db *dbWrapper) CompactIndex() error {
	return db.unsupported()
}

func GetFakeServer(dir string, port int) (server.ImmuServerIf, error) {

	// init master server
	opts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(port).
		WithDir(dir)

	srv := server.
		DefaultServer().
		WithOptions(opts).
		WithDbList(&customDbList{
			dbList: database.NewDatabaseList(),
		})

	err := srv.Initialize()
	if err != nil {
		return nil, err
	}

	return srv, nil
}
