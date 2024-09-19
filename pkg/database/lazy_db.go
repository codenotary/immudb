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

package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrNoNewTransactions = errors.New("no new transactions")

type lazyDB struct {
	m *DBManager

	idx int
}

func (db *lazyDB) GetName() string {
	return db.m.GetNameByIndex(db.idx)
}

func (db *lazyDB) GetOptions() *Options {
	return db.m.GetOptionsByIndex(db.idx)
}

func (db *lazyDB) Path() string {
	opts := db.GetOptions()

	return filepath.Join(opts.GetDBRootPath(), db.GetName())
}

func (db *lazyDB) AsReplica(asReplica, syncReplication bool, syncAcks int) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		db.m.logger.Errorf("%s: AsReplica", err)
		return
	}
	defer db.m.Release(db.idx)

	d.AsReplica(asReplica, syncReplication, syncAcks)
}

func (db *lazyDB) IsReplica() bool {
	d, err := db.m.Get(db.idx)
	if err != nil {
		db.m.logger.Errorf("%s: IsReplica", err)
		return false
	}
	defer db.m.Release(db.idx)

	return d.IsReplica()
}

func (db *lazyDB) IsSyncReplicationEnabled() bool {
	d, err := db.m.Get(db.idx)
	if err != nil {
		db.m.logger.Errorf("%s: IsSyncReplicationEnabled", err)
		return false
	}
	defer db.m.Release(db.idx)

	return d.IsSyncReplicationEnabled()
}

func (db *lazyDB) SetSyncReplication(enabled bool) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		db.m.logger.Errorf("%s: SetSyncReplication", err)
		return
	}
	defer db.m.Release(db.idx)

	d.SetSyncReplication(enabled)
}

func (db *lazyDB) MaxResultSize() int {
	return db.GetOptions().maxResultSize
}

func (db *lazyDB) Health() (waitingCount int, lastReleaseAt time.Time) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		db.m.logger.Errorf("%s: Health", err)
		return
	}
	defer db.m.Release(db.idx)

	return d.Health()
}

func (db *lazyDB) CurrentState() (*schema.ImmutableState, error) {
	return db.m.GetState(db.idx)
}

func (db *lazyDB) Size() (uint64, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return 0, err
	}
	defer db.m.Release(db.idx)

	return d.Size()
}

func (db *lazyDB) TxCount() (uint64, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return 0, err
	}
	defer db.m.Release(db.idx)

	return d.TxCount()
}

func (db *lazyDB) Set(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.Set(ctx, req)
}

func (db *lazyDB) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableSet(ctx, req)
}

func (db *lazyDB) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.Get(ctx, req)
}

func (db *lazyDB) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableGet(ctx, req)
}

func (db *lazyDB) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.GetAll(ctx, req)
}

func (db *lazyDB) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.Delete(ctx, req)
}

func (db *lazyDB) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.SetReference(ctx, req)
}

func (db *lazyDB) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableSetReference(ctx, req)
}

func (db *lazyDB) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.Scan(ctx, req)
}

func (db *lazyDB) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.History(ctx, req)
}

func (db *lazyDB) ExecAll(ctx context.Context, operations *schema.ExecAllRequest) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ExecAll(ctx, operations)
}

func (db *lazyDB) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.Count(ctx, prefix)
}

func (db *lazyDB) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.CountAll(ctx)
}

func (db *lazyDB) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ZAdd(ctx, req)
}

func (db *lazyDB) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableZAdd(ctx, req)
}

func (db *lazyDB) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ZScan(ctx, req)
}

func (db *lazyDB) NewSQLTx(ctx context.Context, opts *sql.TxOptions) (*sql.SQLTx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.NewSQLTx(ctx, opts)
}

func (db *lazyDB) SQLExec(ctx context.Context, tx *sql.SQLTx, req *schema.SQLExecRequest) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, nil, err
	}
	defer db.m.Release(db.idx)

	return d.SQLExec(ctx, tx, req)
}

func (db *lazyDB) SQLExecPrepared(ctx context.Context, tx *sql.SQLTx, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, nil, err
	}
	defer db.m.Release(db.idx)

	return d.SQLExecPrepared(ctx, tx, stmts, params)
}

func (db *lazyDB) InferParameters(ctx context.Context, tx *sql.SQLTx, sql string) (map[string]sql.SQLValueType, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.InferParameters(ctx, tx, sql)
}

func (db *lazyDB) InferParametersPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.SQLStmt) (map[string]sql.SQLValueType, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.InferParametersPrepared(ctx, tx, stmt)
}

func (db *lazyDB) SQLQuery(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) (sql.RowReader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.SQLQuery(ctx, tx, req)
}

func (db *lazyDB) SQLQueryAll(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) ([]*sql.Row, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.SQLQueryAll(ctx, tx, req)
}

func (db *lazyDB) SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.SQLQueryPrepared(ctx, tx, stmt, params)
}

func (db *lazyDB) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableSQLGet(ctx, req)
}

func (db *lazyDB) ListTables(ctx context.Context, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ListTables(ctx, tx)
}

func (db *lazyDB) DescribeTable(ctx context.Context, tx *sql.SQLTx, table string) (*schema.SQLQueryResult, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.DescribeTable(ctx, tx, table)
}

func (db *lazyDB) WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.WaitForTx(ctx, txID, allowPrecommitted)
}

func (db *lazyDB) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.WaitForIndexingUpto(ctx, txID)
}

func (db *lazyDB) TxByID(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.TxByID(ctx, req)
}

func (db *lazyDB) ExportTxByID(ctx context.Context, req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error) {
	state, err := db.CurrentState()
	if err != nil {
		return nil, 0, [sha256.Size]byte{}, err
	}

	if !req.AllowPreCommitted {
		if req.Tx > state.TxId {
			return nil, 0, [sha256.Size]byte{}, ErrNoNewTransactions
		}
	}

	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, 0, [sha256.Size]byte{}, err
	}
	defer db.m.Release(db.idx)

	return d.ExportTxByID(ctx, req)
}

func (db *lazyDB) ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ReplicateTx(ctx, exportedTx, skipIntegrityCheck, waitForIndexing)
}

func (db *lazyDB) AllowCommitUpto(txID uint64, alh [sha256.Size]byte) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.AllowCommitUpto(txID, alh)
}

func (db *lazyDB) DiscardPrecommittedTxsSince(txID uint64) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.DiscardPrecommittedTxsSince(txID)
}

func (db *lazyDB) VerifiableTxByID(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.VerifiableTxByID(ctx, req)
}

func (db *lazyDB) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.TxScan(ctx, req)
}

func (db *lazyDB) FlushIndex(req *schema.FlushIndexRequest) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.FlushIndex(req)
}

func (db *lazyDB) CompactIndex() error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.CompactIndex()
}

func (db *lazyDB) IsClosed() bool {
	return db.m.IsClosed(db.idx)
}

func (db *lazyDB) Close() error {
	return db.m.Close(db.idx)
}

// CreateCollection creates a new collection
func (db *lazyDB) CreateCollection(ctx context.Context, username string, req *protomodel.CreateCollectionRequest) (*protomodel.CreateCollectionResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.CreateCollection(ctx, username, req)
}

// GetCollection returns the collection schema
func (db *lazyDB) GetCollection(ctx context.Context, req *protomodel.GetCollectionRequest) (*protomodel.GetCollectionResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.GetCollection(ctx, req)
}

func (db *lazyDB) GetCollections(ctx context.Context, req *protomodel.GetCollectionsRequest) (*protomodel.GetCollectionsResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.GetCollections(ctx, req)
}

func (db *lazyDB) UpdateCollection(ctx context.Context, username string, req *protomodel.UpdateCollectionRequest) (*protomodel.UpdateCollectionResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.UpdateCollection(ctx, username, req)
}

func (db *lazyDB) DeleteCollection(ctx context.Context, username string, req *protomodel.DeleteCollectionRequest) (*protomodel.DeleteCollectionResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.DeleteCollection(ctx, username, req)
}

func (db *lazyDB) AddField(ctx context.Context, username string, req *protomodel.AddFieldRequest) (*protomodel.AddFieldResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.AddField(ctx, username, req)
}

func (db *lazyDB) RemoveField(ctx context.Context, username string, req *protomodel.RemoveFieldRequest) (*protomodel.RemoveFieldResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.RemoveField(ctx, username, req)
}

func (db *lazyDB) CreateIndex(ctx context.Context, username string, req *protomodel.CreateIndexRequest) (*protomodel.CreateIndexResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.CreateIndex(ctx, username, req)
}

func (db *lazyDB) DeleteIndex(ctx context.Context, username string, req *protomodel.DeleteIndexRequest) (*protomodel.DeleteIndexResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.DeleteIndex(ctx, username, req)
}

func (db *lazyDB) InsertDocuments(ctx context.Context, username string, req *protomodel.InsertDocumentsRequest) (*protomodel.InsertDocumentsResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.InsertDocuments(ctx, username, req)
}

func (db *lazyDB) ReplaceDocuments(ctx context.Context, username string, req *protomodel.ReplaceDocumentsRequest) (*protomodel.ReplaceDocumentsResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ReplaceDocuments(ctx, username, req)
}

func (db *lazyDB) AuditDocument(ctx context.Context, req *protomodel.AuditDocumentRequest) (*protomodel.AuditDocumentResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.AuditDocument(ctx, req)
}

func (db *lazyDB) SearchDocuments(ctx context.Context, query *protomodel.Query, offset int64) (document.DocumentReader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.SearchDocuments(ctx, query, offset)
}

func (db *lazyDB) CountDocuments(ctx context.Context, req *protomodel.CountDocumentsRequest) (*protomodel.CountDocumentsResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.CountDocuments(ctx, req)
}

func (db *lazyDB) ProofDocument(ctx context.Context, req *protomodel.ProofDocumentRequest) (*protomodel.ProofDocumentResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.ProofDocument(ctx, req)
}

func (db *lazyDB) DeleteDocuments(ctx context.Context, username string, req *protomodel.DeleteDocumentsRequest) (*protomodel.DeleteDocumentsResponse, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.DeleteDocuments(ctx, username, req)
}

func (db *lazyDB) FindTruncationPoint(ctx context.Context, until time.Time) (*schema.TxHeader, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return nil, err
	}
	defer db.m.Release(db.idx)

	return d.FindTruncationPoint(ctx, until)
}

func (db *lazyDB) CopySQLCatalog(ctx context.Context, txID uint64) (uint64, error) {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return 0, err
	}
	defer db.m.Release(db.idx)

	return d.CopySQLCatalog(ctx, txID)
}

func (db *lazyDB) TruncateUptoTx(ctx context.Context, txID uint64) error {
	d, err := db.m.Get(db.idx)
	if err != nil {
		return err
	}
	defer db.m.Release(db.idx)

	return d.TruncateUptoTx(ctx, txID)
}
