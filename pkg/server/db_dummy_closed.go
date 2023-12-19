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

package server

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"time"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
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

func (db *closedDB) AsReplica(asReplica, syncReplication bool, syncAcks int) {
}

func (db *closedDB) IsReplica() bool {
	return false
}

func (db *closedDB) IsSyncReplicationEnabled() bool {
	return false
}

func (db *closedDB) SetSyncReplication(enabled bool) {
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

func (db *closedDB) Set(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ExecAll(ctx context.Context, operations *schema.ExecAllRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) NewSQLTx(ctx context.Context, _ *sql.TxOptions) (*sql.SQLTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLExec(ctx context.Context, tx *sql.SQLTx, req *schema.SQLExecRequest) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLExecPrepared(ctx context.Context, tx *sql.SQLTx, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error) {
	return nil, nil, store.ErrAlreadyClosed
}

func (db *closedDB) InferParameters(ctx context.Context, tx *sql.SQLTx, sql string) (map[string]sql.SQLValueType, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) InferParametersPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.SQLStmt) (map[string]sql.SQLValueType, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQuery(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, namedParams []*schema.NamedParam) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) SQLQueryRowReader(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ListTables(ctx context.Context, tx *sql.SQLTx) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) DescribeTable(ctx context.Context, tx *sql.SQLTx, table string) (*schema.SQLQueryResult, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) TxByID(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) ExportTxByID(ctx context.Context, req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error) {
	return nil, 0, mayCommitUpToAlh, store.ErrAlreadyClosed
}

func (db *closedDB) ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*schema.TxHeader, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) AllowCommitUpto(txID uint64, alh [sha256.Size]byte) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) DiscardPrecommittedTxsSince(txID uint64) error {
	return store.ErrAlreadyClosed
}

func (db *closedDB) VerifiableTxByID(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	return nil, store.ErrAlreadyClosed
}

func (db *closedDB) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
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

func (db *closedDB) Truncate(ts time.Duration) error {
	return store.ErrAlreadyClosed
}

// CreateCollection creates a new collection
func (d *closedDB) CreateCollection(ctx context.Context, username string, req *protomodel.CreateCollectionRequest) (*protomodel.CreateCollectionResponse, error) {
	return nil, store.ErrAlreadyClosed
}

// GetCollection returns the collection schema
func (d *closedDB) GetCollection(ctx context.Context, req *protomodel.GetCollectionRequest) (*protomodel.GetCollectionResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) GetCollections(ctx context.Context, req *protomodel.GetCollectionsRequest) (*protomodel.GetCollectionsResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) UpdateCollection(ctx context.Context, username string, req *protomodel.UpdateCollectionRequest) (*protomodel.UpdateCollectionResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) DeleteCollection(ctx context.Context, username string, req *protomodel.DeleteCollectionRequest) (*protomodel.DeleteCollectionResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) AddField(ctx context.Context, username string, req *protomodel.AddFieldRequest) (*protomodel.AddFieldResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) RemoveField(ctx context.Context, username string, req *protomodel.RemoveFieldRequest) (*protomodel.RemoveFieldResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) CreateIndex(ctx context.Context, username string, req *protomodel.CreateIndexRequest) (*protomodel.CreateIndexResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) DeleteIndex(ctx context.Context, username string, req *protomodel.DeleteIndexRequest) (*protomodel.DeleteIndexResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) InsertDocuments(ctx context.Context, username string, req *protomodel.InsertDocumentsRequest) (*protomodel.InsertDocumentsResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) ReplaceDocuments(ctx context.Context, username string, req *protomodel.ReplaceDocumentsRequest) (*protomodel.ReplaceDocumentsResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) AuditDocument(ctx context.Context, req *protomodel.AuditDocumentRequest) (*protomodel.AuditDocumentResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) SearchDocuments(ctx context.Context, query *protomodel.Query, offset int64) (document.DocumentReader, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) CountDocuments(ctx context.Context, req *protomodel.CountDocumentsRequest) (*protomodel.CountDocumentsResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) ProofDocument(ctx context.Context, req *protomodel.ProofDocumentRequest) (*protomodel.ProofDocumentResponse, error) {
	return nil, store.ErrAlreadyClosed
}

func (d *closedDB) DeleteDocuments(ctx context.Context, username string, req *protomodel.DeleteDocumentsRequest) (*protomodel.DeleteDocumentsResponse, error) {
	return nil, store.ErrAlreadyClosed
}
