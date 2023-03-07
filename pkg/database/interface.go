package database

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	schemav2 "github.com/codenotary/immudb/pkg/api/schemav2"
)

type DB interface {
	GetName() string

	// Setttings
	GetOptions() *Options

	Path() string

	AsReplica(asReplica, syncReplication bool, syncAcks int)
	IsReplica() bool

	IsSyncReplicationEnabled() bool
	SetSyncReplication(enabled bool)

	MaxResultSize() int
	UseTimeFunc(timeFunc store.TimeFunc) error

	// State
	Health() (waitingCount int, lastReleaseAt time.Time)
	CurrentState() (*schema.ImmutableState, error)

	Size() (uint64, error)

	// Key-Value
	Set(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error)
	VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error)

	Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error)
	VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error)
	GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error)

	Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error)

	SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxHeader, error)
	VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error)

	Scan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error)

	History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error)

	ExecAll(ctx context.Context, operations *schema.ExecAllRequest) (*schema.TxHeader, error)

	Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error)
	CountAll(ctx context.Context) (*schema.EntryCount, error)

	ZAdd(ctx context.Context, req *schema.ZAddRequest) (*schema.TxHeader, error)
	VerifiableZAdd(ctx context.Context, req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error)
	ZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error)

	// SQL-related
	NewSQLTx(ctx context.Context, opts *sql.TxOptions) (*sql.SQLTx, error)

	SQLExec(ctx context.Context, tx *sql.SQLTx, req *schema.SQLExecRequest) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error)
	SQLExecPrepared(ctx context.Context, tx *sql.SQLTx, stmts []sql.SQLStmt, params map[string]interface{}) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error)

	InferParameters(ctx context.Context, tx *sql.SQLTx, sql string) (map[string]sql.SQLValueType, error)
	InferParametersPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.SQLStmt) (map[string]sql.SQLValueType, error)

	SQLQuery(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error)
	SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, namedParams []*schema.NamedParam) (*schema.SQLQueryResult, error)
	SQLQueryRowReader(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error)

	VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error)

	ListTables(ctx context.Context, tx *sql.SQLTx) (*schema.SQLQueryResult, error)
	DescribeTable(ctx context.Context, tx *sql.SQLTx, table string) (*schema.SQLQueryResult, error)

	// Transactional layer
	WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error
	WaitForIndexingUpto(ctx context.Context, txID uint64) error

	TxByID(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error)
	ExportTxByID(ctx context.Context, req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error)
	ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*schema.TxHeader, error)
	AllowCommitUpto(txID uint64, alh [sha256.Size]byte) error
	DiscardPrecommittedTxsSince(txID uint64) error

	VerifiableTxByID(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error)
	TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error)

	// Maintenance
	FlushIndex(req *schema.FlushIndexRequest) error
	CompactIndex() error

	IsClosed() bool
	Close() error

	ObjectDatabase
}

// ObjectDatabase is the interface for object database
type ObjectDatabase interface {
	// GetCollection returns the collection schema
	GetCollection(ctx context.Context, collection string) (interface{}, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) error

	// GetDocument returns the document
	GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error)
	// CreateDocument creates a new document
	CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (string, error)
}
