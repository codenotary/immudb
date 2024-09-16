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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
)

const (
	MaxKeyResolutionLimit = 1
	MaxKeyScanLimit       = 2500
)

var (
	ErrKeyResolutionLimitReached  = errors.New("key resolution limit reached. It may be due to cyclic references")
	ErrResultSizeLimitExceeded    = errors.New("result size limit exceeded")
	ErrResultSizeLimitReached     = errors.New("result size limit reached")
	ErrIllegalArguments           = store.ErrIllegalArguments
	ErrIllegalState               = store.ErrIllegalState
	ErrIsReplica                  = errors.New("database is read-only because it's a replica")
	ErrNotReplica                 = errors.New("database is NOT a replica")
	ErrReplicaDivergedFromPrimary = errors.New("replica diverged from primary")
	ErrInvalidRevision            = errors.New("invalid key revision number")
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

	// State
	Health() (waitingCount int, lastReleaseAt time.Time)
	CurrentState() (*schema.ImmutableState, error)

	Size() (uint64, error)

	TxCount() (uint64, error)

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

	SQLQuery(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) (sql.RowReader, error)
	SQLQueryAll(ctx context.Context, tx *sql.SQLTx, req *schema.SQLQueryRequest) ([]*sql.Row, error)
	SQLQueryPrepared(ctx context.Context, tx *sql.SQLTx, stmt sql.DataSource, params map[string]interface{}) (sql.RowReader, error)

	VerifiableSQLGet(ctx context.Context, req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error)

	CopySQLCatalog(ctx context.Context, txID uint64) (uint64, error)

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

	// Truncation
	FindTruncationPoint(ctx context.Context, until time.Time) (*schema.TxHeader, error)
	TruncateUptoTx(ctx context.Context, txID uint64) error

	// Maintenance
	FlushIndex(req *schema.FlushIndexRequest) error
	CompactIndex() error

	IsClosed() bool
	Close() error

	DocumentDatabase
}

type replicaState struct {
	precommittedTxID uint64
	precommittedAlh  [sha256.Size]byte
}

// IDB database instance
type db struct {
	st *store.ImmuStore

	sqlEngine      *sql.Engine
	documentEngine *document.Engine

	mutex        *instrumentedRWMutex
	closingMutex sync.Mutex

	Logger  logger.Logger
	options *Options

	name string

	maxResultSize int

	txPool store.TxPool

	replicaStates      map[string]*replicaState
	replicaStatesMutex sync.Mutex
}

// OpenDB Opens an existing Database from disk
func OpenDB(dbName string, multidbHandler sql.MultiDBHandler, opts *Options, log logger.Logger) (DB, error) {
	if dbName == "" {
		return nil, fmt.Errorf("%w: invalid database name provided '%s'", ErrIllegalArguments, dbName)
	}

	log.Infof("opening database '%s' {replica = %v}...", dbName, opts.replica)

	var replicaStates map[string]*replicaState
	// replica states are only managed in primary with synchronous replication
	if !opts.replica && opts.syncAcks > 0 {
		replicaStates = make(map[string]*replicaState, opts.syncAcks)
	}

	dbi := &db{
		Logger:        log,
		options:       opts,
		name:          dbName,
		replicaStates: replicaStates,
		maxResultSize: opts.maxResultSize,
		mutex:         &instrumentedRWMutex{},
	}

	dbDir := dbi.Path()
	_, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("missing database directories: %s", dbDir)
	}

	stOpts := opts.GetStoreOptions().
		WithLogger(log).
		WithMultiIndexing(true).
		WithExternalCommitAllowance(opts.syncReplication)

	dbi.st, err = store.Open(dbDir, stOpts)
	if err != nil {
		return nil, logErr(dbi.Logger, "unable to open database: %s", err)
	}

	for _, prefix := range []byte{SetKeyPrefix, SortedSetKeyPrefix} {
		err := dbi.st.InitIndexing(&store.IndexSpec{
			SourcePrefix: []byte{prefix},
			TargetPrefix: []byte{prefix},
		})
		if err != nil {
			dbi.st.Close()
			return nil, logErr(dbi.Logger, "unable to open database: %s", err)
		}
	}

	dbi.Logger.Infof("loading sql-engine for database '%s' {replica = %v}...", dbName, opts.replica)

	sqlOpts := sql.DefaultOptions().
		WithPrefix([]byte{SQLPrefix}).
		WithMultiDBHandler(multidbHandler).
		WithParseTxMetadataFunc(parseTxMetadata)

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, sqlOpts)
	if err != nil {
		dbi.Logger.Errorf("unable to load sql-engine for database '%s' {replica = %v}. %v", dbName, opts.replica, err)
		return nil, err
	}
	dbi.Logger.Infof("sql-engine ready for database '%s' {replica = %v}", dbName, opts.replica)

	dbi.documentEngine, err = document.NewEngine(dbi.st, document.DefaultOptions().WithPrefix([]byte{DocumentPrefix}))
	if err != nil {
		return nil, err
	}
	dbi.Logger.Infof("document-engine ready for database '%s' {replica = %v}", dbName, opts.replica)

	txPool, err := dbi.st.NewTxHolderPool(opts.readTxPoolSize, false)
	if err != nil {
		return nil, logErr(dbi.Logger, "unable to create tx pool: %s", err)
	}
	dbi.txPool = txPool

	if opts.replica {
		dbi.Logger.Infof("database '%s' {replica = %v} successfully opened", dbName, opts.replica)
		return dbi, nil
	}

	dbi.Logger.Infof("database '%s' {replica = %v} successfully opened", dbName, opts.replica)

	return dbi, nil
}

func parseTxMetadata(data []byte) (map[string]interface{}, error) {
	md := schema.Metadata{}
	if err := md.Unmarshal(data); err != nil {
		return nil, err
	}

	meta := make(map[string]interface{}, len(md))
	for k, v := range md {
		meta[k] = v
	}
	return meta, nil
}

func (d *db) Path() string {
	return filepath.Join(d.options.GetDBRootPath(), d.GetName())
}

func (d *db) allocTx() (*store.Tx, error) {
	tx, err := d.txPool.Alloc()
	if errors.Is(err, store.ErrTxPoolExhausted) {
		return nil, ErrTxReadPoolExhausted
	}
	return tx, err
}

func (d *db) releaseTx(tx *store.Tx) {
	d.txPool.Release(tx)
}

// NewDB Creates a new Database along with it's directories and files
func NewDB(dbName string, multidbHandler sql.MultiDBHandler, opts *Options, log logger.Logger) (DB, error) {
	if dbName == "" {
		return nil, fmt.Errorf("%w: invalid database name provided '%s'", ErrIllegalArguments, dbName)
	}

	log.Infof("creating database '%s' {replica = %v}...", dbName, opts.replica)

	var replicaStates map[string]*replicaState
	// replica states are only managed in primary with synchronous replication
	if !opts.replica && opts.syncAcks > 0 {
		replicaStates = make(map[string]*replicaState, opts.syncAcks)
	}

	dbi := &db{
		Logger:        log,
		options:       opts,
		name:          dbName,
		replicaStates: replicaStates,
		maxResultSize: opts.maxResultSize,
		mutex:         &instrumentedRWMutex{},
	}

	dbDir := filepath.Join(opts.GetDBRootPath(), dbName)

	_, err := os.Stat(dbDir)
	if err == nil {
		return nil, fmt.Errorf("database directories already exist: %s", dbDir)
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(dbi.Logger, "unable to create data folder: %s", err)
	}

	stOpts := opts.GetStoreOptions().
		WithExternalCommitAllowance(opts.syncReplication).
		WithMultiIndexing(true).
		WithLogger(log)

	dbi.st, err = store.Open(dbDir, stOpts)
	if err != nil {
		return nil, logErr(dbi.Logger, "unable to open database: %s", err)
	}

	for _, prefix := range []byte{SetKeyPrefix, SortedSetKeyPrefix} {
		err := dbi.st.InitIndexing(&store.IndexSpec{
			SourcePrefix: []byte{prefix},
			TargetPrefix: []byte{prefix},
		})
		if err != nil {
			dbi.st.Close()
			return nil, logErr(dbi.Logger, "unable to open database: %s", err)
		}
	}

	txPool, err := dbi.st.NewTxHolderPool(opts.readTxPoolSize, false)
	if err != nil {
		return nil, logErr(dbi.Logger, "unable to create tx pool: %s", err)
	}
	dbi.txPool = txPool

	sqlOpts := sql.DefaultOptions().
		WithPrefix([]byte{SQLPrefix}).
		WithMultiDBHandler(multidbHandler).
		WithParseTxMetadataFunc(parseTxMetadata)

	dbi.Logger.Infof("loading sql-engine for database '%s' {replica = %v}...", dbName, opts.replica)

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, sqlOpts)
	if err != nil {
		dbi.Logger.Errorf("unable to load sql-engine for database '%s' {replica = %v}. %v", dbName, opts.replica, err)
		return nil, err
	}
	dbi.Logger.Infof("sql-engine ready for database '%s' {replica = %v}", dbName, opts.replica)

	dbi.documentEngine, err = document.NewEngine(dbi.st, document.DefaultOptions().WithPrefix([]byte{DocumentPrefix}))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}
	dbi.Logger.Infof("document-engine ready for database '%s' {replica = %v}", dbName, opts.replica)

	dbi.Logger.Infof("database '%s' successfully created {replica = %v}", dbName, opts.replica)

	return dbi, nil
}

func (d *db) MaxResultSize() int {
	return d.maxResultSize
}

func (d *db) FlushIndex(req *schema.FlushIndexRequest) error {
	if req == nil {
		return store.ErrIllegalArguments
	}
	return d.st.FlushIndexes(req.CleanupPercentage, req.Synced)
}

// CompactIndex ...
func (d *db) CompactIndex() error {
	return d.st.CompactIndexes()
}

// Set ...
func (d *db) Set(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	return d.set(ctx, req)
}

func (d *db) set(ctx context.Context, req *schema.SetRequest) (*schema.TxHeader, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	tx, err := d.newWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	keys := make(map[[sha256.Size]byte]struct{}, len(req.KVs))

	for _, kv := range req.KVs {
		if len(kv.Key) == 0 {
			return nil, ErrIllegalArguments
		}

		kid := sha256.Sum256(kv.Key)
		_, ok := keys[kid]
		if ok {
			return nil, schema.ErrDuplicatedKeysNotSupported
		}
		keys[kid] = struct{}{}

		e := EncodeEntrySpec(
			kv.Key,
			schema.KVMetadataFromProto(kv.Metadata),
			kv.Value,
		)

		err = tx.Set(e.Key, e.Metadata, e.Value)
		if err != nil {
			return nil, err
		}
	}

	for i := range req.Preconditions {
		c, err := PreconditionFromProto(req.Preconditions[i])
		if err != nil {
			return nil, err
		}

		err = tx.AddPrecondition(c)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", store.ErrInvalidPrecondition, err)
		}
	}

	var hdr *store.TxHeader

	if req.NoWait {
		hdr, err = tx.AsyncCommit(ctx)
	} else {
		hdr, err = tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

func (d *db) newWriteOnlyTx(ctx context.Context) (*store.OngoingTx, error) {
	tx, err := d.st.NewWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}
	return d.txWithMetadata(ctx, tx)
}

func (d *db) newTx(ctx context.Context, opts *store.TxOptions) (*store.OngoingTx, error) {
	tx, err := d.st.NewTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return d.txWithMetadata(ctx, tx)
}

func (d *db) txWithMetadata(ctx context.Context, tx *store.OngoingTx) (*store.OngoingTx, error) {
	meta := schema.MetadataFromContext(ctx)
	if len(meta) > 0 {
		txmd := store.NewTxMetadata()

		data, err := meta.Marshal()
		if err != nil {
			return nil, err
		}

		if err := txmd.WithExtra(data); err != nil {
			return nil, err
		}
		return tx.WithMetadata(txmd), nil
	}
	return tx, nil
}

func checkKeyRequest(req *schema.KeyRequest) error {
	if req == nil {
		return fmt.Errorf(
			"%w: empty request",
			ErrIllegalArguments,
		)
	}

	if len(req.Key) == 0 {
		return fmt.Errorf(
			"%w: empty key",
			ErrIllegalArguments,
		)
	}

	if req.AtTx > 0 {
		if req.SinceTx > 0 {
			return fmt.Errorf(
				"%w: SinceTx should not be specified when AtTx is used",
				ErrIllegalArguments,
			)
		}

		if req.AtRevision != 0 {
			return fmt.Errorf(
				"%w: AtRevision should not be specified when AtTx is used",
				ErrIllegalArguments,
			)
		}
	}

	return nil
}

// Get ...
func (d *db) Get(ctx context.Context, req *schema.KeyRequest) (*schema.Entry, error) {
	err := checkKeyRequest(req)
	if err != nil {
		return nil, err
	}

	currTxID, _ := d.st.CommittedAlh()
	if req.SinceTx > currTxID {
		return nil, fmt.Errorf(
			"%w: SinceTx must not be greater than the current transaction ID",
			ErrIllegalArguments,
		)
	}

	if !req.NoWait && req.AtTx == 0 {
		waitUntilTx := req.SinceTx
		if waitUntilTx == 0 {
			waitUntilTx = currTxID
		}

		err := d.WaitForIndexingUpto(ctx, waitUntilTx)
		if err != nil {
			return nil, err
		}
	}

	if req.AtRevision != 0 {
		return d.getAtRevision(ctx, EncodeKey(req.Key), req.AtRevision, true)
	}

	return d.getAtTx(ctx, EncodeKey(req.Key), req.AtTx, 0, d.st, 0, true)
}

func (d *db) get(ctx context.Context, key []byte, index store.KeyIndex, skipIntegrityCheck bool) (*schema.Entry, error) {
	return d.getAtTx(ctx, key, 0, 0, index, 0, skipIntegrityCheck)
}

func (d *db) getAtTx(
	ctx context.Context,
	key []byte,
	atTx uint64,
	resolved int,
	index store.KeyIndex,
	revision uint64,
	skipIntegrityCheck bool,
) (entry *schema.Entry, err error) {

	var txID uint64
	var val []byte
	var md *store.KVMetadata

	if atTx == 0 {
		valRef, err := index.Get(ctx, key)
		if err != nil {
			return nil, err
		}

		txID = valRef.Tx()
		revision = valRef.HC()
		md = valRef.KVMetadata()

		val, err = valRef.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		txID = atTx

		md, val, err = d.readMetadataAndValue(key, atTx, skipIntegrityCheck)
		if err != nil {
			return nil, err
		}
	}

	return d.resolveValue(ctx, key, val, resolved, txID, md, index, revision, skipIntegrityCheck)
}

func (d *db) readMetadataAndValue(key []byte, atTx uint64, skipIntegrityCheck bool) (*store.KVMetadata, []byte, error) {
	entry, _, err := d.st.ReadTxEntry(atTx, key, skipIntegrityCheck)
	if err != nil {
		return nil, nil, err
	}

	v, err := d.st.ReadValue(entry)
	if err != nil {
		return nil, nil, err
	}

	return entry.Metadata(), v, nil
}

func (d *db) getAtRevision(ctx context.Context, key []byte, atRevision int64, skipIntegrityCheck bool) (entry *schema.Entry, err error) {
	var offset uint64
	var desc bool

	if atRevision > 0 {
		offset = uint64(atRevision) - 1
		desc = false
	} else {
		offset = -uint64(atRevision)
		desc = true
	}

	valRefs, hCount, err := d.st.History(key, offset, desc, 1)
	if errors.Is(err, store.ErrNoMoreEntries) || errors.Is(err, store.ErrOffsetOutOfRange) {
		return nil, ErrInvalidRevision
	}
	if err != nil {
		return nil, err
	}

	if atRevision < 0 {
		atRevision = int64(hCount) + atRevision
	}

	entry, err = d.getAtTx(ctx, key, valRefs[0].Tx(), 0, d.st, uint64(atRevision), skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	return entry, err
}

func (d *db) resolveValue(
	ctx context.Context,
	key []byte,
	val []byte,
	resolved int,
	txID uint64,
	md *store.KVMetadata,
	index store.KeyIndex,
	revision uint64,
	skipIntegrityCheck bool,
) (entry *schema.Entry, err error) {
	if md != nil && md.Deleted() {
		return nil, store.ErrKeyNotFound
	}

	if len(val) < 1 {
		return nil, fmt.Errorf("%w: internal value consistency error - missing value prefix", store.ErrCorruptedData)
	}

	// Reference lookup
	if val[0] == ReferenceValuePrefix {
		if len(val) < 1+8 {
			return nil, fmt.Errorf("%w: internal value consistency error - invalid reference", store.ErrCorruptedData)
		}

		if resolved == MaxKeyResolutionLimit {
			return nil, ErrKeyResolutionLimitReached
		}

		atTx := binary.BigEndian.Uint64(TrimPrefix(val))
		refKey := make([]byte, len(val)-1-8)
		copy(refKey, val[1+8:])

		if index != nil {
			entry, err = d.getAtTx(ctx, refKey, atTx, resolved+1, index, 0, skipIntegrityCheck)
			if err != nil {
				return nil, err
			}
		} else {
			entry = &schema.Entry{
				Key: TrimPrefix(refKey),
				Tx:  atTx,
			}
		}

		entry.ReferencedBy = &schema.Reference{
			Tx:       txID,
			Key:      TrimPrefix(key),
			Metadata: schema.KVMetadataToProto(md),
			AtTx:     atTx,
			Revision: revision,
		}

		return entry, nil
	}

	return &schema.Entry{
		Tx:       txID,
		Key:      TrimPrefix(key),
		Metadata: schema.KVMetadataToProto(md),
		Value:    TrimPrefix(val),
		Revision: revision,
	}, nil
}

func (d *db) Health() (waitingCount int, lastReleaseAt time.Time) {
	return d.mutex.State()
}

// CurrentState ...
func (d *db) CurrentState() (*schema.ImmutableState, error) {
	lastTxID, lastTxAlh := d.st.CommittedAlh()
	lastPreTxID, lastPreTxAlh := d.st.PrecommittedAlh()

	return &schema.ImmutableState{
		TxId:               lastTxID,
		TxHash:             lastTxAlh[:],
		PrecommittedTxId:   lastPreTxID,
		PrecommittedTxHash: lastPreTxAlh[:],
	}, nil
}

// WaitForTx blocks caller until specified tx
func (d *db) WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error {
	return d.st.WaitForTx(ctx, txID, allowPrecommitted)
}

// WaitForIndexingUpto blocks caller until specified tx gets indexed
func (d *db) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	return d.st.WaitForIndexingUpto(ctx, txID)
}

// VerifiableSet ...
func (d *db) VerifiableSet(ctx context.Context, req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	// Preallocate tx buffers
	lastTx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(lastTx)

	txhdr, err := d.Set(ctx, req.SetRequest)
	if err != nil {
		return nil, err
	}

	err = d.st.ReadTx(uint64(txhdr.Id), false, lastTx)
	if err != nil {
		return nil, err
	}

	var prevTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		prevTxHdr = lastTx.Header()
	} else {
		prevTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false, false)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTxHdr, lastTx.Header())
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxToProto(lastTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}

// VerifiableGet ...
func (d *db) VerifiableGet(ctx context.Context, req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	e, err := d.Get(ctx, req.KeyRequest)
	if err != nil {
		return nil, err
	}

	var vTxID uint64
	var vKey []byte

	if e.ReferencedBy == nil {
		vTxID = e.Tx
		vKey = e.Key
	} else {
		vTxID = e.ReferencedBy.Tx
		vKey = e.ReferencedBy.Key
	}

	// key-value inclusion proof
	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	err = d.st.ReadTx(vTxID, false, tx)
	if err != nil {
		return nil, err
	}

	var rootTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		rootTxHdr = tx.Header()
	} else {
		rootTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false, false)
		if err != nil {
			return nil, err
		}
	}

	inclusionProof, err := tx.Proof(EncodeKey(vKey))
	if err != nil {
		return nil, err
	}

	var sourceTxHdr, targetTxHdr *store.TxHeader

	if req.ProveSinceTx <= vTxID {
		sourceTxHdr = rootTxHdr
		targetTxHdr = tx.Header()
	} else {
		sourceTxHdr = tx.Header()
		targetTxHdr = rootTxHdr
	}

	dualProof, err := d.st.DualProof(sourceTxHdr, targetTxHdr)
	if err != nil {
		return nil, err
	}

	verifiableTx := &schema.VerifiableTx{
		Tx:        schema.TxToProto(tx),
		DualProof: schema.DualProofToProto(dualProof),
	}

	return &schema.VerifiableEntry{
		Entry:          e,
		VerifiableTx:   verifiableTx,
		InclusionProof: schema.InclusionProofToProto(inclusionProof),
	}, nil
}

func (d *db) Delete(ctx context.Context, req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	opts := store.DefaultTxOptions()

	if req.SinceTx > 0 {
		if req.SinceTx > d.st.LastPrecommittedTxID() {
			return nil, store.ErrTxNotFound
		}

		opts.WithSnapshotMustIncludeTxID(func(_ uint64) uint64 {
			return req.SinceTx
		})
	}

	tx, err := d.newTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	for _, k := range req.Keys {
		if len(k) == 0 {
			return nil, ErrIllegalArguments
		}

		md := store.NewKVMetadata()

		md.AsDeleted(true)

		e := EncodeEntrySpec(k, md, nil)

		err = tx.Delete(ctx, e.Key)
		if err != nil {
			return nil, err
		}
	}

	var hdr *store.TxHeader
	if req.NoWait {
		hdr, err = tx.AsyncCommit(ctx)
	} else {
		hdr, err = tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

// GetAll ...
func (d *db) GetAll(ctx context.Context, req *schema.KeyListRequest) (*schema.Entries, error) {
	snap, err := d.snapshotSince(ctx, []byte{SetKeyPrefix}, req.SinceTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	list := &schema.Entries{}

	for _, key := range req.Keys {
		e, err := d.get(ctx, EncodeKey(key), snap, true)
		if err == nil || errors.Is(err, store.ErrKeyNotFound) {
			if e != nil {
				list.Entries = append(list.Entries, e)
			}
		} else {
			return nil, err
		}
	}

	return list, nil
}

func (d *db) Size() (uint64, error) {
	return d.st.Size()
}

// TxCount ...
func (d *db) TxCount() (uint64, error) {
	return d.st.TxCount(), nil
}

// Count ...
func (d *db) Count(ctx context.Context, prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	if prefix == nil {
		return nil, ErrIllegalArguments
	}

	tx, err := d.st.NewTx(ctx, store.DefaultTxOptions().WithMode(store.ReadOnlyTx))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	keyReader, err := tx.NewKeyReader(store.KeyReaderSpec{
		Prefix: WrapWithPrefix(prefix.Prefix, SetKeyPrefix),
	})
	if err != nil {
		return nil, err
	}
	defer keyReader.Close()

	count := 0

	for {
		_, _, err := keyReader.Read(ctx)
		if errors.Is(err, store.ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return nil, err
		}

		count++
	}

	return &schema.EntryCount{Count: uint64(count)}, nil
}

// CountAll ...
func (d *db) CountAll(ctx context.Context) (*schema.EntryCount, error) {
	return d.Count(ctx, &schema.KeyPrefix{})
}

// TxByID ...
func (d *db) TxByID(ctx context.Context, req *schema.TxRequest) (*schema.Tx, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	var snap *store.Snapshot
	var err error

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	if !req.KeepReferencesUnresolved {
		snap, err = d.snapshotSince(ctx, []byte{SetKeyPrefix}, req.SinceTx)
		if err != nil {
			return nil, err
		}
		defer snap.Close()
	}

	// key-value inclusion proof
	err = d.st.ReadTx(req.Tx, false, tx)
	if err != nil {
		return nil, err
	}

	return d.serializeTx(ctx, tx, req.EntriesSpec, snap, true)
}

func (d *db) snapshotSince(ctx context.Context, prefix []byte, txID uint64) (*store.Snapshot, error) {
	currTxID, _ := d.st.CommittedAlh()

	if txID > currTxID {
		return nil, ErrIllegalArguments
	}

	waitUntilTx := txID
	if waitUntilTx == 0 {
		waitUntilTx = currTxID
	}

	return d.st.SnapshotMustIncludeTxID(ctx, prefix, waitUntilTx)
}

func (d *db) serializeTx(ctx context.Context, tx *store.Tx, spec *schema.EntriesSpec, snap *store.Snapshot, skipIntegrityCheck bool) (*schema.Tx, error) {
	if spec == nil {
		return schema.TxToProto(tx), nil
	}

	stx := &schema.Tx{
		Header: schema.TxHeaderToProto(tx.Header()),
	}

	for _, e := range tx.Entries() {
		switch e.Key()[0] {
		case SetKeyPrefix:
			{
				if spec.KvEntriesSpec == nil || spec.KvEntriesSpec.Action == schema.EntryTypeAction_EXCLUDE {
					break
				}

				if spec.KvEntriesSpec.Action == schema.EntryTypeAction_ONLY_DIGEST {
					stx.Entries = append(stx.Entries, schema.TxEntryToProto(e))
					break
				}

				v, err := d.st.ReadValue(e)
				if errors.Is(err, store.ErrExpiredEntry) {
					break
				}
				if err != nil {
					return nil, err
				}

				if spec.KvEntriesSpec.Action == schema.EntryTypeAction_RAW_VALUE {
					kve := schema.TxEntryToProto(e)
					kve.Value = v
					stx.Entries = append(stx.Entries, kve)
					break
				}

				// resolve entry
				var index store.KeyIndex
				if snap != nil {
					index = snap
				}

				kve, err := d.resolveValue(ctx, e.Key(), v, 0, tx.Header().ID, e.Metadata(), index, 0, skipIntegrityCheck)
				if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrExpiredEntry) {
					break // ignore deleted ones (referenced key may have been deleted)
				}
				if err != nil {
					return nil, err
				}

				stx.KvEntries = append(stx.KvEntries, kve)
			}
		case SortedSetKeyPrefix:
			{
				if spec.ZEntriesSpec == nil || spec.ZEntriesSpec.Action == schema.EntryTypeAction_EXCLUDE {
					break
				}

				if spec.ZEntriesSpec.Action == schema.EntryTypeAction_ONLY_DIGEST {
					stx.Entries = append(stx.Entries, schema.TxEntryToProto(e))
					break
				}

				if spec.ZEntriesSpec.Action == schema.EntryTypeAction_RAW_VALUE {
					v, err := d.st.ReadValue(e)
					if errors.Is(err, store.ErrExpiredEntry) {
						break
					}
					if err != nil {
						return nil, err
					}

					kve := schema.TxEntryToProto(e)
					kve.Value = v
					stx.Entries = append(stx.Entries, kve)
					break
				}

				// zKey = [1+setLenLen+set+scoreLen+keyLenLen+1+key+txIDLen]
				zKey := e.Key()

				setLen := int(binary.BigEndian.Uint64(zKey[1:]))
				set := make([]byte, setLen)
				copy(set, zKey[1+setLenLen:])

				scoreOff := 1 + setLenLen + setLen
				scoreB := binary.BigEndian.Uint64(zKey[scoreOff:])
				score := math.Float64frombits(scoreB)

				keyOff := scoreOff + scoreLen + keyLenLen
				key := make([]byte, len(zKey)-keyOff-txIDLen)
				copy(key, zKey[keyOff:])

				atTx := binary.BigEndian.Uint64(zKey[keyOff+len(key):])

				var entry *schema.Entry
				var err error

				if snap != nil {
					entry, err = d.getAtTx(ctx, key, atTx, 1, snap, 0, skipIntegrityCheck)
					if errors.Is(err, store.ErrKeyNotFound) || errors.Is(err, store.ErrExpiredEntry) {
						break // ignore deleted ones (referenced key may have been deleted)
					}
					if err != nil {
						return nil, err
					}
				}

				zentry := &schema.ZEntry{
					Set:   set,
					Key:   key[1:],
					Entry: entry,
					Score: score,
					AtTx:  atTx,
				}

				stx.ZEntries = append(stx.ZEntries, zentry)
			}
		case SQLPrefix:
			{
				if spec.SqlEntriesSpec == nil || spec.SqlEntriesSpec.Action == schema.EntryTypeAction_EXCLUDE {
					break
				}

				if spec.SqlEntriesSpec.Action == schema.EntryTypeAction_ONLY_DIGEST {
					stx.Entries = append(stx.Entries, schema.TxEntryToProto(e))
					break
				}

				if spec.SqlEntriesSpec.Action == schema.EntryTypeAction_RAW_VALUE {
					v, err := d.st.ReadValue(e)
					if errors.Is(err, store.ErrExpiredEntry) {
						break
					}
					if err != nil {
						return nil, err
					}

					kve := schema.TxEntryToProto(e)
					kve.Value = v
					stx.Entries = append(stx.Entries, kve)
					break
				}

				return nil, fmt.Errorf("%w: sql entry resolution is not supported", ErrIllegalArguments)
			}
		}
	}

	return stx, nil
}

func (d *db) mayUpdateReplicaState(committedTxID uint64, newReplicaState *schema.ReplicaState) error {
	d.replicaStatesMutex.Lock()
	defer d.replicaStatesMutex.Unlock()

	// clean up replicaStates
	// it's safe to remove up to latest tx committed in primary
	for uuid, st := range d.replicaStates {
		if st.precommittedTxID <= committedTxID {
			delete(d.replicaStates, uuid)
		}
	}

	if newReplicaState.PrecommittedTxID <= committedTxID {
		// as far as the primary is concerned, nothing really new has happened
		return nil
	}

	newReplicaAlh := schema.DigestFromProto(newReplicaState.PrecommittedAlh)

	replicaSt, ok := d.replicaStates[newReplicaState.UUID]
	if ok {
		if newReplicaState.PrecommittedTxID < replicaSt.precommittedTxID {
			return fmt.Errorf("%w: the newly informed replica state lags behind the previously informed one", ErrIllegalArguments)
		}

		if newReplicaState.PrecommittedTxID == replicaSt.precommittedTxID {
			// as of the last informed replica status update, nothing has changed
			return nil
		}

		// actual replication progress is informed by the replica
		replicaSt.precommittedTxID = newReplicaState.PrecommittedTxID
		replicaSt.precommittedAlh = newReplicaAlh
	} else {
		// replica informs first replication state
		d.replicaStates[newReplicaState.UUID] = &replicaState{
			precommittedTxID: newReplicaState.PrecommittedTxID,
			precommittedAlh:  newReplicaAlh,
		}
	}

	// check up to which tx enough replicas ack replication and it's safe to commit
	mayCommitUpToTxID := uint64(0)
	if len(d.replicaStates) > 0 {
		mayCommitUpToTxID = math.MaxUint64
	}

	allowances := 0

	// we may clean up replicaStates from those who are lagging behind commit
	for _, st := range d.replicaStates {
		if st.precommittedTxID < mayCommitUpToTxID {
			mayCommitUpToTxID = st.precommittedTxID
		}
		allowances++
	}

	if allowances >= d.options.syncAcks {
		err := d.st.AllowCommitUpto(mayCommitUpToTxID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *db) ExportTxByID(ctx context.Context, req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error) {
	if req == nil {
		return nil, 0, mayCommitUpToAlh, ErrIllegalArguments
	}

	if d.replicaStates == nil && req.ReplicaState != nil {
		return nil, 0, mayCommitUpToAlh, fmt.Errorf("%w: replica state was NOT expected", ErrIllegalState)
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, 0, mayCommitUpToAlh, err
	}
	defer d.releaseTx(tx)

	committedTxID, committedAlh := d.st.CommittedAlh()
	preCommittedTxID, _ := d.st.PrecommittedAlh()

	if req.ReplicaState != nil {
		if req.ReplicaState.CommittedTxID > 0 {
			// validate replica commit state
			if req.ReplicaState.CommittedTxID > committedTxID {
				return nil, committedTxID, committedAlh, fmt.Errorf("%w: replica commit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}

			// integrityCheck is currently required to validate Alh
			expectedReplicaCommitHdr, err := d.st.ReadTxHeader(req.ReplicaState.CommittedTxID, false, false)
			if err != nil {
				return nil, committedTxID, committedAlh, err
			}

			replicaCommittedAlh := schema.DigestFromProto(req.ReplicaState.CommittedAlh)

			if expectedReplicaCommitHdr.Alh() != replicaCommittedAlh {
				return nil, expectedReplicaCommitHdr.ID, expectedReplicaCommitHdr.Alh(), fmt.Errorf("%w: replica commit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}
		}

		if req.ReplicaState.PrecommittedTxID > 0 {
			// validate replica precommit state
			if req.ReplicaState.PrecommittedTxID > preCommittedTxID {
				return nil, committedTxID, committedAlh, fmt.Errorf("%w: replica precommit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}

			// integrityCheck is currently required to validate Alh
			expectedReplicaPrecommitHdr, err := d.st.ReadTxHeader(req.ReplicaState.PrecommittedTxID, true, false)
			if err != nil {
				return nil, committedTxID, committedAlh, err
			}

			replicaPreCommittedAlh := schema.DigestFromProto(req.ReplicaState.PrecommittedAlh)

			if expectedReplicaPrecommitHdr.Alh() != replicaPreCommittedAlh {
				return nil, expectedReplicaPrecommitHdr.ID, expectedReplicaPrecommitHdr.Alh(), fmt.Errorf("%w: replica precommit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}

			// primary will provide commit state to the replica so it can commit pre-committed transactions
			if req.ReplicaState.PrecommittedTxID < committedTxID {
				// if replica is behind current commit state in primary
				// return the alh up to the point known by the replica.
				// That way the replica is able to validate is following the right primary.
				mayCommitUpToTxID = req.ReplicaState.PrecommittedTxID
				mayCommitUpToAlh = replicaPreCommittedAlh
			} else {
				mayCommitUpToTxID = committedTxID
				mayCommitUpToAlh = committedAlh
			}
		}

		err = d.mayUpdateReplicaState(committedTxID, req.ReplicaState)
		if err != nil {
			return nil, mayCommitUpToTxID, mayCommitUpToAlh, err
		}
	}

	// it might be the case primary will commit some txs (even there could be inmem-precommitted txs)
	// current timeout it's not a special value but at least a relative one
	// note: primary might also be waiting ack from any replica (even this primary may do progress)

	// TODO: under some circumstances, replica might not be able to do further progress until primary
	// has made changes, such wait doesn't need to have a timeout, reducing networking and CPU utilization
	var cancel context.CancelFunc

	if req.ReplicaState != nil {
		ctx, cancel = context.WithTimeout(ctx, d.options.storeOpts.SyncFrequency*4)
		defer cancel()
	}

	err = d.WaitForTx(ctx, req.Tx, req.AllowPreCommitted)
	if ctx.Err() != nil {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, nil
	}
	if err != nil {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, err
	}

	txbs, err = d.st.ExportTx(req.Tx, req.AllowPreCommitted, req.SkipIntegrityCheck, tx)
	if err != nil {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, err
	}

	return txbs, mayCommitUpToTxID, mayCommitUpToAlh, nil
}

func (d *db) ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*schema.TxHeader, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if !d.isReplica() {
		return nil, ErrNotReplica
	}

	hdr, err := d.st.ReplicateTx(ctx, exportedTx, skipIntegrityCheck, waitForIndexing)
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

// AllowCommitUpto is used by replicas to commit transactions once committed in primary
func (d *db) AllowCommitUpto(txID uint64, alh [sha256.Size]byte) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if !d.isReplica() {
		return ErrNotReplica
	}

	// replica pre-committed state must be consistent with primary

	committedTxID, committedAlh := d.st.CommittedAlh()
	// handling a particular case in an optimized manner
	if committedTxID == txID {
		if committedAlh != alh {
			return fmt.Errorf("%w: replica commit state diverged from primary's", ErrIllegalState)
		}
		return nil
	}

	hdr, err := d.st.ReadTxHeader(txID, true, false)
	if err != nil {
		return err
	}

	if hdr.Alh() != alh {
		return fmt.Errorf("%w: replica commit state diverged from primary's", ErrIllegalState)
	}

	return d.st.AllowCommitUpto(txID)
}

func (d *db) DiscardPrecommittedTxsSince(txID uint64) error {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	_, err := d.st.DiscardPrecommittedTxsSince(txID)

	return err
}

// VerifiableTxByID ...
func (d *db) VerifiableTxByID(ctx context.Context, req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, fmt.Errorf("%w: latest txID=%d is lower than specified as initial tx=%d", ErrIllegalState, lastTxID, req.ProveSinceTx)
	}

	var snap *store.Snapshot
	var err error

	if !req.KeepReferencesUnresolved {
		snap, err = d.snapshotSince(ctx, []byte{SetKeyPrefix}, req.SinceTx)
		if err != nil {
			return nil, err
		}
		defer snap.Close()
	}

	reqTx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(reqTx)

	err = d.st.ReadTx(req.Tx, false, reqTx)
	if err != nil {
		return nil, err
	}

	var sourceTxHdr, targetTxHdr *store.TxHeader
	var rootTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		rootTxHdr = reqTx.Header()
	} else {
		rootTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false, false)
		if err != nil {
			return nil, err
		}
	}

	if req.ProveSinceTx <= req.Tx {
		sourceTxHdr = rootTxHdr
		targetTxHdr = reqTx.Header()
	} else {
		sourceTxHdr = reqTx.Header()
		targetTxHdr = rootTxHdr
	}

	dualProof, err := d.st.DualProof(sourceTxHdr, targetTxHdr)
	if err != nil {
		return nil, err
	}

	sReqTx, err := d.serializeTx(ctx, reqTx, req.EntriesSpec, snap, true)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        sReqTx,
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}

// TxScan ...
func (d *db) TxScan(ctx context.Context, req *schema.TxScanRequest) (*schema.TxList, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	if int(req.Limit) > d.maxResultSize {
		return nil, fmt.Errorf("%w: the specified limit (%d) is larger than the maximum allowed one (%d)",
			ErrResultSizeLimitExceeded, req.Limit, d.maxResultSize)
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	limit := int(req.Limit)
	if req.Limit == 0 {
		limit = d.maxResultSize
	}

	snap, err := d.snapshotSince(ctx, []byte{SetKeyPrefix}, req.SinceTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	txReader, err := d.st.NewTxReader(req.InitialTx, req.Desc, tx)
	if err != nil {
		return nil, err
	}

	txList := &schema.TxList{}

	for l := 1; l <= limit; l++ {
		tx, err := txReader.Read()
		if errors.Is(err, store.ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return nil, err
		}

		sTx, err := d.serializeTx(ctx, tx, req.EntriesSpec, snap, true)
		if err != nil {
			return nil, err
		}

		txList.Txs = append(txList.Txs, sTx)
	}

	return txList, nil
}

// History ...
func (d *db) History(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	if int(req.Limit) > d.maxResultSize {
		return nil, fmt.Errorf("%w: the specified limit (%d) is larger than the maximum allowed one (%d)",
			ErrResultSizeLimitExceeded, req.Limit, d.maxResultSize)
	}

	currTxID, _ := d.st.CommittedAlh()

	if req.SinceTx > currTxID {
		return nil, ErrIllegalArguments
	}

	waitUntilTx := req.SinceTx
	if waitUntilTx == 0 {
		waitUntilTx = currTxID
	}

	err := d.WaitForIndexingUpto(ctx, waitUntilTx)
	if err != nil {
		return nil, err
	}

	limit := int(req.Limit)
	if limit == 0 {
		limit = d.maxResultSize
	}

	key := EncodeKey(req.Key)

	valRefs, _, err := d.st.History(key, req.Offset, req.Desc, limit)
	if err != nil && err != store.ErrOffsetOutOfRange {
		return nil, err
	}

	list := &schema.Entries{
		Entries: make([]*schema.Entry, len(valRefs)),
	}

	for i, valRef := range valRefs {
		val, err := valRef.Resolve()
		if err != nil && err != store.ErrExpiredEntry {
			return nil, err
		}
		if len(val) > 0 {
			val = TrimPrefix(val)
		}

		list.Entries[i] = &schema.Entry{
			Tx:       valRef.Tx(),
			Key:      req.Key,
			Metadata: schema.KVMetadataToProto(valRef.KVMetadata()),
			Value:    val,
			Expired:  errors.Is(err, store.ErrExpiredEntry),
			Revision: valRef.HC(),
		}
	}
	return list, nil
}

func (d *db) IsClosed() bool {
	d.closingMutex.Lock()
	defer d.closingMutex.Unlock()

	return d.st.IsClosed()
}

// Close ...
func (d *db) Close() (err error) {
	d.closingMutex.Lock()
	defer d.closingMutex.Unlock()

	d.Logger.Infof("closing database '%s'...", d.name)

	defer func() {
		if err == nil {
			d.Logger.Infof("database '%s' successfully closed", d.name)
		} else {
			d.Logger.Infof("%v: while closing database '%s'", err, d.name)
		}
	}()

	return d.st.Close()
}

// GetName ...
func (d *db) GetName() string {
	return d.name
}

// GetOptions ...
func (d *db) GetOptions() *Options {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.options
}

func (d *db) AsReplica(asReplica, syncReplication bool, syncAcks int) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.replicaStatesMutex.Lock()
	defer d.replicaStatesMutex.Unlock()

	d.options.replica = asReplica
	d.options.syncAcks = syncAcks
	d.options.syncReplication = syncReplication

	if asReplica {
		d.replicaStates = nil
	} else if syncAcks > 0 {
		d.replicaStates = make(map[string]*replicaState, syncAcks)
	}

	d.st.SetExternalCommitAllowance(syncReplication)
}

func (d *db) IsReplica() bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.isReplica()
}

func (d *db) isReplica() bool {
	return d.options.replica
}

func (d *db) IsSyncReplicationEnabled() bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.options.syncReplication
}

func (d *db) SetSyncReplication(enabled bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.st.SetExternalCommitAllowance(enabled)

	d.options.syncReplication = enabled
}

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
	}
	return err
}

// CopyCatalog creates a copy of the sql catalog and returns a transaction
// that can be used to commit the copy.
func (d *db) CopyCatalogToTx(ctx context.Context, tx *store.OngoingTx) error {
	// copy the sql catalog
	err := d.sqlEngine.CopyCatalogToTx(ctx, tx)
	if err != nil {
		return err
	}

	// copy the document store catalog
	err = d.documentEngine.CopyCatalogToTx(ctx, tx)
	if err != nil {
		return err
	}

	return nil
}

func (d *db) FindTruncationPoint(ctx context.Context, until time.Time) (*schema.TxHeader, error) {
	hdr, err := d.st.LastTxUntil(until)
	if errors.Is(err, store.ErrTxNotFound) {
		return nil, ErrRetentionPeriodNotReached
	}
	if err != nil {
		return nil, err
	}

	// look for the newst transaction with entries
	for err == nil {
		if hdr.NEntries > 0 {
			break
		}

		if ctx.Err() != nil {
			return nil, err
		}

		hdr, err = d.st.ReadTxHeader(hdr.ID-1, false, false)
	}
	return schema.TxHeaderToProto(hdr), nil
}

func (d *db) TruncateUptoTx(_ context.Context, txID uint64) error {
	return d.st.TruncateUptoTx(txID)
}
