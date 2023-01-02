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

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/watchers"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
)

const MaxKeyResolutionLimit = 1
const MaxKeyScanLimit = 1000

const dbInstanceName = "dbinstance"

var ErrKeyResolutionLimitReached = errors.New("key resolution limit reached. It may be due to cyclic references")
var ErrResultSizeLimitExceeded = errors.New("result size limit exceeded")
var ErrResultSizeLimitReached = errors.New("result size limit reached")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrIllegalState = store.ErrIllegalState
var ErrIsReplica = errors.New("database is read-only because it's a replica")
var ErrNotReplica = errors.New("database is NOT a replica")
var ErrReplicaDivergedFromPrimary = errors.New("replica diverged from primary")
var ErrInvalidRevision = errors.New("invalid key revision number")

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
	Set(req *schema.SetRequest) (*schema.TxHeader, error)
	VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error)

	Get(req *schema.KeyRequest) (*schema.Entry, error)
	VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error)
	GetAll(req *schema.KeyListRequest) (*schema.Entries, error)

	Delete(req *schema.DeleteKeysRequest) (*schema.TxHeader, error)

	SetReference(req *schema.ReferenceRequest) (*schema.TxHeader, error)
	VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error)

	Scan(req *schema.ScanRequest) (*schema.Entries, error)

	History(req *schema.HistoryRequest) (*schema.Entries, error)

	ExecAll(operations *schema.ExecAllRequest) (*schema.TxHeader, error)

	Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error)
	CountAll() (*schema.EntryCount, error)

	ZAdd(req *schema.ZAddRequest) (*schema.TxHeader, error)
	VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error)
	ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error)

	// SQL-related
	NewSQLTx(ctx context.Context, opts *sql.TxOptions) (*sql.SQLTx, error)

	SQLExec(req *schema.SQLExecRequest, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error)
	SQLExecPrepared(stmts []sql.SQLStmt, params map[string]interface{}, tx *sql.SQLTx) (ntx *sql.SQLTx, ctxs []*sql.SQLTx, err error)

	InferParameters(sql string, tx *sql.SQLTx) (map[string]sql.SQLValueType, error)
	InferParametersPrepared(stmt sql.SQLStmt, tx *sql.SQLTx) (map[string]sql.SQLValueType, error)

	SQLQuery(req *schema.SQLQueryRequest, tx *sql.SQLTx) (*schema.SQLQueryResult, error)
	SQLQueryPrepared(stmt sql.DataSource, namedParams []*schema.NamedParam, tx *sql.SQLTx) (*schema.SQLQueryResult, error)
	SQLQueryRowReader(stmt sql.DataSource, params map[string]interface{}, tx *sql.SQLTx) (sql.RowReader, error)

	VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error)

	ListTables(tx *sql.SQLTx) (*schema.SQLQueryResult, error)
	DescribeTable(table string, tx *sql.SQLTx) (*schema.SQLQueryResult, error)

	// Transactional layer
	WaitForTx(txID uint64, allowPrecommitted bool, cancellation <-chan struct{}) error
	WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error

	TxByID(req *schema.TxRequest) (*schema.Tx, error)
	ExportTxByID(req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error)
	ReplicateTx(exportedTx []byte) (*schema.TxHeader, error)
	AllowCommitUpto(txID uint64, alh [sha256.Size]byte) error
	DiscardPrecommittedTxsSince(txID uint64) error

	VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error)
	TxScan(req *schema.TxScanRequest) (*schema.TxList, error)

	// Maintenance
	FlushIndex(req *schema.FlushIndexRequest) error
	CompactIndex() error

	IsClosed() bool
	Close() error
}

type uuid = string

type replicaState struct {
	precommittedTxID uint64
	precommittedAlh  [sha256.Size]byte
}

// IDB database instance
type db struct {
	st *store.ImmuStore

	sqlEngine     *sql.Engine
	sqlInitCancel chan (struct{})
	sqlInit       sync.WaitGroup

	mutex        *instrumentedRWMutex
	closingMutex sync.Mutex

	Logger  logger.Logger
	options *Options

	name string

	maxResultSize int

	txPool store.TxPool

	replicaStates      map[uuid]*replicaState
	replicaStatesMutex sync.Mutex
}

// OpenDB Opens an existing Database from disk
func OpenDB(dbName string, multidbHandler sql.MultiDBHandler, op *Options, log logger.Logger) (DB, error) {
	if dbName == "" {
		return nil, fmt.Errorf("%w: invalid database name provided '%s'", ErrIllegalArguments, dbName)
	}

	log.Infof("Opening database '%s' {replica = %v}...", dbName, op.replica)

	var replicaStates map[uuid]*replicaState
	// replica states are only managed in primary with synchronous replication
	if !op.replica && op.syncAcks > 0 {
		replicaStates = make(map[uuid]*replicaState, op.syncAcks)
	}

	dbi := &db{
		Logger:        log,
		options:       op,
		name:          dbName,
		replicaStates: replicaStates,
		maxResultSize: MaxKeyScanLimit,
		mutex:         &instrumentedRWMutex{},
	}

	dbDir := dbi.Path()
	_, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("missing database directories: %s", dbDir)
	}

	stOpts := op.GetStoreOptions().
		WithLogger(log).
		WithExternalCommitAllowance(op.syncReplication)

	dbi.st, err = store.Open(dbDir, stOpts)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, sql.DefaultOptions().WithPrefix([]byte{SQLPrefix}))
	if err != nil {
		return nil, err
	}

	txPool, err := dbi.st.NewTxHolderPool(op.readTxPoolSize, false)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to create tx pool: %s", err)
	}
	dbi.txPool = txPool

	if op.replica {
		dbi.sqlEngine.SetMultiDBHandler(multidbHandler)

		dbi.Logger.Infof("Database '%s' {replica = %v} successfully opened", dbName, op.replica)
		return dbi, nil
	}

	dbi.sqlInitCancel = make(chan struct{})
	dbi.sqlInit.Add(1)

	go func() {
		defer dbi.sqlInit.Done()

		dbi.Logger.Infof("Loading SQL Engine for database '%s' {replica = %v}...", dbName, op.replica)

		err := dbi.initSQLEngine()
		if err != nil {
			dbi.Logger.Errorf("Unable to load SQL Engine for database '%s' {replica = %v}. %v", dbName, op.replica, err)
			return
		}

		dbi.sqlEngine.SetMultiDBHandler(multidbHandler)

		dbi.Logger.Infof("SQL Engine ready for database '%s' {replica = %v}", dbName, op.replica)
	}()

	dbi.Logger.Infof("Database '%s' {replica = %v} successfully opened", dbName, op.replica)

	return dbi, nil
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

func (d *db) initSQLEngine() error {
	err := d.sqlEngine.SetCurrentDatabase(dbInstanceName)
	if err != nil && err != sql.ErrDatabaseDoesNotExist {
		return err
	}

	if err == sql.ErrDatabaseDoesNotExist {
		return fmt.Errorf("automatic SQL initialization of databases created with older versions is now disabled. " +
			"Please reach out to the immudb maintainers at the Discord channel if you need any assistance")
	}

	return nil
}

// NewDB Creates a new Database along with it's directories and files
func NewDB(dbName string, multidbHandler sql.MultiDBHandler, op *Options, log logger.Logger) (DB, error) {
	if dbName == "" {
		return nil, fmt.Errorf("%w: invalid database name provided '%s'", ErrIllegalArguments, dbName)
	}

	log.Infof("Creating database '%s' {replica = %v}...", dbName, op.replica)

	var replicaStates map[uuid]*replicaState
	// replica states are only managed in primary with synchronous replication
	if !op.replica && op.syncAcks > 0 {
		replicaStates = make(map[uuid]*replicaState, op.syncAcks)
	}

	dbi := &db{
		Logger:        log,
		options:       op,
		name:          dbName,
		replicaStates: replicaStates,
		maxResultSize: MaxKeyScanLimit,
		mutex:         &instrumentedRWMutex{},
	}

	dbDir := filepath.Join(op.GetDBRootPath(), dbName)

	_, err := os.Stat(dbDir)
	if err == nil {
		return nil, fmt.Errorf("Database directories already exist: %s", dbDir)
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(dbi.Logger, "Unable to create data folder: %s", err)
	}

	stOpts := op.GetStoreOptions().WithLogger(log)
	// TODO: it's not currently possible to set:
	// WithExternalCommitAllowance(op.syncReplication) due to sql init steps

	dbi.st, err = store.Open(dbDir, stOpts)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	// TODO: may be moved to store opts once sql init steps are removed
	defer func() {
		dbi.st.SetExternalCommitAllowance(op.syncReplication)
	}()

	txPool, err := dbi.st.NewTxHolderPool(op.readTxPoolSize, false)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to create tx pool: %s", err)
	}
	dbi.txPool = txPool

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, sql.DefaultOptions().WithPrefix([]byte{SQLPrefix}))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	if !op.replica {
		// TODO: get rid off this sql initialization
		_, _, err = dbi.sqlEngine.ExecPreparedStmts([]sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbInstanceName}}, nil, nil)
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
		}

		err = dbi.sqlEngine.SetCurrentDatabase(dbInstanceName)
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
		}
	}

	dbi.sqlEngine.SetMultiDBHandler(multidbHandler)

	dbi.Logger.Infof("Database '%s' successfully created {replica = %v}", dbName, op.replica)

	return dbi, nil
}

func (d *db) MaxResultSize() int {
	return d.maxResultSize
}

// UseTimeFunc ...
func (d *db) UseTimeFunc(timeFunc store.TimeFunc) error {
	return d.st.UseTimeFunc(timeFunc)
}

func (d *db) FlushIndex(req *schema.FlushIndexRequest) error {
	if req == nil {
		return store.ErrIllegalArguments
	}

	return d.st.FlushIndex(req.CleanupPercentage, req.Synced)
}

// CompactIndex ...
func (d *db) CompactIndex() error {
	return d.st.CompactIndex()
}

// Set ...
func (d *db) Set(req *schema.SetRequest) (*schema.TxHeader, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	return d.set(req)
}

func (d *db) set(req *schema.SetRequest) (*schema.TxHeader, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	tx, err := d.st.NewWriteOnlyTx()
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
		hdr, err = tx.AsyncCommit()
	} else {
		hdr, err = tx.Commit()
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
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
func (d *db) Get(req *schema.KeyRequest) (*schema.Entry, error) {
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

		err := d.WaitForIndexingUpto(waitUntilTx, nil)
		if err != nil {
			return nil, err
		}
	}

	if req.AtRevision != 0 {
		return d.getAtRevision(EncodeKey(req.Key), req.AtRevision)
	}

	return d.getAtTx(EncodeKey(req.Key), req.AtTx, 0, d.st, 0)
}

func (d *db) get(key []byte, index store.KeyIndex) (*schema.Entry, error) {
	return d.getAtTx(key, 0, 0, index, 0)
}

func (d *db) getAtTx(
	key []byte,
	atTx uint64,
	resolved int,
	index store.KeyIndex,
	revision uint64,
) (entry *schema.Entry, err error) {
	var txID uint64
	var val []byte
	var md *store.KVMetadata

	if atTx == 0 {
		valRef, err := index.Get(key)
		if err != nil {
			return nil, err
		}

		txID = valRef.Tx()

		md = valRef.KVMetadata()

		val, err = valRef.Resolve()
		if err != nil {
			return nil, err
		}

		// Revision can be calculated from the history count
		revision = valRef.HC()

	} else {
		txID = atTx

		md, val, err = d.readMetadataAndValue(key, atTx)
		if err != nil {
			return nil, err
		}
	}

	return d.resolveValue(key, val, resolved, txID, md, index, revision)
}

func (d *db) getAtRevision(key []byte, atRevision int64) (entry *schema.Entry, err error) {
	var offset uint64
	var desc bool

	if atRevision > 0 {
		offset = uint64(atRevision) - 1
		desc = false
	} else {
		offset = -uint64(atRevision)
		desc = true
	}

	txs, hCount, err := d.st.History(key, offset, desc, 1)
	if errors.Is(err, store.ErrNoMoreEntries) || errors.Is(err, store.ErrOffsetOutOfRange) {
		return nil, ErrInvalidRevision
	}
	if err != nil {
		return nil, err
	}

	if atRevision < 0 {
		atRevision = int64(hCount) + atRevision
	}

	entry, err = d.getAtTx(key, txs[0], 0, d.st, uint64(atRevision))
	if err != nil {
		return nil, err
	}

	return entry, err
}

func (d *db) resolveValue(
	key []byte,
	val []byte,
	resolved int,
	txID uint64,
	md *store.KVMetadata,
	index store.KeyIndex,
	revision uint64,
) (entry *schema.Entry, err error) {
	if md != nil && md.Deleted() {
		return nil, store.ErrKeyNotFound
	}

	if len(val) < 1 {
		return nil, fmt.Errorf(
			"%w: internal value consistency error - missing value prefix",
			store.ErrCorruptedData,
		)
	}

	// Reference lookup
	if val[0] == ReferenceValuePrefix {
		if len(val) < 1+8 {
			return nil, fmt.Errorf(
				"%w: internal value consistency error - invalid reference",
				store.ErrCorruptedData,
			)
		}
		if resolved == MaxKeyResolutionLimit {
			return nil, ErrKeyResolutionLimitReached
		}

		atTx := binary.BigEndian.Uint64(TrimPrefix(val))
		refKey := make([]byte, len(val)-1-8)
		copy(refKey, val[1+8:])

		if index != nil {
			entry, err = d.getAtTx(refKey, atTx, resolved+1, index, 0)
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

func (d *db) readMetadataAndValue(key []byte, atTx uint64) (*store.KVMetadata, []byte, error) {
	entry, _, err := d.st.ReadTxEntry(atTx, key)
	if err != nil {
		return nil, nil, err
	}

	v, err := d.st.ReadValue(entry)
	if err != nil {
		return nil, nil, err
	}

	return entry.Metadata(), v, nil
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
func (d *db) WaitForTx(txID uint64, allowPrecommitted bool, cancellation <-chan struct{}) error {
	return d.st.WaitForTx(txID, allowPrecommitted, cancellation)
}

// WaitForIndexingUpto blocks caller until specified tx gets indexed
func (d *db) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	return d.st.WaitForIndexingUpto(txID, cancellation)
}

// VerifiableSet ...
func (d *db) VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
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

	txhdr, err := d.Set(req.SetRequest)
	if err != nil {
		return nil, err
	}

	err = d.st.ReadTx(uint64(txhdr.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		prevTxHdr = lastTx.Header()
	} else {
		prevTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false)
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
func (d *db) VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	e, err := d.Get(req.KeyRequest)
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

	err = d.st.ReadTx(vTxID, tx)
	if err != nil {
		return nil, err
	}

	var rootTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		rootTxHdr = tx.Header()
	} else {
		rootTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false)
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

func (d *db) Delete(req *schema.DeleteKeysRequest) (*schema.TxHeader, error) {
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
		opts.WithSnapshotMustIncludeTxID(func(_ uint64) uint64 {
			return req.SinceTx
		})
	}

	tx, err := d.st.NewTx(opts)
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

		err = tx.Delete(e.Key)
		if err != nil {
			return nil, err
		}
	}

	var hdr *store.TxHeader
	if req.NoWait {
		hdr, err = tx.AsyncCommit()
	} else {
		hdr, err = tx.Commit()
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

// GetAll ...
func (d *db) GetAll(req *schema.KeyListRequest) (*schema.Entries, error) {
	snap, err := d.snapshotSince(req.SinceTx)
	if err != nil {
		return nil, err
	}
	defer snap.Close()

	list := &schema.Entries{}

	for _, key := range req.Keys {
		e, err := d.get(EncodeKey(key), snap)
		if err == nil || err == store.ErrKeyNotFound {
			if e != nil {
				list.Entries = append(list.Entries, e)
			}
		} else {
			return nil, err
		}
	}

	return list, nil
}

// Size ...
func (d *db) Size() (uint64, error) {
	return d.st.TxCount(), nil
}

// Count ...
func (d *db) Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// CountAll ...
func (d *db) CountAll() (*schema.EntryCount, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// TxByID ...
func (d *db) TxByID(req *schema.TxRequest) (*schema.Tx, error) {
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
		snap, err = d.snapshotSince(req.SinceTx)
		if err != nil {
			return nil, err
		}
		defer snap.Close()
	}

	// key-value inclusion proof
	err = d.st.ReadTx(req.Tx, tx)
	if err != nil {
		return nil, err
	}

	return d.serializeTx(tx, req.EntriesSpec, snap)
}

func (d *db) snapshotSince(txID uint64) (*store.Snapshot, error) {
	currTxID, _ := d.st.CommittedAlh()

	if txID > currTxID {
		return nil, ErrIllegalArguments
	}

	waitUntilTx := txID
	if waitUntilTx == 0 {
		waitUntilTx = currTxID
	}

	return d.st.SnapshotMustIncludeTxID(waitUntilTx)
}

func (d *db) serializeTx(tx *store.Tx, spec *schema.EntriesSpec, snap *store.Snapshot) (*schema.Tx, error) {
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
				if err == store.ErrExpiredEntry {
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

				kve, err := d.resolveValue(e.Key(), v, 0, tx.Header().ID, e.Metadata(), index, 0)
				if err == store.ErrKeyNotFound || err == store.ErrExpiredEntry {
					// ignore deleted ones (referenced key may have been deleted)
					break
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
					if err == store.ErrExpiredEntry {
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
					entry, err = d.getAtTx(key, atTx, 1, snap, 0)
					if err == store.ErrKeyNotFound || err == store.ErrExpiredEntry {
						// ignore deleted ones (referenced key may have been deleted)
						break
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
					if err == store.ErrExpiredEntry {
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

func (d *db) ExportTxByID(req *schema.ExportTxRequest) (txbs []byte, mayCommitUpToTxID uint64, mayCommitUpToAlh [sha256.Size]byte, err error) {

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
				return nil, committedTxID, committedAlh,
					fmt.Errorf("%w: replica commit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}

			expectedReplicaCommitHdr, err := d.st.ReadTxHeader(req.ReplicaState.CommittedTxID, false)
			if err != nil {
				return nil, committedTxID, committedAlh, err
			}

			replicaCommittedAlh := schema.DigestFromProto(req.ReplicaState.CommittedAlh)

			if expectedReplicaCommitHdr.Alh() != replicaCommittedAlh {
				return nil, expectedReplicaCommitHdr.ID, expectedReplicaCommitHdr.Alh(),
					fmt.Errorf("%w: replica commit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}
		}

		if req.ReplicaState.PrecommittedTxID > 0 {
			// validate replica precommit state
			if req.ReplicaState.PrecommittedTxID > preCommittedTxID {
				return nil, committedTxID, committedAlh,
					fmt.Errorf("%w: replica precommit state diverged from primary's", ErrReplicaDivergedFromPrimary)
			}

			expectedReplicaPrecommitHdr, err := d.st.ReadTxHeader(req.ReplicaState.PrecommittedTxID, true)
			if err != nil {
				return nil, committedTxID, committedAlh, err
			}

			replicaPreCommittedAlh := schema.DigestFromProto(req.ReplicaState.PrecommittedAlh)

			if expectedReplicaPrecommitHdr.Alh() != replicaPreCommittedAlh {
				return nil, expectedReplicaPrecommitHdr.ID, expectedReplicaPrecommitHdr.Alh(),
					fmt.Errorf("%w: replica precommit state diverged from primary's", ErrReplicaDivergedFromPrimary)
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
	ctx, cancel := context.WithTimeout(context.Background(), d.options.storeOpts.SyncFrequency*4)
	defer cancel()

	err = d.WaitForTx(req.Tx, req.AllowPreCommitted, ctx.Done())
	if errors.Is(err, watchers.ErrCancellationRequested) {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, nil
	}
	if err != nil {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, err
	}

	txbs, err = d.st.ExportTx(req.Tx, req.AllowPreCommitted, tx)
	if err != nil {
		return nil, mayCommitUpToTxID, mayCommitUpToAlh, err
	}

	return txbs, mayCommitUpToTxID, mayCommitUpToAlh, nil
}

func (d *db) ReplicateTx(exportedTx []byte) (*schema.TxHeader, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if !d.isReplica() {
		return nil, ErrNotReplica
	}

	hdr, err := d.st.ReplicateTx(exportedTx, false)
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

	hdr, err := d.st.ReadTxHeader(txID, true)
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
func (d *db) VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
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
		snap, err = d.snapshotSince(req.SinceTx)
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

	err = d.st.ReadTx(req.Tx, reqTx)
	if err != nil {
		return nil, err
	}

	var sourceTxHdr, targetTxHdr *store.TxHeader
	var rootTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		rootTxHdr = reqTx.Header()
	} else {
		rootTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false)
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

	sReqTx, err := d.serializeTx(reqTx, req.EntriesSpec, snap)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        sReqTx,
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}

// TxScan ...
func (d *db) TxScan(req *schema.TxScanRequest) (*schema.TxList, error) {
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

	snap, err := d.snapshotSince(req.SinceTx)
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
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		sTx, err := d.serializeTx(tx, req.EntriesSpec, snap)
		if err != nil {
			return nil, err
		}

		txList.Txs = append(txList.Txs, sTx)

		if l == d.maxResultSize {
			return txList,
				fmt.Errorf("%w: found at least %d entries (maximum limit). "+
					"Pagination over large results can be achieved by using the limit and initialTx arguments",
					ErrResultSizeLimitReached, d.maxResultSize)
		}
	}

	return txList, nil
}

// History ...
func (d *db) History(req *schema.HistoryRequest) (*schema.Entries, error) {
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

	err := d.WaitForIndexingUpto(waitUntilTx, nil)
	if err != nil {
		return nil, err
	}

	limit := int(req.Limit)

	if req.Limit == 0 {
		limit = d.maxResultSize
	}

	key := EncodeKey(req.Key)

	txs, hCount, err := d.st.History(key, req.Offset, req.Desc, limit)
	if err != nil && err != store.ErrOffsetOutOfRange {
		return nil, err
	}

	list := &schema.Entries{
		Entries: make([]*schema.Entry, len(txs)),
	}

	revision := req.Offset + 1
	if req.Desc {
		revision = hCount - req.Offset
	}

	for i, txID := range txs {
		entry, _, err := d.st.ReadTxEntry(txID, key)
		if err != nil {
			return nil, err
		}

		val, err := d.st.ReadValue(entry)
		if err != nil && err != store.ErrExpiredEntry {
			return nil, err
		}
		if len(val) > 0 {
			val = TrimPrefix(val)
		}

		list.Entries[i] = &schema.Entry{
			Tx:       txID,
			Key:      req.Key,
			Metadata: schema.KVMetadataToProto(entry.Metadata()),
			Value:    val,
			Expired:  err == store.ErrExpiredEntry,
			Revision: revision,
		}

		if req.Desc {
			revision--
		} else {
			revision++
		}
	}

	if limit == d.maxResultSize && hCount >= uint64(d.maxResultSize) {
		return list,
			fmt.Errorf("%w: found at least %d entries (the maximum limit). "+
				"Pagination over large results can be achieved by using the limit and initialTx arguments",
				ErrResultSizeLimitReached, d.maxResultSize)
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

	d.Logger.Infof("Closing database '%s'...", d.name)

	defer func() {
		if err == nil {
			d.Logger.Infof("Database '%s' succesfully closed", d.name)
		} else {
			d.Logger.Infof("%v: while closing database '%s'", err, d.name)
		}
	}()

	if d.sqlInitCancel != nil {
		close(d.sqlInitCancel)
		d.sqlInitCancel = nil
	}

	d.sqlInit.Wait() // Wait for SQL Engine initialization to conclude

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
		d.replicaStates = make(map[uuid]*replicaState, syncAcks)
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
