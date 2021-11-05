/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
)

const MaxKeyResolutionLimit = 1
const MaxKeyScanLimit = 1000

const dbInstanceName = "dbinstance"

var ErrMaxKeyResolutionLimitReached = errors.New("max key resolution limit reached. It may be due to cyclic references")
var ErrMaxKeyScanLimitExceeded = errors.New("max key scan limit exceeded")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrIllegalState = store.ErrIllegalState
var ErrIsReplica = errors.New("database is read-only because it's a replica")
var ErrNotReplica = errors.New("database is NOT a replica")

type DB interface {
	CurrentState() (*schema.ImmutableState, error)
	WaitForTx(txID uint64, cancellation <-chan struct{}) error
	WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error
	Set(req *schema.SetRequest) (*schema.TxHeader, error)
	Get(req *schema.KeyRequest) (*schema.Entry, error)
	VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error)
	VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error)
	GetAll(req *schema.KeyListRequest) (*schema.Entries, error)
	Delete(req *schema.DeleteKeysRequest) (*schema.TxHeader, error)
	ExecAll(operations *schema.ExecAllRequest) (*schema.TxHeader, error)
	Size() (uint64, error)
	Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error)
	CountAll() (*schema.EntryCount, error)
	TxByID(req *schema.TxRequest) (*schema.Tx, error)
	ExportTxByID(req *schema.TxRequest) ([]byte, error)
	ReplicateTx(exportedTx []byte) (*schema.TxHeader, error)
	VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error)
	TxScan(req *schema.TxScanRequest) (*schema.TxList, error)
	History(req *schema.HistoryRequest) (*schema.Entries, error)
	SetReference(req *schema.ReferenceRequest) (*schema.TxHeader, error)
	VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error)
	ZAdd(req *schema.ZAddRequest) (*schema.TxHeader, error)
	ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error)
	VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error)
	Scan(req *schema.ScanRequest) (*schema.Entries, error)
	Close() error
	GetOptions() *Options
	AsReplica(asReplica bool)
	IsReplica() bool
	UseTimeFunc(timeFunc store.TimeFunc) error
	CompactIndex() error
	VerifiableSQLGet(req *schema.VerifiableSQLGetRequest) (*schema.VerifiableSQLEntry, error)
	SQLExec(req *schema.SQLExecRequest) (*schema.SQLExecResult, error)
	SQLExecPrepared(stmts []sql.SQLStmt, namedParams []*schema.NamedParam, waitForIndexing bool) (*schema.SQLExecResult, error)
	InferParameters(sql string) (map[string]sql.SQLValueType, error)
	InferParametersPrepared(stmt sql.SQLStmt) (map[string]sql.SQLValueType, error)
	UseSnapshot(req *schema.UseSnapshotRequest) error
	SQLQuery(req *schema.SQLQueryRequest) (*schema.SQLQueryResult, error)
	SQLQueryPrepared(stmt *sql.SelectStmt, namedParams []*schema.NamedParam, renewSnapshot bool) (*schema.SQLQueryResult, error)
	SQLQueryRowReader(stmt *sql.SelectStmt, renewSnapshot bool) (sql.RowReader, error)
	ListTables() (*schema.SQLQueryResult, error)
	DescribeTable(table string) (*schema.SQLQueryResult, error)
	GetName() string
}

//IDB database instance
type db struct {
	st *store.ImmuStore

	sqlEngine     *sql.Engine
	sqlInitCancel chan (struct{})
	sqlInit       sync.WaitGroup

	tx1, tx2 *store.Tx
	mutex    sync.RWMutex

	Logger  logger.Logger
	options *Options

	name string
}

// OpenDB Opens an existing Database from disk
func OpenDB(op *Options, log logger.Logger) (DB, error) {
	log.Infof("Opening database '%s' {replica = %v}...", op.dbName, op.replica)

	var err error

	dbi := &db{
		Logger:  log,
		options: op,
		name:    op.dbName,
	}

	dbDir := dbi.path()

	_, dbErr := os.Stat(dbDir)
	if os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("missing database directories: %s", dbDir)
	}

	dbi.st, err = store.Open(dbDir, op.GetStoreOptions().WithLog(log))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	dbi.tx1 = dbi.st.NewTx()
	dbi.tx2 = dbi.st.NewTx()

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, dbi.st, sql.DefaultOptions().WithPrefix([]byte{SQLPrefix}))
	if err != nil {
		return nil, err
	}

	if op.replica {
		dbi.Logger.Infof("Database '%s' {replica = %v} successfully opened", op.dbName, op.replica)
		return dbi, nil
	}

	dbi.sqlInitCancel = make(chan struct{})
	dbi.sqlInit.Add(1)

	go func() {
		defer dbi.sqlInit.Done()

		dbi.Logger.Infof("Loading SQL Engine for database '%s' {replica = %v}...", op.dbName, op.replica)

		err := dbi.initSQLEngine()
		if err != nil {
			dbi.Logger.Errorf("Unable to load SQL Engine for database '%s' {replica = %v}. %v", op.dbName, op.replica, err)
			return
		}

		dbi.Logger.Infof("SQL Engine ready for database '%s' {replica = %v}", op.dbName, op.replica)
	}()

	dbi.Logger.Infof("Database '%s' {replica = %v} successfully opened", op.dbName, op.replica)

	return dbi, nil
}

func (d *db) path() string {
	return filepath.Join(d.options.GetDBRootPath(), d.options.GetDBName())
}

func (d *db) initSQLEngine() error {
	err := d.sqlEngine.EnsureCatalogReady(d.sqlInitCancel)
	if err != nil {
		return err
	}

	// Warn about existent SQL data
	exists, err := d.st.ExistKeyWith(append([]byte{SQLPrefix}, []byte("CATALOG.TABLE.")...), nil, false)
	if err != nil {
		return err
	}
	if exists {
		d.Logger.Warningf("Existent SQL data won’t be automatically migrated. Please reach out to the immudb maintainers at the Discord channel if you need any assistance.")
	}

	err = d.sqlEngine.UseDatabase(dbInstanceName)
	if err != nil && err != sql.ErrDatabaseDoesNotExist {
		return err
	}

	if err == sql.ErrDatabaseDoesNotExist {
		_, err = d.sqlEngine.ExecPreparedStmts([]sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbInstanceName}}, nil, true)
		if err != nil {
			return logErr(d.Logger, "Unable to open store: %s", err)
		}

		err = d.sqlEngine.UseDatabase(dbInstanceName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *db) reloadSQLCatalog() error {
	err := d.sqlEngine.ReloadCatalog(nil)
	if err != nil {
		return err
	}

	err = d.sqlEngine.UseDatabase(dbInstanceName)
	if err == sql.ErrDatabaseDoesNotExist {
		return ErrSQLNotReady
	}

	return err
}

// NewDB Creates a new Database along with it's directories and files
func NewDB(op *Options, log logger.Logger) (DB, error) {
	log.Infof("Creating database '%s' {replica = %v}...", op.dbName, op.replica)

	var err error

	dbi := &db{
		Logger:  log,
		options: op,
		name:    op.dbName,
	}

	dbDir := filepath.Join(op.GetDBRootPath(), op.GetDBName())

	if _, dbErr := os.Stat(dbDir); dbErr == nil {
		return nil, fmt.Errorf("Database directories already exist")
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(dbi.Logger, "Unable to create data folder: %s", err)
	}

	dbi.st, err = store.Open(dbDir, op.GetStoreOptions().WithLog(log))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	dbi.tx1 = dbi.st.NewTx()
	dbi.tx2 = dbi.st.NewTx()

	dbi.sqlEngine, err = sql.NewEngine(dbi.st, dbi.st, sql.DefaultOptions().WithPrefix([]byte{SQLPrefix}))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
	}

	if !op.replica {
		_, err = dbi.sqlEngine.ExecPreparedStmts([]sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbInstanceName}}, nil, true)
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
		}

		err = dbi.sqlEngine.UseDatabase(dbInstanceName)
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open database: %s", err)
		}
	}

	dbi.Logger.Infof("Database '%s' successfully created {replica = %v}", op.dbName, op.replica)

	return dbi, nil
}

func (d *db) isReplica() bool {
	return d.options.replica
}

// UseTimeFunc ...
func (d *db) UseTimeFunc(timeFunc store.TimeFunc) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.st.UseTimeFunc(timeFunc)
}

// CompactIndex ...
func (d *db) CompactIndex() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	err := d.sqlEngine.CloseSnapshot()
	if err != nil {
		return err
	}

	err = d.st.CompactIndex()
	if err != nil {
		return err
	}

	return d.sqlEngine.RenewSnapshot()
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

	entries := make([]*store.EntrySpec, len(req.KVs))

	for i, kv := range req.KVs {
		if len(kv.Key) == 0 {
			return nil, ErrIllegalArguments
		}

		entries[i] = EncodeEntrySpec(kv.Key, schema.KVMetadataFromProto(kv.Metadata), kv.Value)
	}

	hdr, err := d.st.Commit(&store.TxSpec{Entries: entries, WaitForIndexing: !req.NoWait})
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

//Get ...
func (d *db) Get(req *schema.KeyRequest) (*schema.Entry, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, ErrIllegalArguments
	}

	currTxID, _ := d.st.Alh()

	if (req.AtTx > 0 && req.SinceTx > 0) || req.SinceTx > currTxID {
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

	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.getAt(EncodeKey(req.Key), req.AtTx, 0, d.st, d.tx1)
}

func (d *db) get(key []byte, index store.KeyIndex, tx *store.Tx) (*schema.Entry, error) {
	return d.getAt(key, 0, 0, index, tx)
}

func (d *db) getAt(key []byte, atTx uint64, resolved int, index store.KeyIndex, tx *store.Tx) (entry *schema.Entry, err error) {
	var txID uint64
	var val []byte
	var md *store.KVMetadata

	if atTx == 0 {
		valRef, err := index.Get(key, store.IgnoreDeleted)
		if err != nil {
			return nil, err
		}

		txID = valRef.Tx()

		md = valRef.KVMetadata()

		val, err = valRef.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		txID = atTx

		md, val, err = d.readValue(key, atTx, tx)
		if err != nil {
			return nil, err
		}
	}

	//Reference lookup
	if val[0] == ReferenceValuePrefix {
		if resolved == MaxKeyResolutionLimit {
			return nil, ErrMaxKeyResolutionLimitReached
		}

		atTx := binary.BigEndian.Uint64(TrimPrefix(val))
		refKey := make([]byte, len(val)-1-8)
		copy(refKey, val[1+8:])

		entry, err := d.getAt(refKey, atTx, resolved+1, index, tx)
		if err != nil {
			return nil, err
		}

		entry.ReferencedBy = &schema.Reference{
			Tx:       txID,
			Key:      TrimPrefix(key),
			Metadata: schema.KVMetadataToProto(md),
			AtTx:     atTx,
		}

		return entry, nil
	}

	return &schema.Entry{
		Tx:       txID,
		Key:      TrimPrefix(key),
		Metadata: schema.KVMetadataToProto(md),
		Value:    TrimPrefix(val),
	}, err
}

func (d *db) readValue(key []byte, atTx uint64, tx *store.Tx) (*store.KVMetadata, []byte, error) {
	err := d.st.ReadTx(atTx, tx)
	if err != nil {
		return nil, nil, err
	}

	return d.st.ReadValue(tx, key)
}

// CurrentState ...
func (d *db) CurrentState() (*schema.ImmutableState, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	lastTxID, lastTxAlh := d.st.Alh()

	return &schema.ImmutableState{
		TxId:   lastTxID,
		TxHash: lastTxAlh[:],
	}, nil
}

// WaitForTx blocks caller until specified tx gets committed
func (d *db) WaitForTx(txID uint64, cancellation <-chan struct{}) error {
	return d.st.WaitForTx(txID, cancellation)
}

// WaitForIndexingUpto blocks caller until specified tx gets indexed
func (d *db) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	return d.st.WaitForIndexingUpto(txID, cancellation)
}

//VerifiableSet ...
func (d *db) VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	txhdr, err := d.Set(req.SetRequest)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	lastTx := d.tx1

	err = d.st.ReadTx(uint64(txhdr.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveSinceTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.tx2

		err = d.st.ReadTx(req.ProveSinceTx, prevTx)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTx, lastTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxToProto(lastTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}

//VerifiableGet ...
func (d *db) VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	e, err := d.Get(req.KeyRequest)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	txEntry := d.tx1

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
	err = d.st.ReadTx(vTxID, txEntry)
	if err != nil {
		return nil, err
	}

	inclusionProof, err := d.tx1.Proof(EncodeKey(vKey))
	if err != nil {
		return nil, err
	}

	var rootTx *store.Tx

	if req.ProveSinceTx == 0 {
		rootTx = txEntry
	} else {
		rootTx = d.tx2

		err = d.st.ReadTx(req.ProveSinceTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	var sourceTx, targetTx *store.Tx

	if req.ProveSinceTx <= vTxID {
		sourceTx = rootTx
		targetTx = txEntry
	} else {
		sourceTx = txEntry
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	verifiableTx := &schema.VerifiableTx{
		Tx:        schema.TxToProto(txEntry),
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

	currTxID, _ := d.st.Alh()

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

	entries := make([]*store.EntrySpec, len(req.Keys))

	for i, k := range req.Keys {
		if len(k) == 0 {
			return nil, ErrIllegalArguments
		}

		entries[i] = EncodeEntrySpec(k, store.NewKVMetadata().AsDeleted(true), nil)
		entries[i].Constraint = store.MustExistAndNotDeleted
	}

	hdr, err := d.st.Commit(&store.TxSpec{Entries: entries, WaitForIndexing: !req.NoWait})
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

//GetAll ...
func (d *db) GetAll(req *schema.KeyListRequest) (*schema.Entries, error) {
	currTxID, _ := d.st.Alh()

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

	d.mutex.Lock()
	defer d.mutex.Unlock()

	snapshot, err := d.st.SnapshotSince(waitUntilTx)
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()

	list := &schema.Entries{}

	for _, key := range req.Keys {
		e, err := d.get(EncodeKey(key), snapshot, d.tx1)
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

//Size ...
func (d *db) Size() (uint64, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return d.st.TxCount(), nil
}

//Count ...
func (d *db) Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// CountAll ...
func (d *db) CountAll() (*schema.EntryCount, error) {
	return nil, fmt.Errorf("Functionality not yet supported: %s", "Count")
}

// TxByID ...
func (d *db) TxByID(req *schema.TxRequest) (*schema.Tx, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, ErrIllegalArguments
	}

	// key-value inclusion proof
	err := d.st.ReadTx(req.Tx, d.tx1)
	if err != nil {
		return nil, err
	}

	return schema.TxToProto(d.tx1), nil
}

func (d *db) ExportTxByID(req *schema.TxRequest) ([]byte, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, ErrIllegalArguments
	}

	return d.st.ExportTx(req.Tx, d.tx1)
}

func (d *db) ReplicateTx(exportedTx []byte) (*schema.TxHeader, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if !d.isReplica() {
		return nil, ErrNotReplica
	}

	hdr, err := d.st.ReplicateTx(exportedTx, false)
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}

//VerifiableTxByID ...
func (d *db) VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, ErrIllegalState
	}

	// key-value inclusion proof
	reqTx := d.tx1

	err := d.st.ReadTx(req.Tx, reqTx)
	if err != nil {
		return nil, err
	}

	var sourceTx, targetTx *store.Tx

	var rootTx *store.Tx

	if req.ProveSinceTx == 0 {
		rootTx = reqTx
	} else {
		rootTx = d.tx2

		err = d.st.ReadTx(req.ProveSinceTx, rootTx)
		if err != nil {
			return nil, err
		}
	}

	if req.ProveSinceTx <= req.Tx {
		sourceTx = rootTx
		targetTx = reqTx
	} else {
		sourceTx = reqTx
		targetTx = rootTx
	}

	dualProof, err := d.st.DualProof(sourceTx, targetTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxToProto(reqTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}

//TxScan ...
func (d *db) TxScan(req *schema.TxScanRequest) (*schema.TxList, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, ErrIllegalArguments
	}

	if req.Limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	limit := int(req.Limit)

	if req.Limit == 0 {
		limit = MaxKeyScanLimit
	}

	txReader, err := d.st.NewTxReader(req.InitialTx, req.Desc, d.tx1)
	if err != nil {
		return nil, err
	}

	txList := &schema.TxList{}

	for i := 0; i < limit; i++ {
		tx, err := txReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		txList.Txs = append(txList.Txs, schema.TxToProto(tx))
	}

	return txList, nil
}

//History ...
func (d *db) History(req *schema.HistoryRequest) (*schema.Entries, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if req == nil {
		return nil, ErrIllegalArguments
	}

	if req.Limit > MaxKeyScanLimit {
		return nil, ErrMaxKeyScanLimitExceeded
	}

	currTxID, _ := d.st.Alh()

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
		limit = MaxKeyScanLimit
	}

	key := EncodeKey(req.Key)

	txs, err := d.st.History(key, req.Offset, req.Desc, limit)
	if err != nil && err != store.ErrOffsetOutOfRange {
		return nil, err
	}

	list := &schema.Entries{
		Entries: make([]*schema.Entry, len(txs)),
	}

	for i, tx := range txs {
		err = d.st.ReadTx(tx, d.tx1)
		if err != nil {
			return nil, err
		}

		md, val, err := d.st.ReadValue(d.tx1, key)
		if err != nil {
			return nil, err
		}

		list.Entries[i] = &schema.Entry{
			Tx:       tx,
			Key:      req.Key,
			Metadata: schema.KVMetadataToProto(md),
			Value:    TrimPrefix(val),
		}
	}

	return list, nil
}

//Close ...
func (d *db) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.sqlInitCancel != nil {
		close(d.sqlInitCancel)
	}

	d.sqlInit.Wait() // Wait for SQL Engine initialization to conclude

	err := d.sqlEngine.Close()
	if err != nil {
		return err
	}

	return d.st.Close()
}

// GetName ...
func (d *db) GetName() string {
	return d.name
}

//GetOptions ...
func (d *db) GetOptions() *Options {
	return d.options
}

func (d *db) AsReplica(asReplica bool) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.options.replica = asReplica
}

func (d *db) IsReplica() bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.options.replica
}

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
	}
	return err
}
