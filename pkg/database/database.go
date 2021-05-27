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
	"github.com/golang/protobuf/ptypes/empty"
)

const MaxKeyResolutionLimit = 1
const MaxKeyScanLimit = 1000

var ErrMaxKeyResolutionLimitReached = errors.New("max key resolution limit reached. It may be due to cyclic references")
var ErrMaxKeyScanLimitExceeded = errors.New("max key scan limit exceeded")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrIllegalState = store.ErrIllegalState

type DB interface {
	Health(e *empty.Empty) (*schema.HealthResponse, error)
	CurrentState() (*schema.ImmutableState, error)
	WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error
	Set(req *schema.SetRequest) (*schema.TxMetadata, error)
	Get(req *schema.KeyRequest) (*schema.Entry, error)
	VerifiableSet(req *schema.VerifiableSetRequest) (*schema.VerifiableTx, error)
	VerifiableGet(req *schema.VerifiableGetRequest) (*schema.VerifiableEntry, error)
	GetAll(req *schema.KeyListRequest) (*schema.Entries, error)
	ExecAll(operations *schema.ExecAllRequest) (*schema.TxMetadata, error)
	Size() (uint64, error)
	Count(prefix *schema.KeyPrefix) (*schema.EntryCount, error)
	CountAll() (*schema.EntryCount, error)
	TxByID(req *schema.TxRequest) (*schema.Tx, error)
	VerifiableTxByID(req *schema.VerifiableTxRequest) (*schema.VerifiableTx, error)
	TxScan(req *schema.TxScanRequest) (*schema.TxList, error)
	History(req *schema.HistoryRequest) (*schema.Entries, error)
	SetReference(req *schema.ReferenceRequest) (*schema.TxMetadata, error)
	VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error)
	ZAdd(req *schema.ZAddRequest) (*schema.TxMetadata, error)
	ZScan(req *schema.ZScanRequest) (*schema.ZEntries, error)
	VerifiableZAdd(req *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error)
	Scan(req *schema.ScanRequest) (*schema.Entries, error)
	Close() error
	GetOptions() *DbOptions
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
	st      *store.ImmuStore
	ctlogSt *store.ImmuStore

	sqlEngine *sql.Engine

	tx1, tx2 *store.Tx
	mutex    sync.RWMutex

	Logger  logger.Logger
	options *DbOptions

	name string
}

// OpenDb Opens an existing Database from disk
func OpenDb(op *DbOptions, systemDB DB, log logger.Logger) (DB, error) {
	var err error

	dbi := &db{
		Logger:  log,
		options: op,
		name:    op.dbName,
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())

	_, dbErr := os.Stat(dbDir)
	if os.IsNotExist(dbErr) {
		return nil, fmt.Errorf("Missing database directories")
	}

	dbi.st, err = store.Open(dbDir, op.GetStoreOptions().WithLog(log))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	dbi.tx1 = dbi.st.NewTx()
	dbi.tx2 = dbi.st.NewTx()

	if systemDB == nil {
		dbi.ctlogSt = dbi.st
	} else {
		catalogDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), "catalog")

		_, dbErr := os.Stat(catalogDir)
		migrateCatalog := os.IsNotExist(dbErr)

		dbi.ctlogSt, err = store.Open(catalogDir, op.GetStoreOptions().WithLog(log))
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
		}

		if migrateCatalog {
			dbi.Logger.Infof("Migrating catalog from systemdb to %s...", catalogDir)

			systemDBI, _ := systemDB.(*db)
			sqlEngine, err := sql.NewEngine(systemDBI.st, dbi.st, []byte{SQLPrefix})
			if err != nil {
				sqlEngine.Close()
				return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
			}

			err = sqlEngine.DumpCatalogTo(op.dbName, dbi.ctlogSt)
			if err != nil {
				sqlEngine.Close()
				return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
			}

			err = sqlEngine.Close()
			if err != nil {
				return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
			}

			dbi.Logger.Infof("Catalog successfully migrated from systemdb to %s", catalogDir)
		}
	}

	dbi.sqlEngine, err = sql.NewEngine(dbi.ctlogSt, dbi.st, []byte{SQLPrefix})
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	err = dbi.sqlEngine.UseDatabase(dbi.options.dbName)
	if err == sql.ErrDatabaseDoesNotExist {
		// Database registration may be needed when opening a database created with an older version of immudb (older than v1.0.0)

		log.Infof("Registering database '%s' in the catalog...", dbDir)
		_, _, err = dbi.sqlEngine.ExecPreparedStmts([]sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbi.options.dbName}}, nil, true)
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
		}
		log.Infof("Database '%s' successfully registered", dbDir)

		err = dbi.sqlEngine.UseDatabase(dbi.options.dbName)
	}
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	return dbi, nil
}

// NewDb Creates a new Database along with it's directories and files
func NewDb(op *DbOptions, systemDB DB, log logger.Logger) (DB, error) {
	var err error

	dbi := &db{
		Logger:  log,
		options: op,
		name:    op.dbName,
	}

	dbDir := filepath.Join(op.GetDbRootPath(), op.GetDbName())

	if _, dbErr := os.Stat(dbDir); dbErr == nil {
		return nil, fmt.Errorf("Database directories already exist")
	}

	if err = os.MkdirAll(dbDir, os.ModePerm); err != nil {
		return nil, logErr(dbi.Logger, "Unable to create data folder: %s", err)
	}

	dbi.st, err = store.Open(dbDir, op.GetStoreOptions().WithLog(log))
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	dbi.tx1 = dbi.st.NewTx()
	dbi.tx2 = dbi.st.NewTx()

	if systemDB == nil {
		dbi.ctlogSt = dbi.st
	} else {
		catalogDir := filepath.Join(op.GetDbRootPath(), op.GetDbName(), "catalog")

		dbi.ctlogSt, err = store.Open(catalogDir, op.GetStoreOptions().WithLog(log))
		if err != nil {
			return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
		}
	}

	dbi.sqlEngine, err = sql.NewEngine(dbi.ctlogSt, dbi.st, []byte{SQLPrefix})
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	_, _, err = dbi.sqlEngine.ExecPreparedStmts([]sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbi.options.dbName}}, nil, true)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	err = dbi.sqlEngine.UseDatabase(dbi.options.dbName)
	if err != nil {
		return nil, logErr(dbi.Logger, "Unable to open store: %s", err)
	}

	return dbi, nil
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
func (d *db) Set(req *schema.SetRequest) (*schema.TxMetadata, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.set(req)
}

func (d *db) set(req *schema.SetRequest) (*schema.TxMetadata, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	entries := make([]*store.KV, len(req.KVs))

	for i, kv := range req.KVs {
		if len(kv.Key) == 0 {
			return nil, ErrIllegalArguments
		}

		entries[i] = EncodeKV(kv.Key, kv.Value)
	}

	txMetatadata, err := d.st.Commit(entries, !req.NoWait)
	if err != nil {
		return nil, err
	}

	return schema.TxMetatadaTo(txMetatadata), nil
}

//Get ...
func (d *db) Get(req *schema.KeyRequest) (*schema.Entry, error) {
	if req == nil || len(req.Key) == 0 {
		return nil, ErrIllegalArguments
	}

	if req.AtTx > 0 && req.SinceTx > 0 {
		return nil, ErrIllegalArguments
	}

	err := d.st.WaitForIndexingUpto(req.SinceTx, nil)
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
	var ktx uint64
	var val []byte

	if atTx == 0 {
		val, ktx, _, err = index.Get(key)
		if err != nil {
			return nil, err
		}
	} else {
		val, err = d.readValue(key, atTx, tx)
		if err != nil {
			return nil, err
		}
		ktx = atTx
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
			Tx:   ktx,
			Key:  TrimPrefix(key),
			AtTx: atTx,
		}

		return entry, nil
	}

	return &schema.Entry{Key: TrimPrefix(key), Value: TrimPrefix(val), Tx: ktx}, err
}

func (d *db) readValue(key []byte, atTx uint64, tx *store.Tx) ([]byte, error) {
	err := d.st.ReadTx(atTx, tx)
	if err != nil {
		return nil, err
	}

	return d.st.ReadValue(tx, key)
}

//Health ...
func (d *db) Health(*empty.Empty) (*schema.HealthResponse, error) {
	return &schema.HealthResponse{Status: true, Version: fmt.Sprintf("%d", store.Version)}, nil
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

// WaitForIndexingUpto blocks caller until specified tx gets committed
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

	txMetatadata, err := d.Set(req.SetRequest)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	lastTx := d.tx1

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
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
		Tx:        schema.TxTo(lastTx),
		DualProof: schema.DualProofTo(dualProof),
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
		Tx:        schema.TxTo(txEntry),
		DualProof: schema.DualProofTo(dualProof),
	}

	return &schema.VerifiableEntry{
		Entry:          e,
		VerifiableTx:   verifiableTx,
		InclusionProof: schema.InclusionProofTo(inclusionProof),
	}, nil
}

//GetAll ...
func (d *db) GetAll(req *schema.KeyListRequest) (*schema.Entries, error) {
	err := d.st.WaitForIndexingUpto(req.SinceTx, nil)
	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	snapshot, err := d.st.SnapshotSince(req.SinceTx)
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

	return schema.TxTo(d.tx1), nil
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
		Tx:        schema.TxTo(reqTx),
		DualProof: schema.DualProofTo(dualProof),
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

		txList.Txs = append(txList.Txs, schema.TxTo(tx))
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

	err := d.st.WaitForIndexingUpto(req.SinceTx, nil)
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

		val, err := d.st.ReadValue(d.tx1, key)
		if err != nil {
			return nil, err
		}

		list.Entries[i] = &schema.Entry{Key: req.Key, Value: TrimPrefix(val), Tx: tx}
	}

	return list, nil
}

//Close ...
func (d *db) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

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
func (d *db) GetOptions() *DbOptions {
	return d.options
}

func logErr(log logger.Logger, formattedMessage string, err error) error {
	if err != nil {
		log.Errorf(formattedMessage, err)
	}
	return err
}
