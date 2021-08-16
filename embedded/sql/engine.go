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
package sql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"sync"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var ErrNoSupported = errors.New("not yet supported")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrColumnDoesNotExist = errors.New("column does not exist")
var ErrColumnNotIndexed = errors.New("column is not indexed")
var ErrInvalidPK = errors.New("primary key of invalid type. Supported types are: INTEGER, STRING[256], TIMESTAMP OR BLOB[256]")
var ErrLimitedAutoIncrement = errors.New("only INTEGER primary keys can be set as auto incremental")
var ErrNoValueForAutoIncrementalColumn = errors.New("no value should be specified for auto incremental columns")
var ErrDuplicatedColumn = errors.New("duplicated column")
var ErrInvalidColumn = errors.New("invalid column")
var ErrPKCanNotBeNull = errors.New("primary key can not be null")
var ErrNotNullableColumnCannotBeNull = errors.New("not nullable column can not be null")
var ErrIndexedColumnCanNotBeNull = errors.New("indexed column can not be null")
var ErrIndexAlreadyExists = errors.New("index already exists")
var ErrInvalidNumberOfValues = errors.New("invalid number of values provided")
var ErrInvalidValue = errors.New("invalid value provided")
var ErrInferredMultipleTypes = errors.New("inferred multiple types")
var ErrExpectingDQLStmt = errors.New("illegal statement. DQL statement expected")
var ErrLimitedOrderBy = errors.New("order is limit to one indexed column")
var ErrLimitedGroupBy = errors.New("group by requires ordering by the grouping column")
var ErrIllegalMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrCatalogNotReady = errors.New("catalog not ready")
var ErrNoMoreRows = store.ErrNoMoreEntries
var ErrLimitedJoins = errors.New("joins limited to tables")
var ErrInvalidTypes = errors.New("invalid types")
var ErrUnsupportedJoinType = errors.New("unsupported join type")
var ErrInvalidCondition = errors.New("invalid condition")
var ErrHavingClauseRequiresGroupClause = errors.New("having clause requires group clause")
var ErrNotComparableValues = errors.New("values are not comparable")
var ErrUnexpected = errors.New("unexpected error")
var ErrMaxKeyLengthExceeded = errors.New("max key length exceeded")
var ErrColumnIsNotAnAggregation = errors.New("column is not an aggregation")
var ErrLimitedCount = errors.New("only unbounded counting is supported i.e. COUNT()")
var ErrTxDoesNotExist = errors.New("tx does not exist")
var ErrDivisionByZero = errors.New("division by zero")
var ErrMissingParameter = errors.New("missing paramter")
var ErrUnsupportedParameter = errors.New("unsupported parameter")
var ErrLimitedIndex = errors.New("index creation is only supported on empty tables")
var ErrAlreadyClosed = errors.New("sql engine already closed")

var mKeyVal = [32]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

const asKey = true

const EncIDLen = 8
const EncLenLen = 4

type Engine struct {
	catalogStore *store.ImmuStore
	dataStore    *store.ImmuStore

	prefix []byte

	catalog *Catalog // in-mem current catalog (used for INSERT, DDL statements and SELECT statements without UseSnapshotStmt)

	implicitDB string

	snapshot       *store.Snapshot
	snapAsBeforeTx uint64

	closed bool

	mutex sync.RWMutex
}

func NewEngine(catalogStore, dataStore *store.ImmuStore, prefix []byte) (*Engine, error) {
	if catalogStore == nil || dataStore == nil {
		return nil, ErrIllegalArguments
	}

	e := &Engine{
		catalogStore: catalogStore,
		dataStore:    dataStore,
		prefix:       make([]byte, len(prefix)),
	}

	copy(e.prefix, prefix)

	return e, nil
}

// TODO (jeroiraz); this operation won't be needed with a transactional in-memory catalog
func (e *Engine) EnsureCatalogReady(cancellation <-chan struct{}) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	if e.catalog != nil {
		return nil
	}

	return e.loadCatalog(cancellation)
}

func (e *Engine) ReloadCatalog(cancellation <-chan struct{}) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	return e.loadCatalog(cancellation)
}

func (e *Engine) loadCatalog(cancellation <-chan struct{}) error {
	lastTxID, _ := e.catalogStore.Alh()
	err := e.catalogStore.WaitForIndexingUpto(lastTxID, cancellation)
	if err != nil {
		return err
	}

	latestCatalogSnap, err := e.catalogStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}
	defer latestCatalogSnap.Close()

	latestDataSnap := latestCatalogSnap

	if e.catalogStore != e.dataStore {
		lastTxID, _ := e.dataStore.Alh()
		err := e.dataStore.WaitForIndexingUpto(lastTxID, cancellation)
		if err != nil {
			return err
		}

		latestDataSnap, err = e.dataStore.SnapshotSince(math.MaxUint64)
		if err != nil {
			return err
		}
		defer latestDataSnap.Close()
	}

	c, err := e.catalogFrom(latestCatalogSnap, latestDataSnap)
	if err != nil {
		return err
	}

	e.catalog = c
	return nil
}

func (e *Engine) Close() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	if e.snapshot != nil {
		err := e.snapshot.Close()
		if err != nil {
			return err
		}
	}

	e.closed = true

	return nil
}

func (e *Engine) UseDatabase(dbName string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
	if e.catalog == nil {
		return ErrCatalogNotReady
	}

	db, err := e.catalog.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	e.implicitDB = db.name

	return nil
}

func (e *Engine) DatabaseInUse() (*Database, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	return e.databaseInUse()
}

func (e *Engine) databaseInUse() (*Database, error) {
	if e.implicitDB == "" {
		return nil, ErrNoDatabaseSelected
	}

	return e.catalog.GetDatabaseByName(e.implicitDB)
}

func (e *Engine) UseSnapshot(sinceTx uint64, asBeforeTx uint64) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	return e.useSnapshot(sinceTx, asBeforeTx)
}

func (e *Engine) useSnapshot(sinceTx uint64, asBeforeTx uint64) error {
	if sinceTx > 0 && sinceTx < asBeforeTx {
		return ErrIllegalArguments
	}

	txID, _ := e.dataStore.Alh()
	if txID < sinceTx || txID < asBeforeTx {
		return ErrTxDoesNotExist
	}

	err := e.dataStore.WaitForIndexingUpto(sinceTx, nil)
	if err != nil {
		return err
	}

	if sinceTx == 0 {
		sinceTx = math.MaxUint64
	}

	if e.snapshot == nil || e.snapshot.Ts() < sinceTx {
		if e.snapshot != nil {
			err = e.snapshot.Close()
			if err != nil {
				return err
			}
		}

		e.snapshot, err = e.dataStore.SnapshotSince(sinceTx)
		if err != nil {
			return err
		}
	}

	e.snapAsBeforeTx = asBeforeTx

	return nil
}

func (e *Engine) getSnapshot() (*store.Snapshot, error) {
	if e.snapshot == nil {
		err := e.useSnapshot(0, 0)
		if err != nil {
			return nil, err
		}
	}

	return e.snapshot, nil
}

func (e *Engine) RenewSnapshot() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	return e.renewSnapshot()
}

func (e *Engine) renewSnapshot() error {
	if e.snapshot == nil {
		return e.useSnapshot(0, 0)
	}

	return e.useSnapshot(0, e.snapAsBeforeTx)
}

func (e *Engine) CloseSnapshot() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	if e.snapshot != nil {
		err := e.snapshot.Close()
		e.snapshot = nil
		return err
	}

	return nil
}

func (e *Engine) DumpCatalogTo(srcName, dstName string, targetStore *store.ImmuStore) error {
	if len(srcName) == 0 || len(dstName) == 0 || targetStore == nil {
		return ErrIllegalArguments
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return ErrAlreadyClosed
	}

	db, err := e.catalog.GetDatabaseByName(srcName)
	if err != nil {
		return err
	}

	snap, err := e.catalogStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}
	defer snap.Close()

	var entries []*store.KV

	dbKey := e.mapKey(catalogDatabasePrefix, EncodeID(db.ID()))

	entries = append(entries, &store.KV{Key: dbKey, Value: []byte(dstName)})

	tableEntries, err := e.entriesWithPrefix(e.mapKey(catalogTablePrefix, EncodeID(db.ID())), snap)
	if err != nil {
		return err
	}

	entries = append(entries, tableEntries...)

	colEntries, err := e.entriesWithPrefix(e.mapKey(catalogColumnPrefix, EncodeID(db.ID())), snap)
	if err != nil {
		return err
	}

	entries = append(entries, colEntries...)

	idxEntries, err := e.entriesWithPrefix(e.mapKey(catalogIndexPrefix), snap)
	if err != nil {
		return err
	}

	entries = append(entries, idxEntries...)

	_, err = targetStore.Commit(entries, true)

	return err
}

func (e *Engine) entriesWithPrefix(prefix []byte, snap *store.Snapshot) ([]*store.KV, error) {
	var entries []*store.KV

	dbReader, err := snap.NewKeyReader(&store.KeyReaderSpec{Prefix: prefix})
	if err != nil {
		return nil, err
	}
	defer dbReader.Close()

	for {
		mkey, vref, _, _, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		v, err := vref.Resolve()
		if err != nil {
			return nil, err
		}

		entries = append(entries, &store.KV{Key: mkey, Value: v})
	}

	return entries, nil
}

func (e *Engine) catalogFrom(catalogSnap, dataSnap *store.Snapshot) (*Catalog, error) {
	catalog := newCatalog()

	dbReaderSpec := &store.KeyReaderSpec{
		Prefix: e.mapKey(catalogDatabasePrefix),
	}

	dbReader, err := catalogSnap.NewKeyReader(dbReaderSpec)
	if err != nil {
		return nil, err
	}
	defer dbReader.Close()

	for {
		mkey, vref, _, _, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		id, err := e.unmapDatabaseID(mkey)
		if err != nil {
			return nil, err
		}

		v, err := vref.Resolve()
		if err != nil {
			return nil, err
		}

		db, err := catalog.newDatabase(id, string(v))
		if err != nil {
			return nil, err
		}

		err = e.loadTables(db, catalogSnap, dataSnap)
		if err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

func (e *Engine) loadTables(db *Database, catalogSnap, dataSnap *store.Snapshot) error {
	dbReaderSpec := &store.KeyReaderSpec{
		Prefix: e.mapKey(catalogTablePrefix, EncodeID(db.id)),
	}

	tableReader, err := catalogSnap.NewKeyReader(dbReaderSpec)
	if err != nil {
		return err
	}
	defer tableReader.Close()

	for {
		mkey, vref, _, _, err := tableReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		_, tableID, pkID, err := e.unmapTableID(mkey)
		if err != nil {
			return err
		}

		colSpecs, pkName, err := e.loadColSpecs(db.id, tableID, pkID, catalogSnap)
		if err != nil {
			return err
		}

		if len(colSpecs) < int(pkID) {
			return ErrCorruptedData
		}

		v, err := vref.Resolve()
		if err != nil {
			return err
		}

		table, err := db.newTable(string(v), colSpecs, pkName)
		if err != nil {
			return err
		}

		if tableID != table.id {
			return ErrCorruptedData
		}

		if table.pk.autoIncrement {
			val, err := e.loadMaxPK(dataSnap, table)
			if err == store.ErrNoMoreEntries {
				break
			}
			if err != nil {
				return err
			}

			table.maxPK = val.Value().(uint64)
		}

		indexes, err := e.loadIndexes(db.id, tableID, catalogSnap)
		if err != nil {
			return err
		}

		for _, colID := range indexes {
			table.indexes[colID] = struct{}{}
		}
	}

	return nil
}

func (e *Engine) loadMaxPK(dataSnap *store.Snapshot, table *Table) (TypedValue, error) {
	pkReaderSpec := &store.KeyReaderSpec{
		Prefix:    e.mapKey(RowPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(table.pk.id)),
		DescOrder: true,
	}

	pkReader, err := dataSnap.NewKeyReader(pkReaderSpec)
	if err != nil {
		return nil, err
	}
	defer pkReader.Close()

	mkey, _, _, _, err := pkReader.Read()
	if err != nil {
		return nil, err
	}

	_, _, _, _, encMaxPK, err := e.unmapRow(mkey)
	if err != nil {
		return nil, err
	}

	val, _, err := DecodeValue(encMaxPK, IntegerType)
	if err != nil {
		return nil, err
	}

	return val, err
}

func (e *Engine) loadColSpecs(dbID, tableID, pkID uint64, snap *store.Snapshot) (specs []*ColSpec, pkName string, err error) {
	initialKey := e.mapKey(catalogColumnPrefix, EncodeID(dbID), EncodeID(tableID))

	dbReaderSpec := &store.KeyReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	colSpecReader, err := snap.NewKeyReader(dbReaderSpec)
	if err != nil {
		return nil, "", err
	}
	defer colSpecReader.Close()

	specs = make([]*ColSpec, 0)

	pkFound := false

	for {
		mkey, vref, _, _, err := colSpecReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, "", err
		}

		_, _, colID, colType, err := e.unmapColSpec(mkey)
		if err != nil {
			return nil, "", err
		}

		v, err := vref.Resolve()
		if err != nil {
			return nil, "", err
		}
		if len(v) < 1 {
			return nil, "", ErrCorruptedData
		}

		spec := &ColSpec{
			colName:       string(v[1:]),
			colType:       colType,
			autoIncrement: v[0]&autoIncrementFlag != 0,
			notNull:       v[0]&nullableFlag != 0,
		}

		specs = append(specs, spec)

		if int(colID) != len(specs) {
			return nil, "", ErrCorruptedData
		}

		if colID == pkID {
			pkName = spec.colName
			pkFound = true
		}
	}

	if !pkFound {
		return nil, "", ErrCorruptedData
	}

	return
}

func (e *Engine) loadIndexes(dbID, tableID uint64, snap *store.Snapshot) ([]uint64, error) {
	initialKey := e.mapKey(catalogIndexPrefix, EncodeID(dbID), EncodeID(tableID))

	idxReaderSpec := &store.KeyReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	idxSpecReader, err := snap.NewKeyReader(idxReaderSpec)
	if err != nil {
		return nil, err
	}
	defer idxSpecReader.Close()

	indexes := make([]uint64, 0)

	for {
		mkey, _, _, _, err := idxSpecReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		_, _, colID, err := e.unmapIndex(mkey)
		if err != nil {
			return nil, err
		}

		indexes = append(indexes, colID)
	}

	return indexes, nil
}

func (e *Engine) trimPrefix(mkey []byte, mappingPrefix []byte) ([]byte, error) {
	if len(e.prefix)+len(mappingPrefix) > len(mkey) ||
		!bytes.Equal(e.prefix, mkey[:len(e.prefix)]) ||
		!bytes.Equal(mappingPrefix, mkey[len(e.prefix):len(e.prefix)+len(mappingPrefix)]) {
		return nil, ErrIllegalMappedKey
	}

	return mkey[len(e.prefix)+len(mappingPrefix):], nil
}

func (e *Engine) unmapDatabaseID(mkey []byte) (dbID uint64, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogDatabasePrefix))
	if err != nil {
		return 0, err
	}

	if len(encID) != EncIDLen {
		return 0, ErrCorruptedData
	}

	return binary.BigEndian.Uint64(encID), nil
}

func (e *Engine) unmapTableID(mkey []byte) (dbID, tableID, pkID uint64, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogTablePrefix))
	if err != nil {
		return 0, 0, 0, err
	}

	if len(encID) != EncIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[EncIDLen:])
	pkID = binary.BigEndian.Uint64(encID[2*EncIDLen:])

	return
}

func (e *Engine) unmapColSpec(mkey []byte) (dbID, tableID, colID uint64, colType SQLValueType, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogColumnPrefix))
	if err != nil {
		return 0, 0, 0, "", err
	}

	if len(encID) < EncIDLen*3 {
		return 0, 0, 0, "", ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[EncIDLen:])
	colID = binary.BigEndian.Uint64(encID[2*EncIDLen:])

	colType, err = asType(string(encID[EncIDLen*3:]))
	if err != nil {
		return 0, 0, 0, "", ErrCorruptedData
	}

	return
}

func asType(t string) (SQLValueType, error) {
	if t == IntegerType ||
		t == BooleanType ||
		t == VarcharType ||
		t == BLOBType ||
		t == TimestampType {
		return t, nil
	}

	return t, ErrCorruptedData
}

func (e *Engine) unmapIndex(mkey []byte) (dbID, tableID, colID uint64, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogIndexPrefix))
	if err != nil {
		return 0, 0, 0, err
	}

	if len(encID) < EncIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[EncIDLen:])
	colID = binary.BigEndian.Uint64(encID[2*EncIDLen:])

	return
}

func (e *Engine) unmapRow(mkey []byte) (dbID, tableID, colID uint64, encVal, encPKVal []byte, err error) {
	enc, err := e.trimPrefix(mkey, []byte(RowPrefix))
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}

	if len(enc) < EncIDLen*3+EncLenLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	off := 0

	dbID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	tableID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	colID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	//read optional index value followed by pk value

	valLen := int(binary.BigEndian.Uint32(enc[off:]))
	off += EncLenLen

	if len(enc)-off < valLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	if len(enc)-off > valLen {
		encVal = make([]byte, EncLenLen+valLen)
		binary.BigEndian.PutUint32(encVal, uint32(valLen))
		copy(encVal[EncLenLen:], enc[off:off+valLen])
		off += int(valLen)

		if len(enc)-off < EncLenLen {
			return 0, 0, 0, nil, nil, ErrCorruptedData
		}

		valLen = int(binary.BigEndian.Uint32(enc[off:]))
		off += EncLenLen
	}

	// read encPKVal
	if len(enc)-off != valLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	encPKVal = make([]byte, EncLenLen+valLen)
	binary.BigEndian.PutUint32(encPKVal, uint32(valLen))
	copy(encPKVal[EncLenLen:], enc[off:])
	off += len(encPKVal)

	return
}

func (e *Engine) mapKey(mappingPrefix string, encValues ...[]byte) []byte {
	return MapKey(e.prefix, mappingPrefix, encValues...)
}

func MapKey(prefix []byte, mappingPrefix string, encValues ...[]byte) []byte {
	mkeyLen := len(prefix) + len(mappingPrefix)

	for _, ev := range encValues {
		mkeyLen += len(ev)
	}

	mkey := make([]byte, mkeyLen)

	off := 0

	copy(mkey, prefix)
	off += len(prefix)

	copy(mkey[off:], []byte(mappingPrefix))
	off += len(mappingPrefix)

	for _, ev := range encValues {
		copy(mkey[off:], ev)
		off += len(ev)
	}

	return mkey
}

func EncodeID(id uint64) []byte {
	var encID [EncIDLen]byte
	binary.BigEndian.PutUint64(encID[:], id)
	return encID[:]
}

func maxKeyVal(colType SQLValueType) []byte {
	switch colType {
	case IntegerType:
		{
			return mKeyVal[:EncIDLen]
		}
	}
	return mKeyVal[:]
}

func EncodeRawValue(val interface{}, colType SQLValueType, asKey bool) ([]byte, error) {
	switch colType {
	case VarcharType:
		{
			strVal, ok := val.(string)
			if !ok {
				return nil, ErrInvalidValue
			}

			if asKey && len(strVal) > len(maxKeyVal(VarcharType)) {
				return nil, ErrInvalidPK
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(strVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal)))
			copy(encv[EncLenLen:], []byte(strVal))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := val.(uint64)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [EncLenLen + EncIDLen]byte
			binary.BigEndian.PutUint32(encv[:], uint32(EncIDLen))
			binary.BigEndian.PutUint64(encv[EncLenLen:], intVal)

			return encv[:], nil
		}
	case BooleanType:
		{
			boolVal, ok := val.(bool)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [EncLenLen + 1]byte
			binary.BigEndian.PutUint32(encv[:], uint32(1))
			if boolVal {
				encv[EncLenLen] = 1
			}

			return encv[:], nil
		}
	case BLOBType:
		{
			var blobVal []byte

			if val != nil {
				b, ok := val.([]byte)
				if !ok {
					return nil, ErrInvalidValue
				}
				blobVal = b
			}

			if asKey && len(blobVal) > len(maxKeyVal(BLOBType)) {
				return nil, ErrInvalidPK
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(blobVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(blobVal)))
			copy(encv[EncLenLen:], blobVal)

			return encv[:], nil
		}
	}

	/*
		time
	*/

	return nil, ErrInvalidValue
}

func EncodeValue(val TypedValue, colType SQLValueType, asKey bool) ([]byte, error) {
	switch colType {
	case VarcharType:
		{
			strVal, ok := val.(*Varchar)
			if !ok {
				return nil, ErrInvalidValue
			}

			if asKey && len(strVal.val) > len(maxKeyVal(VarcharType)) {
				return nil, ErrInvalidPK
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(strVal.val))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal.val)))
			copy(encv[EncLenLen:], []byte(strVal.val))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := val.(*Number)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [EncLenLen + EncIDLen]byte
			binary.BigEndian.PutUint32(encv[:], uint32(EncIDLen))
			binary.BigEndian.PutUint64(encv[EncLenLen:], intVal.val)

			return encv[:], nil
		}
	case BooleanType:
		{
			boolVal, ok := val.(*Bool)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [EncLenLen + 1]byte
			binary.BigEndian.PutUint32(encv[:], uint32(1))
			if boolVal.val {
				encv[EncLenLen] = 1
			}

			return encv[:], nil
		}
	case BLOBType:
		{
			blobVal, ok := val.(*Blob)
			if !ok {
				return nil, ErrInvalidValue
			}

			if asKey && len(blobVal.val) > len(maxKeyVal(BLOBType)) {
				return nil, ErrInvalidPK
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(blobVal.val))
			binary.BigEndian.PutUint32(encv[:], uint32(len(blobVal.val)))
			copy(encv[EncLenLen:], blobVal.val)

			return encv[:], nil
		}
	}

	/*
		time
	*/

	return nil, ErrInvalidValue
}

func DecodeValue(b []byte, colType SQLValueType) (TypedValue, int, error) {
	if len(b) < EncLenLen {
		return nil, 0, ErrCorruptedData
	}

	vlen := int(binary.BigEndian.Uint32(b[:]))
	voff := EncLenLen

	if vlen < 0 || len(b) < voff+vlen {
		return nil, 0, ErrCorruptedData
	}

	switch colType {
	case VarcharType:
		{
			v := string(b[voff : voff+vlen])
			voff += vlen

			return &Varchar{val: v}, voff, nil
		}
	case IntegerType:
		{
			if vlen > 8 {
				return nil, 0, ErrCorruptedData
			}

			buff := [8]byte{0}
			copy(buff[8-vlen:], b[voff:voff+vlen])
			v := binary.BigEndian.Uint64(buff[:])
			voff += vlen

			return &Number{val: v}, voff, nil
		}
	case BooleanType:
		{
			if vlen != 1 {
				return nil, 0, ErrCorruptedData
			}

			v := b[voff] == 1
			voff += 1

			return &Bool{val: v}, voff, nil
		}
	case BLOBType:
		{
			v := b[voff : voff+vlen]
			voff += vlen

			return &Blob{val: v}, voff, nil
		}
	}

	return nil, 0, ErrCorruptedData
}

func (e *Engine) ExistDatabase(db string) (bool, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return false, ErrAlreadyClosed
	}

	if e.catalog == nil {
		return false, ErrCatalogNotReady
	}

	return e.catalog.ExistDatabase(db), nil
}

func (e *Engine) GetDatabaseByName(db string) (*Database, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	return e.catalog.GetDatabaseByName(db)
}

func (e *Engine) GetTableByName(dbName, tableName string) (*Table, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	return e.catalog.GetTableByName(dbName, tableName)
}

func (e *Engine) InferParameters(sql string) (map[string]SQLValueType, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	return e.inferParametersFrom(strings.NewReader(sql))
}

func (e *Engine) inferParametersFrom(r io.ByteReader) (map[string]SQLValueType, error) {
	stmts, err := Parse(r)
	if err != nil {
		return nil, err
	}

	// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	implicitDB, err := e.databaseInUse()
	if err != nil {
		return nil, err
	}

	params := make(map[string]SQLValueType)

	for _, stmt := range stmts {
		err = stmt.inferParameters(e, implicitDB, params)
		if err != nil {
			return nil, err
		}
	}

	return params, nil
}

func (e *Engine) InferParametersPreparedStmt(stmt SQLStmt) (map[string]SQLValueType, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	implicitDB, err := e.databaseInUse()
	if err != nil {
		return nil, err
	}

	params := make(map[string]SQLValueType)

	err = stmt.inferParameters(e, implicitDB, params)

	return params, err
}

// exist database directly on catalogStore: // existKey(e.mapKey(catalogDatabase, db), e.catalogStore)
func (e *Engine) QueryStmt(sql string, params map[string]interface{}, renewSnapshot bool) (RowReader, error) {
	return e.Query(strings.NewReader(sql), params, renewSnapshot)
}

func (e *Engine) Query(sql io.ByteReader, params map[string]interface{}, renewSnapshot bool) (RowReader, error) {
	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, ErrExpectingDQLStmt
	}

	stmt, ok := stmts[0].(*SelectStmt)
	if !ok {
		return nil, ErrExpectingDQLStmt
	}

	return e.QueryPreparedStmt(stmt, params, renewSnapshot)
}

func (e *Engine) QueryPreparedStmt(stmt *SelectStmt, params map[string]interface{}, renewSnapshot bool) (RowReader, error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
	if e.catalog == nil {
		return nil, ErrCatalogNotReady
	}

	if renewSnapshot {
		err := e.renewSnapshot()
		if err != nil && err != tbtree.ErrReadersNotClosed {
			return nil, err
		}
	}

	snapshot, err := e.getSnapshot()
	if err != nil {
		return nil, err
	}

	implicitDB, err := e.databaseInUse()
	if err != nil {
		return nil, err
	}

	_, err = stmt.compileUsing(e, implicitDB, params)
	if err != nil {
		return nil, err
	}

	return stmt.Resolve(e, implicitDB, snapshot, params, nil)
}

func (e *Engine) ExecStmt(sql string, params map[string]interface{}, waitForIndexing bool) (summary *ExecSummary, err error) {
	return e.Exec(strings.NewReader(sql), params, waitForIndexing)
}

func (e *Engine) Exec(sql io.ByteReader, params map[string]interface{}, waitForIndexing bool) (summary *ExecSummary, err error) {
	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	return e.ExecPreparedStmts(stmts, params, waitForIndexing)
}

type ExecSummary struct {
	DDTxs []*store.TxMetadata
	DMTxs []*store.TxMetadata

	UpdatedRows     int
	LastInsertedPKs map[string]uint64
}

func (e *Engine) ExecPreparedStmts(stmts []SQLStmt, params map[string]interface{}, waitForIndexing bool) (summary *ExecSummary, err error) {
	if len(stmts) == 0 {
		return nil, ErrIllegalArguments
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	if e.catalog == nil {
		err := e.loadCatalog(nil)
		if err != nil {
			return nil, err
		}
	}

	implicitDB, err := e.databaseInUse()
	if err != nil && err != ErrNoDatabaseSelected {
		return nil, err
	}

	summary = &ExecSummary{
		LastInsertedPKs: make(map[string]uint64),
	}

	for _, stmt := range stmts {
		txSummary, err := stmt.compileUsing(e, implicitDB, params)
		if err != nil {
			e.resetCatalog() // in-memory catalog changes needs to be reverted
			return summary, err
		}

		implicitDB = txSummary.db

		if len(txSummary.ces) > 0 && len(txSummary.des) > 0 {
			e.resetCatalog() // in-memory catalog changes needs to be reverted
			return summary, ErrDDLorDMLTxOnly
		}

		if len(txSummary.ces) > 0 {
			txmd, err := e.catalogStore.Commit(txSummary.ces, waitForIndexing)
			// TODO (jeroiraz): implement transactional in-memory catalog
			if err != nil {
				e.resetCatalog() // in-memory catalog changes needs to be reverted
				return summary, err
			}

			summary.DDTxs = append(summary.DDTxs, txmd)
		}

		if len(txSummary.des) > 0 {
			txmd, err := e.dataStore.Commit(txSummary.des, waitForIndexing)
			if err != nil {
				e.resetCatalog() // in-memory catalog changes needs to be reverted
				return summary, err
			}

			summary.DMTxs = append(summary.DMTxs, txmd)
		}

		summary.UpdatedRows += txSummary.updatedRows

		for t, pk := range txSummary.lastInsertedPKs {
			summary.LastInsertedPKs[t] = pk
		}
	}

	e.catalog.mutated = false

	return summary, nil
}

func (e *Engine) resetCatalog() {
	if !e.catalog.mutated {
		return
	}

	e.catalog = nil
}
