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
var ErrIllegalMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrCatalogNotReady = errors.New("catalog not ready")
var ErrNoMoreRows = store.ErrNoMoreEntries
var ErrLimitedJoins = errors.New("joins limited to tables")
var ErrInvalidJointColumn = errors.New("invalid joint column")
var ErrJointColumnNotFound = errors.New("joint column not found")
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

	catalogRWMux sync.RWMutex

	implicitDB *Database

	snapshot       *store.Snapshot
	snapAsBeforeTx uint64

	closed bool

	mutex sync.Mutex
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

	err := e.LoadCatalog()
	if err != nil {
		return nil, err
	}

	return e, nil
}

// TODO (jeroiraz); this operation won't be needed with a transactional in-memory catalog
func (e *Engine) EnsureCatalogReady() error {
	e.catalogRWMux.Lock()
	defer e.catalogRWMux.Unlock()

	if e.catalog != nil {
		return nil
	}

	return e.LoadCatalog()
}

func (e *Engine) LoadCatalog() error {
	lastTxID, _ := e.catalogStore.Alh()
	err := e.catalogStore.WaitForIndexingUpto(lastTxID, nil)
	if err != nil {
		return err
	}

	latestSnapshot, err := e.catalogStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}
	defer latestSnapshot.Close()

	c, err := e.catalogFrom(latestSnapshot)
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

	e.catalogRWMux.RLock()
	defer e.catalogRWMux.RUnlock()

	if e.catalog == nil {
		// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
		return ErrCatalogNotReady
	}

	db, err := e.catalog.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	e.implicitDB = db

	return nil
}

func (e *Engine) DatabaseInUse() (*Database, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

	return e.implicitDB, nil
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

func (e *Engine) Snapshot() (*store.Snapshot, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.closed {
		return nil, ErrAlreadyClosed
	}

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

	e.mutex.Lock()
	defer e.mutex.Unlock()

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

func (e *Engine) catalogFrom(snap *store.Snapshot) (*Catalog, error) {
	catalog := newCatalog()

	initialKey := e.mapKey(catalogDatabasePrefix)
	dbReaderSpec := &store.KeyReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	dbReader, err := snap.NewKeyReader(dbReaderSpec)
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

		err = e.loadTables(db, snap)
		if err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

func (e *Engine) loadTables(db *Database, snap *store.Snapshot) error {
	initialKey := e.mapKey(catalogTablePrefix, EncodeID(db.id))

	dbReaderSpec := &store.KeyReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	tableReader, err := snap.NewKeyReader(dbReaderSpec)
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

		colSpecs, pkName, err := e.loadColSpecs(db.id, tableID, pkID, snap)
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

		indexes, err := e.loadIndexes(db.id, tableID, snap)
		if err != nil {
			return err
		}

		for _, colID := range indexes {
			table.indexes[colID] = struct{}{}
		}
	}

	return nil
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

		spec := &ColSpec{colName: string(v[1:]), colType: colType, notNull: v[0] == 1}

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

func (e *Engine) unmapIndexedRow(mkey []byte) (dbID, tableID, colID uint64, encVal, encPKVal []byte, err error) {
	enc, err := e.trimPrefix(mkey, []byte(RowPrefix))
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}

	if len(enc) < EncIDLen*3+2*EncLenLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	off := 0

	dbID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	tableID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	colID = binary.BigEndian.Uint64(enc[off:])
	off += EncIDLen

	//read index value
	valLen := int(binary.BigEndian.Uint32(enc[off:]))
	off += EncLenLen

	if len(enc)-off < valLen+EncLenLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	encVal = make([]byte, EncLenLen+valLen)
	binary.BigEndian.PutUint32(encVal, uint32(valLen))
	copy(encVal[EncLenLen:], enc[off:off+valLen])
	off += int(valLen)

	// read encPKVal
	pkValLen := int(binary.BigEndian.Uint32(enc[off:]))
	off += EncLenLen

	if len(enc)-off != pkValLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	encPKVal = make([]byte, EncLenLen+pkValLen)
	binary.BigEndian.PutUint32(encPKVal, uint32(pkValLen))
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

	if len(b) < vlen {
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
			v := binary.BigEndian.Uint64(b[voff : voff+vlen])
			voff += vlen

			return &Number{val: v}, voff, nil
		}
	case BooleanType:
		{
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

func (e *Engine) Catalog() *Catalog {
	e.catalogRWMux.RLock()
	defer e.catalogRWMux.RUnlock()

	return e.catalog
}

func (e *Engine) InferParameters(sql string) (map[string]SQLValueType, error) {
	return e.inferParametersFrom(strings.NewReader(sql))
}

func (e *Engine) inferParametersFrom(r io.ByteReader) (map[string]SQLValueType, error) {
	stmts, err := Parse(r)
	if err != nil {
		return nil, err
	}

	e.catalogRWMux.RLock()
	defer e.catalogRWMux.RUnlock()

	if e.catalog == nil {
		// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
		return nil, ErrCatalogNotReady
	}

	params := make(map[string]SQLValueType, 0)

	for _, stmt := range stmts {
		err = stmt.inferParameters(e, e.implicitDB, params)
		if err != nil {
			return nil, err
		}
	}

	return params, nil
}

func (e *Engine) InferParametersPreparedStmt(stmt SQLStmt) (map[string]SQLValueType, error) {
	e.catalogRWMux.RLock()
	defer e.catalogRWMux.RUnlock()

	if e.catalog == nil {
		// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
		return nil, ErrCatalogNotReady
	}

	params := make(map[string]SQLValueType, 0)

	err := stmt.inferParameters(e, e.implicitDB, params)

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
	if len(stmts) > 1 {
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

	e.catalogRWMux.RLock()
	defer e.catalogRWMux.RUnlock()

	if e.catalog == nil {
		// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
		return nil, ErrCatalogNotReady
	}

	if renewSnapshot {
		err := e.RenewSnapshot()
		if err != nil && err != tbtree.ErrReadersNotClosed {
			return nil, err
		}
	}

	snapshot, err := e.Snapshot()
	if err != nil {
		return nil, err
	}

	implicitDB, err := e.DatabaseInUse()
	if err != nil {
		return nil, err
	}

	_, _, _, err = stmt.compileUsing(e, implicitDB, params)
	if err != nil {
		return nil, err
	}

	return stmt.Resolve(e, implicitDB, snapshot, params, nil)
}

func (e *Engine) ExecStmt(sql string, params map[string]interface{}, waitForIndexing bool) (ddTxs, dmTxs []*store.TxMetadata, err error) {
	return e.Exec(strings.NewReader(sql), params, waitForIndexing)
}

func (e *Engine) Exec(sql io.ByteReader, params map[string]interface{}, waitForIndexing bool) (ddTxs, dmTxs []*store.TxMetadata, err error) {
	if e.catalog == nil {
		err := e.LoadCatalog()
		if err != nil {
			return nil, nil, err
		}
	}

	stmts, err := Parse(sql)
	if err != nil {
		return nil, nil, err
	}

	return e.ExecPreparedStmts(stmts, params, waitForIndexing)
}

func (e *Engine) ExecPreparedStmts(stmts []SQLStmt, params map[string]interface{}, waitForIndexing bool) (ddTxs, dmTxs []*store.TxMetadata, err error) {
	e.catalogRWMux.Lock()
	defer e.catalogRWMux.Unlock()

	if e.catalog == nil {
		// TODO (jeroiraz): won't be needed when in-memory catalog becomes transactional
		return nil, nil, ErrCatalogNotReady
	}

	implicitDB, err := e.DatabaseInUse()
	if err != nil {
		return nil, nil, err
	}

	for _, stmt := range stmts {
		centries, dentries, db, err := stmt.compileUsing(e, implicitDB, params)
		if err != nil {
			return ddTxs, dmTxs, err
		}

		implicitDB = db

		if len(centries) > 0 && len(dentries) > 0 {
			return ddTxs, dmTxs, ErrDDLorDMLTxOnly
		}

		if len(centries) > 0 {
			txmd, err := e.catalogStore.Commit(centries, waitForIndexing)
			if err != nil {
				// TODO (jeroiraz): implement transactional in-memory catalog
				e.catalog = nil // in-memory catalog changes needs to be reverted
				return ddTxs, dmTxs, err
			}

			ddTxs = append(ddTxs, txmd)
		}

		if len(dentries) > 0 {
			txmd, err := e.dataStore.Commit(dentries, waitForIndexing)
			if err != nil {
				return ddTxs, dmTxs, err
			}

			dmTxs = append(dmTxs, txmd)
		}
	}

	return ddTxs, dmTxs, nil
}
