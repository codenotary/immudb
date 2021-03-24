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
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var ErrIllegalArguments = store.ErrIllegalArguments
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrColumnDoesNotExist = errors.New("columns does not exist")
var ErrColumnNotIndexed = errors.New("column is not indexed")
var ErrInvalidPK = errors.New("primary key of invalid type. Supported types are: INTEGER, STRING[256], TIMESTAMP OR BLOB[256]")
var ErrDuplicatedColumn = errors.New("duplicated column")
var ErrInvalidColumn = errors.New("invalid column")
var ErrPKCanNotBeNull = errors.New("primary key can not be null")
var ErrIndexAlreadyExists = errors.New("index already exists")
var ErrInvalidNumberOfValues = errors.New("invalid number of values provided")
var ErrInvalidValue = errors.New("invalid value provided")
var ErrExpectingDQLStmt = errors.New("illegal statement. DQL statement expected")
var ErrLimitedOrderBy = errors.New("order is limit to one indexed column")
var ErrIllegelMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrNoMoreEntries = store.ErrNoMoreEntries
var ErrLimitedJoins = errors.New("joins limited to tables")
var ErrInvalidJointColumn = errors.New("invalid joint column")
var ErrJointColumnNotFound = errors.New("joint column not found")
var ErrUnsupportedJoinType = errors.New("unsupported join type")

var mKeyVal = [32]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

const asKey = true

const encIDLen = 8
const encLenLen = 4

type Engine struct {
	catalogStore *store.ImmuStore
	dataStore    *store.ImmuStore

	prefix []byte

	catalog *Catalog // in-mem current catalog (used for INSERT, DDL statements and SELECT statements without UseSnapshotStmt)

	catalogRWMux sync.RWMutex

	implicitDatabase string

	snapSinceTx, snapUptoTx uint64
}

func NewEngine(catalogStore, dataStore *store.ImmuStore, prefix []byte) (*Engine, error) {
	e := &Engine{
		catalogStore: catalogStore,
		dataStore:    dataStore,
		prefix:       make([]byte, len(prefix)),
	}

	copy(e.prefix, prefix)

	err := e.loadCatalog()
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) loadCatalog() error {
	e.catalog = nil

	lastTxID, _ := e.catalogStore.Alh()
	waitForIndexingUpto(e.catalogStore, lastTxID)

	latestSnapshot, err := e.catalogStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return err
	}

	c, err := e.catalogFrom(latestSnapshot)
	if err != nil {
		return err
	}

	e.catalog = c
	return nil
}

func waitForIndexingUpto(st *store.ImmuStore, txID uint64) error {
	if txID == 0 {
		return nil
	}

	for {
		its, err := st.IndexInfo()
		if err != nil {
			return err
		}

		if its >= txID {
			return nil
		}

		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

func (e *Engine) catalogFrom(snap *store.Snapshot) (*Catalog, error) {
	catalog := newCatalog()

	initialKey := e.mapKey(catalogDatabasePrefix)
	dbReaderSpec := &tbtree.ReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	dbReader, err := snap.NewKeyReader(dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		return catalog, nil
	}
	if err != nil {
		return nil, err
	}

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

		db, err := catalog.newDatabase(string(v))
		if err != nil {
			return nil, err
		}

		if id != db.id {
			return nil, ErrCorruptedData
		}

		err = e.loadTables(db, snap)
		if err != nil {
			return nil, err
		}
	}

	return catalog, nil
}

func (e *Engine) loadTables(db *Database, snap *store.Snapshot) error {
	initialKey := e.mapKey(catalogTablePrefix, encodeID(db.id))

	dbReaderSpec := &tbtree.ReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	tableReader, err := snap.NewKeyReader(dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		return nil
	}
	if err != nil {
		return err
	}

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
	initialKey := e.mapKey(catalogColumnPrefix, encodeID(dbID), encodeID(tableID))

	dbReaderSpec := &tbtree.ReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	colSpecReader, err := snap.NewKeyReader(dbReaderSpec)
	if err == store.ErrNoMoreEntries {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}

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

		spec := &ColSpec{colName: string(v), colType: colType}

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
	initialKey := e.mapKey(catalogIndexPrefix, encodeID(dbID), encodeID(tableID))

	idxReaderSpec := &tbtree.ReaderSpec{
		SeekKey: initialKey,
		Prefix:  initialKey,
	}

	idxSpecReader, err := snap.NewKeyReader(idxReaderSpec)
	if err == store.ErrNoMoreEntries {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

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
		return nil, ErrIllegelMappedKey
	}

	return mkey[len(e.prefix)+len(mappingPrefix):], nil
}

func (e *Engine) unmapDatabaseID(mkey []byte) (dbID uint64, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogDatabasePrefix))
	if err != nil {
		return 0, err
	}

	if len(encID) != encIDLen {
		return 0, ErrCorruptedData
	}

	return binary.BigEndian.Uint64(encID), nil
}

func (e *Engine) unmapTableID(mkey []byte) (dbID, tableID, pkID uint64, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogTablePrefix))
	if err != nil {
		return 0, 0, 0, err
	}

	if len(encID) != encIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[encIDLen:])
	pkID = binary.BigEndian.Uint64(encID[2*encIDLen:])

	return
}

func (e *Engine) unmapColSpec(mkey []byte) (dbID, tableID, colID uint64, colType SQLValueType, err error) {
	encID, err := e.trimPrefix(mkey, []byte(catalogColumnPrefix))
	if err != nil {
		return 0, 0, 0, "", err
	}

	if len(encID) < encIDLen*3 {
		return 0, 0, 0, "", ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[encIDLen:])
	colID = binary.BigEndian.Uint64(encID[2*encIDLen:])

	colType, err = asType(string(encID[encIDLen*3:]))
	if err != nil {
		return 0, 0, 0, "", ErrCorruptedData
	}

	return
}

func asType(t string) (SQLValueType, error) {
	if t == IntegerType ||
		t == BooleanType ||
		t == StringType ||
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

	if len(encID) < encIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint64(encID)
	tableID = binary.BigEndian.Uint64(encID[encIDLen:])
	colID = binary.BigEndian.Uint64(encID[2*encIDLen:])

	return
}

func (e *Engine) unmapIndexedRow(mkey []byte) (dbID, tableID, colID uint64, encVal, encPKVal []byte, err error) {
	enc, err := e.trimPrefix(mkey, []byte(rowPrefix))
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}

	if len(enc) < encIDLen*3+2*encLenLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	off := 0

	dbID = binary.BigEndian.Uint64(enc[off:])
	off += encIDLen

	tableID = binary.BigEndian.Uint64(enc[off:])
	off += encIDLen

	colID = binary.BigEndian.Uint64(enc[off:])
	off += encIDLen

	//read index value
	valLen := int(binary.BigEndian.Uint32(enc[off:]))
	off += encLenLen

	if len(enc)-off < valLen+encLenLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	encPKVal = make([]byte, encLenLen+valLen)
	binary.BigEndian.PutUint32(encPKVal, uint32(valLen))
	copy(encPKVal[encLenLen:], enc[off:off+valLen])
	off += int(valLen)

	// read encPKVal
	pkValLen := int(binary.BigEndian.Uint32(enc[off:]))
	off += encLenLen

	if len(enc)-off != pkValLen {
		return 0, 0, 0, nil, nil, ErrCorruptedData
	}

	encPKVal = make([]byte, encLenLen+pkValLen)
	binary.BigEndian.PutUint32(encPKVal, uint32(pkValLen))
	copy(encPKVal[encLenLen:], enc[off:])
	off += len(encPKVal)

	return
}

func existKey(key []byte, st *store.ImmuStore) (bool, error) {
	_, _, _, err := st.Get([]byte(key))
	if err == nil {
		return true, nil
	}
	if err != store.ErrKeyNotFound {
		return false, err
	}
	return false, nil
}

func (e *Engine) mapKey(mappingPrefix string, encValues ...[]byte) []byte {
	mkeyLen := len(e.prefix) + len(mappingPrefix)

	for _, ev := range encValues {
		mkeyLen += len(ev)
	}

	mkey := make([]byte, mkeyLen)

	off := 0

	copy(mkey, e.prefix)
	off += len(e.prefix)

	copy(mkey[off:], []byte(mappingPrefix))
	off += len(mappingPrefix)

	for _, ev := range encValues {
		copy(mkey[off:], ev)
		off += len(ev)
	}

	return mkey
}

func encodeID(id uint64) []byte {
	var encID [encIDLen]byte
	binary.BigEndian.PutUint64(encID[:], id)
	return encID[:]
}

func maxKeyVal(colType SQLValueType) []byte {
	switch colType {
	case IntegerType:
		{
			return mKeyVal[:encIDLen]
		}
	}
	return mKeyVal[:]
}

func encodeValue(val Value, colType SQLValueType, asKey bool) ([]byte, error) {
	switch colType {
	case StringType:
		{
			strVal, ok := val.(*String)
			if !ok {
				return nil, ErrInvalidValue
			}

			if asKey && len(strVal.val) > len(maxKeyVal(StringType)) {
				return nil, ErrInvalidPK
			}

			// len(v) + v
			encv := make([]byte, encLenLen+len(strVal.val))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal.val)))
			copy(encv[encLenLen:], []byte(strVal.val))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := val.(*Number)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [encLenLen + encIDLen]byte
			binary.BigEndian.PutUint32(encv[:], uint32(encIDLen))
			binary.BigEndian.PutUint64(encv[encLenLen:], intVal.val)

			return encv[:], nil
		}
	case BooleanType:
		{
			boolVal, ok := val.(*Bool)
			if !ok {
				return nil, ErrInvalidValue
			}

			// len(v) + v
			var encv [encLenLen + 1]byte
			binary.BigEndian.PutUint32(encv[:], uint32(1))
			if boolVal.val {
				encv[encLenLen] = 1
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
			encv := make([]byte, encLenLen+len(blobVal.val))
			binary.BigEndian.PutUint32(encv[:], uint32(len(blobVal.val)))
			copy(encv[encLenLen:], blobVal.val)

			return encv[:], nil
		}
	}

	/*
		time
	*/

	return nil, ErrInvalidValue
}

func decodeValue(b []byte, colType SQLValueType) (Value, int, error) {
	if len(b) < encLenLen {
		return nil, 0, ErrCorruptedData
	}

	voff := 0

	vlen := int(binary.BigEndian.Uint32(b[voff:]))
	voff += encLenLen

	if len(b) < vlen {
		return nil, voff, ErrCorruptedData
	}

	switch colType {
	case StringType:
		{
			v := string(b[voff : voff+vlen])
			voff += vlen
			return &String{val: v}, voff, nil
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
			voff += vlen
			return &Bool{val: v}, voff, nil
		}
	case BLOBType:
		{
			v := b[voff : voff+vlen]
			voff += vlen
			return &Blob{val: v}, voff, nil
		}
	}

	return nil, voff, ErrCorruptedData
}

func (e *Engine) Catalog() *Catalog {
	return e.catalog
}

// exist database directly on catalogStore: // existKey(e.mapKey(catalogDatabase, db), e.catalogStore)
func (e *Engine) QueryStmt(sql string) (RowReader, error) {
	return e.Query(strings.NewReader(sql))
}

func (e *Engine) Query(sql io.ByteReader) (RowReader, error) {
	if e.catalog == nil {
		err := e.loadCatalog()
		if err != nil {
			return nil, err
		}
	}

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

	snap, err := e.dataStore.SnapshotSince(math.MaxUint64)
	if err != nil {
		return nil, err
	}

	return stmt.Resolve(e, snap, nil)
}

func (e *Engine) ExecStmt(sql string) (*store.TxMetadata, error) {
	return e.Exec(strings.NewReader(sql))
}

func (e *Engine) Exec(sql io.ByteReader) (*store.TxMetadata, error) {
	if e.catalog == nil {
		err := e.loadCatalog()
		if err != nil {
			return nil, err
		}
	}

	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}

	if includesDDL(stmts) {
		e.catalogRWMux.Lock()
		defer e.catalogRWMux.Unlock()
	} else {
		e.catalogRWMux.RLock()
		defer e.catalogRWMux.RUnlock()
	}

	for _, stmt := range stmts {
		centries, dentries, err := stmt.CompileUsing(e)
		if err != nil {
			return nil, err
		}

		if len(centries) > 0 && len(dentries) > 0 {
			return nil, ErrDDLorDMLTxOnly
		}

		if len(centries) > 0 {
			txmd, err := e.catalogStore.Commit(centries)
			if err != nil {
				return nil, e.loadCatalog()
			}

			return txmd, nil
		}

		if len(dentries) > 0 {
			return e.dataStore.Commit(dentries)
		}
	}

	return nil, nil
}

func includesDDL(stmts []SQLStmt) bool {
	for _, stmt := range stmts {
		if stmt.isDDL() {
			return true
		}
	}
	return false
}
