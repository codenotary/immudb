/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

var ErrNoSupported = errors.New("not supported")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrParsingError = errors.New("parsing error")
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrNoDatabaseSelected = errors.New("no database selected")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrColumnDoesNotExist = errors.New("column does not exist")
var ErrColumnAlreadyExists = errors.New("column already exists")
var ErrSameOldAndNewColumnName = errors.New("same old and new column names")
var ErrColumnNotIndexed = errors.New("column is not indexed")
var ErrFunctionDoesNotExist = errors.New("function does not exist")
var ErrLimitedKeyType = errors.New("indexed key of invalid type. Supported types are: INTEGER, VARCHAR[256] OR BLOB[256]")
var ErrLimitedAutoIncrement = errors.New("only INTEGER single-column primary keys can be set as auto incremental")
var ErrLimitedMaxLen = errors.New("only VARCHAR and BLOB types support max length")
var ErrDuplicatedColumn = errors.New("duplicated column")
var ErrInvalidColumn = errors.New("invalid column")
var ErrPKCanNotBeNull = errors.New("primary key can not be null")
var ErrPKCanNotBeUpdated = errors.New("primary key can not be updated")
var ErrNotNullableColumnCannotBeNull = errors.New("not nullable column can not be null")
var ErrNewColumnMustBeNullable = errors.New("new column must be nullable")
var ErrIndexAlreadyExists = errors.New("index already exists")
var ErrMaxNumberOfColumnsInIndexExceeded = errors.New("number of columns in multi-column index exceeded")
var ErrNoAvailableIndex = errors.New("no available index")
var ErrInvalidNumberOfValues = errors.New("invalid number of values provided")
var ErrInvalidValue = errors.New("invalid value provided")
var ErrInferredMultipleTypes = errors.New("inferred multiple types")
var ErrExpectingDQLStmt = errors.New("illegal statement. DQL statement expected")
var ErrLimitedOrderBy = errors.New("order is limit to one indexed column")
var ErrLimitedGroupBy = errors.New("group by requires ordering by the grouping column")
var ErrIllegalMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrNoMoreRows = store.ErrNoMoreEntries
var ErrInvalidTypes = errors.New("invalid types")
var ErrUnsupportedJoinType = errors.New("unsupported join type")
var ErrInvalidCondition = errors.New("invalid condition")
var ErrHavingClauseRequiresGroupClause = errors.New("having clause requires group clause")
var ErrNotComparableValues = errors.New("values are not comparable")
var ErrUnexpected = errors.New("unexpected error")
var ErrMaxKeyLengthExceeded = errors.New("max key length exceeded")
var ErrMaxLengthExceeded = errors.New("max length exceeded")
var ErrColumnIsNotAnAggregation = errors.New("column is not an aggregation")
var ErrLimitedCount = errors.New("only unbounded counting is supported i.e. COUNT(*)")
var ErrTxDoesNotExist = errors.New("tx does not exist")
var ErrNestedTxNotSupported = errors.New("nested tx are not supported")
var ErrNoOngoingTx = errors.New("no ongoing transaction")
var ErrNonTransactionalStmt = errors.New("non transactional statement")
var ErrDivisionByZero = errors.New("division by zero")
var ErrMissingParameter = errors.New("missing parameter")
var ErrUnsupportedParameter = errors.New("unsupported parameter")
var ErrDuplicatedParameters = errors.New("duplicated parameters")
var ErrLimitedIndexCreation = errors.New("index creation is only supported on empty tables")
var ErrTooManyRows = errors.New("too many rows")
var ErrAlreadyClosed = store.ErrAlreadyClosed
var ErrAmbiguousSelector = errors.New("ambiguous selector")
var ErrUnsupportedCast = errors.New("unsupported cast")
var ErrColumnMismatchInUnionStmt = errors.New("column mismatch in union statement")

var maxKeyLen = 256

const EncIDLen = 4
const EncLenLen = 4

const MaxNumberOfColumnsInIndex = 8

type Engine struct {
	store *store.ImmuStore

	prefix        []byte
	distinctLimit int
	autocommit    bool

	currentDatabase string

	multidbHandler MultiDBHandler

	mutex sync.RWMutex
}

type MultiDBHandler interface {
	ListDatabases(ctx context.Context) ([]string, error)
	CreateDatabase(ctx context.Context, db string, ifNotExists bool) error
	UseDatabase(ctx context.Context, db string) error
	ExecPreparedStmts(ctx context.Context, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error)
}

//SQLTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW
type SQLTx struct {
	engine *Engine

	ctx context.Context

	tx *store.OngoingTx

	currentDB *Database
	catalog   *Catalog // in-mem catalog

	explicitClose bool

	updatedRows      int
	lastInsertedPKs  map[string]int64 // last inserted PK by table name
	firstInsertedPKs map[string]int64 // first inserted PK by table name

	txHeader *store.TxHeader // header is set once tx is committed

	committed bool
	closed    bool
}

func NewEngine(store *store.ImmuStore, opts *Options) (*Engine, error) {
	if store == nil || !ValidOpts(opts) {
		return nil, ErrIllegalArguments
	}

	e := &Engine{
		store:         store,
		prefix:        make([]byte, len(opts.prefix)),
		distinctLimit: opts.distinctLimit,
		autocommit:    opts.autocommit,
	}

	copy(e.prefix, opts.prefix)

	// TODO: find a better way to handle parsing errors
	yyErrorVerbose = true

	return e, nil
}

func (e *Engine) SetMultiDBHandler(handler MultiDBHandler) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.multidbHandler = handler
}

func (e *Engine) SetCurrentDatabase(dbName string) error {
	tx, err := e.NewTx(context.Background())
	if err != nil {
		return err
	}
	defer tx.Cancel()

	db, exists := tx.catalog.dbsByName[dbName]
	if !exists {
		return ErrDatabaseDoesNotExist
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.currentDatabase = db.name

	return nil
}

func (e *Engine) CurrentDatabase() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.currentDatabase
}

func (e *Engine) NewTx(ctx context.Context) (*SQLTx, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	tx, err := e.store.NewTx()
	if err != nil {
		return nil, err
	}

	catalog := newCatalog()

	err = catalog.load(e.prefix, tx)
	if err != nil {
		return nil, err
	}

	var currentDB *Database

	if e.currentDatabase != "" {
		db, exists := catalog.dbsByName[e.currentDatabase]
		if !exists {
			return nil, ErrDatabaseDoesNotExist
		}

		currentDB = db
	}

	return &SQLTx{
		engine:           e,
		ctx:              ctx,
		tx:               tx,
		catalog:          catalog,
		currentDB:        currentDB,
		lastInsertedPKs:  make(map[string]int64),
		firstInsertedPKs: make(map[string]int64),
	}, nil
}

func (sqlTx *SQLTx) useDatabase(dbName string) error {
	db, err := sqlTx.catalog.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	sqlTx.currentDB = db

	return nil
}

func (sqlTx *SQLTx) Database() *Database {
	return sqlTx.currentDB
}

func (sqlTx *SQLTx) UpdatedRows() int {
	return sqlTx.updatedRows
}

func (sqlTx *SQLTx) LastInsertedPKs() map[string]int64 {
	return sqlTx.lastInsertedPKs
}

func (sqlTx *SQLTx) FirstInsertedPKs() map[string]int64 {
	return sqlTx.firstInsertedPKs
}

func (sqlTx *SQLTx) TxHeader() *store.TxHeader {
	return sqlTx.txHeader
}

func (sqlTx *SQLTx) sqlPrefix() []byte {
	return sqlTx.engine.prefix
}

func (sqlTx *SQLTx) distinctLimit() int {
	return sqlTx.engine.distinctLimit
}

func (sqlTx *SQLTx) newKeyReader(rSpec *store.KeyReaderSpec) (*store.KeyReader, error) {
	return sqlTx.tx.NewKeyReader(rSpec)
}

func (sqlTx *SQLTx) get(key []byte) (store.ValueRef, error) {
	return sqlTx.tx.Get(key)
}

func (sqlTx *SQLTx) set(key []byte, metadata *store.KVMetadata, value []byte) error {
	return sqlTx.tx.Set(key, metadata, value)
}

func (sqlTx *SQLTx) existKeyWith(prefix, neq []byte) (bool, error) {
	return sqlTx.tx.ExistKeyWith(prefix, neq)
}

func (sqlTx *SQLTx) Cancel() error {
	if sqlTx.closed {
		return ErrAlreadyClosed
	}

	sqlTx.closed = true

	return sqlTx.tx.Cancel()
}

func (sqlTx *SQLTx) commit() error {
	if sqlTx.closed {
		return ErrAlreadyClosed
	}

	sqlTx.committed = true
	sqlTx.closed = true

	hdr, err := sqlTx.tx.Commit()
	if err != nil && err != store.ErrorNoEntriesProvided {
		return err
	}

	sqlTx.txHeader = hdr

	return nil
}

func (sqlTx *SQLTx) Closed() bool {
	return sqlTx.closed
}

func (c *Catalog) load(sqlPrefix []byte, tx *store.OngoingTx) error {
	dbReaderSpec := &store.KeyReaderSpec{
		Prefix:  mapKey(sqlPrefix, catalogDatabasePrefix),
		Filters: []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
	}

	dbReader, err := tx.NewKeyReader(dbReaderSpec)
	if err != nil {
		return err
	}
	defer dbReader.Close()

	for {
		mkey, vref, err := dbReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		id, err := unmapDatabaseID(sqlPrefix, mkey)
		if err != nil {
			return err
		}

		v, err := vref.Resolve()
		if err != nil {
			return err
		}

		db, err := c.newDatabase(id, string(v))
		if err != nil {
			return err
		}

		err = db.loadTables(sqlPrefix, tx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) loadTables(sqlPrefix []byte, tx *store.OngoingTx) error {
	dbReaderSpec := &store.KeyReaderSpec{
		Prefix:  mapKey(sqlPrefix, catalogTablePrefix, EncodeID(db.id)),
		Filters: []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
	}

	tableReader, err := tx.NewKeyReader(dbReaderSpec)
	if err != nil {
		return err
	}
	defer tableReader.Close()

	for {
		mkey, vref, err := tableReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		dbID, tableID, err := unmapTableID(sqlPrefix, mkey)
		if err != nil {
			return err
		}

		if dbID != db.id {
			return ErrCorruptedData
		}

		colSpecs, err := loadColSpecs(db.id, tableID, tx, sqlPrefix)
		if err != nil {
			return err
		}

		v, err := vref.Resolve()
		if err != nil {
			return err
		}

		table, err := db.newTable(string(v), colSpecs)
		if err != nil {
			return err
		}

		if tableID != table.id {
			return ErrCorruptedData
		}

		err = table.loadIndexes(sqlPrefix, tx)
		if err != nil {
			return err
		}

		if table.autoIncrementPK {
			encMaxPK, err := loadMaxPK(sqlPrefix, tx, table)
			if err == store.ErrNoMoreEntries {
				continue
			}
			if err != nil {
				return err
			}

			if len(encMaxPK) != 9 {
				return ErrCorruptedData
			}

			if encMaxPK[0] != KeyValPrefixNotNull {
				return ErrCorruptedData
			}

			// map to signed integer space
			encMaxPK[1] ^= 0x80

			table.maxPK = int64(binary.BigEndian.Uint64(encMaxPK[1:]))
		}
	}

	return nil
}

func loadMaxPK(sqlPrefix []byte, tx *store.OngoingTx, table *Table) ([]byte, error) {
	pkReaderSpec := &store.KeyReaderSpec{
		Prefix:    mapKey(sqlPrefix, PIndexPrefix, EncodeID(table.db.id), EncodeID(table.id), EncodeID(PKIndexID)),
		DescOrder: true,
	}

	pkReader, err := tx.NewKeyReader(pkReaderSpec)
	if err != nil {
		return nil, err
	}
	defer pkReader.Close()

	mkey, _, err := pkReader.Read()
	if err != nil {
		return nil, err
	}

	return unmapIndexEntry(table.primaryIndex, sqlPrefix, mkey)
}

func loadColSpecs(dbID, tableID uint32, tx *store.OngoingTx, sqlPrefix []byte) (specs []*ColSpec, err error) {
	initialKey := mapKey(sqlPrefix, catalogColumnPrefix, EncodeID(dbID), EncodeID(tableID))

	dbReaderSpec := &store.KeyReaderSpec{
		Prefix:  initialKey,
		Filters: []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
	}

	colSpecReader, err := tx.NewKeyReader(dbReaderSpec)
	if err != nil {
		return nil, err
	}
	defer colSpecReader.Close()

	specs = make([]*ColSpec, 0)

	for {
		mkey, vref, err := colSpecReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		mdbID, mtableID, colID, colType, err := unmapColSpec(sqlPrefix, mkey)
		if err != nil {
			return nil, err
		}

		if dbID != mdbID || tableID != mtableID {
			return nil, ErrCorruptedData
		}

		v, err := vref.Resolve()
		if err != nil {
			return nil, err
		}
		if len(v) < 6 {
			return nil, ErrCorruptedData
		}

		spec := &ColSpec{
			colName:       string(v[5:]),
			colType:       colType,
			maxLen:        int(binary.BigEndian.Uint32(v[1:])),
			autoIncrement: v[0]&autoIncrementFlag != 0,
			notNull:       v[0]&nullableFlag != 0,
		}

		specs = append(specs, spec)

		if int(colID) != len(specs) {
			return nil, ErrCorruptedData
		}
	}

	return
}

func (table *Table) loadIndexes(sqlPrefix []byte, tx *store.OngoingTx) error {
	initialKey := mapKey(sqlPrefix, catalogIndexPrefix, EncodeID(table.db.id), EncodeID(table.id))

	idxReaderSpec := &store.KeyReaderSpec{
		Prefix:  initialKey,
		Filters: []store.FilterFn{store.IgnoreExpired, store.IgnoreDeleted},
	}

	idxSpecReader, err := tx.NewKeyReader(idxReaderSpec)
	if err != nil {
		return err
	}
	defer idxSpecReader.Close()

	for {
		mkey, vref, err := idxSpecReader.Read()
		if err == store.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		dbID, tableID, indexID, err := unmapIndex(sqlPrefix, mkey)
		if err != nil {
			return err
		}

		if table.id != tableID || table.db.id != dbID {
			return ErrCorruptedData
		}

		v, err := vref.Resolve()
		if err != nil {
			return err
		}

		// v={unique {colID1}(ASC|DESC)...{colIDN}(ASC|DESC)}
		colSpecLen := EncIDLen + 1

		if len(v) < 1+colSpecLen || len(v)%colSpecLen != 1 {
			return ErrCorruptedData
		}

		var colIDs []uint32

		for i := 1; i < len(v); i += colSpecLen {
			colID := binary.BigEndian.Uint32(v[i:])

			// TODO: currently only ASC order is supported
			if v[i+EncIDLen] != 0 {
				return ErrCorruptedData
			}

			colIDs = append(colIDs, colID)
		}

		index, err := table.newIndex(v[0] > 0, colIDs)
		if err != nil {
			return err
		}

		if indexID != index.id {
			return ErrCorruptedData
		}
	}

	return nil
}

func trimPrefix(prefix, mkey []byte, mappingPrefix []byte) ([]byte, error) {
	if len(prefix)+len(mappingPrefix) > len(mkey) ||
		!bytes.Equal(prefix, mkey[:len(prefix)]) ||
		!bytes.Equal(mappingPrefix, mkey[len(prefix):len(prefix)+len(mappingPrefix)]) {
		return nil, ErrIllegalMappedKey
	}

	return mkey[len(prefix)+len(mappingPrefix):], nil
}

func unmapDatabaseID(prefix, mkey []byte) (dbID uint32, err error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogDatabasePrefix))
	if err != nil {
		return 0, err
	}

	if len(encID) != EncIDLen {
		return 0, ErrCorruptedData
	}

	return binary.BigEndian.Uint32(encID), nil
}

func unmapTableID(prefix, mkey []byte) (dbID, tableID uint32, err error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogTablePrefix))
	if err != nil {
		return 0, 0, err
	}

	if len(encID) != EncIDLen*2 {
		return 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])

	return
}

func unmapColSpec(prefix, mkey []byte) (dbID, tableID, colID uint32, colType SQLValueType, err error) {
	encID, err := trimPrefix(prefix, mkey, []byte(catalogColumnPrefix))
	if err != nil {
		return 0, 0, 0, "", err
	}

	if len(encID) < EncIDLen*3 {
		return 0, 0, 0, "", ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])
	colID = binary.BigEndian.Uint32(encID[2*EncIDLen:])

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

func unmapIndex(sqlPrefix, mkey []byte) (dbID, tableID, indexID uint32, err error) {
	encID, err := trimPrefix(sqlPrefix, mkey, []byte(catalogIndexPrefix))
	if err != nil {
		return 0, 0, 0, err
	}

	if len(encID) != EncIDLen*3 {
		return 0, 0, 0, ErrCorruptedData
	}

	dbID = binary.BigEndian.Uint32(encID)
	tableID = binary.BigEndian.Uint32(encID[EncIDLen:])
	indexID = binary.BigEndian.Uint32(encID[EncIDLen*2:])

	return
}

func unmapIndexEntry(index *Index, sqlPrefix, mkey []byte) (encPKVals []byte, err error) {
	if index == nil {
		return nil, ErrIllegalArguments
	}

	enc, err := trimPrefix(sqlPrefix, mkey, []byte(index.prefix()))
	if err != nil {
		return nil, ErrCorruptedData
	}

	if len(enc) <= EncIDLen*3 {
		return nil, ErrCorruptedData
	}

	off := 0

	dbID := binary.BigEndian.Uint32(enc[off:])
	off += EncIDLen

	tableID := binary.BigEndian.Uint32(enc[off:])
	off += EncIDLen

	indexID := binary.BigEndian.Uint32(enc[off:])
	off += EncIDLen

	if dbID != index.table.db.id || tableID != index.table.id || indexID != index.id {
		return nil, ErrCorruptedData
	}

	if !index.IsPrimary() {
		//read index values
		for _, col := range index.cols {
			if enc[off] == KeyValPrefixNull {
				off += 1
				continue
			}
			if enc[off] != KeyValPrefixNotNull {
				return nil, ErrCorruptedData
			}
			off += 1

			maxLen := col.MaxLen()
			if variableSized(col.colType) {
				maxLen += EncLenLen
			}
			if len(enc)-off < maxLen {
				return nil, ErrCorruptedData
			}

			off += maxLen
		}
	}

	//PK cannot be nil
	if len(enc)-off < 1 {
		return nil, ErrCorruptedData
	}

	return enc[off:], nil
}

func variableSized(sqlType SQLValueType) bool {
	return sqlType == VarcharType || sqlType == BLOBType
}

func mapKey(prefix []byte, mappingPrefix string, encValues ...[]byte) []byte {
	return MapKey(prefix, mappingPrefix, encValues...)
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

func EncodeID(id uint32) []byte {
	var encID [EncIDLen]byte
	binary.BigEndian.PutUint32(encID[:], id)
	return encID[:]
}

func EncodeValue(val interface{}, colType SQLValueType, maxLen int) ([]byte, error) {
	switch colType {
	case VarcharType:
		{
			strVal, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a string: %w", ErrInvalidValue,
				)
			}

			if maxLen > 0 && len(strVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(strVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(strVal)))
			copy(encv[EncLenLen:], []byte(strVal))

			return encv, nil
		}
	case IntegerType:
		{
			intVal, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf(
					"value is not an integer: %w", ErrInvalidValue,
				)
			}

			// map to unsigned integer space
			// len(v) + v
			var encv [EncLenLen + 8]byte
			binary.BigEndian.PutUint32(encv[:], uint32(8))
			binary.BigEndian.PutUint64(encv[EncLenLen:], uint64(intVal))

			return encv[:], nil
		}
	case BooleanType:
		{
			boolVal, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a boolean: %w", ErrInvalidValue,
				)
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
				v, ok := val.([]byte)
				if !ok {
					return nil, fmt.Errorf(
						"value is not a blob: %w", ErrInvalidValue,
					)
				}
				blobVal = v
			}

			if maxLen > 0 && len(blobVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// len(v) + v
			encv := make([]byte, EncLenLen+len(blobVal))
			binary.BigEndian.PutUint32(encv[:], uint32(len(blobVal)))
			copy(encv[EncLenLen:], blobVal)

			return encv[:], nil
		}
	case TimestampType:
		{
			timeVal, ok := val.(time.Time)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a timestamp: %w", ErrInvalidValue,
				)
			}

			// len(v) + v
			var encv [EncLenLen + 8]byte
			binary.BigEndian.PutUint32(encv[:], uint32(8))
			binary.BigEndian.PutUint64(encv[EncLenLen:], uint64(TimeToInt64(timeVal)))

			return encv[:], nil
		}
	}

	return nil, ErrInvalidValue
}

const (
	KeyValPrefixNull       byte = 0x20
	KeyValPrefixNotNull    byte = 0x80
	KeyValPrefixUpperBound byte = 0xFF
)

func EncodeAsKey(val interface{}, colType SQLValueType, maxLen int) ([]byte, error) {
	if maxLen <= 0 {
		return nil, ErrInvalidValue
	}
	if maxLen > maxKeyLen {
		return nil, ErrMaxKeyLengthExceeded
	}

	if val == nil {
		return []byte{KeyValPrefixNull}, nil
	}

	switch colType {
	case VarcharType:
		{
			strVal, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a string: %w", ErrInvalidValue,
				)
			}

			if len(strVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// notnull + value + padding + len(value)
			encv := make([]byte, 1+maxLen+EncLenLen)
			encv[0] = KeyValPrefixNotNull
			copy(encv[1:], []byte(strVal))
			binary.BigEndian.PutUint32(encv[len(encv)-EncLenLen:], uint32(len(strVal)))

			return encv, nil
		}
	case IntegerType:
		{
			if maxLen != 8 {
				return nil, ErrCorruptedData
			}

			intVal, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf(
					"value is not an integer: %w", ErrInvalidValue,
				)
			}

			// v
			var encv [9]byte
			encv[0] = KeyValPrefixNotNull
			binary.BigEndian.PutUint64(encv[1:], uint64(intVal))
			// map to unsigned integer space for lexical sorting order
			encv[1] ^= 0x80

			return encv[:], nil
		}
	case BooleanType:
		{
			if maxLen != 1 {
				return nil, ErrCorruptedData
			}

			boolVal, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a boolean: %w", ErrInvalidValue,
				)
			}

			// v
			var encv [2]byte
			encv[0] = KeyValPrefixNotNull
			if boolVal {
				encv[1] = 1
			}

			return encv[:], nil
		}
	case BLOBType:
		{
			blobVal, ok := val.([]byte)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a blob: %w", ErrInvalidValue,
				)
			}

			if len(blobVal) > maxLen {
				return nil, ErrMaxLengthExceeded
			}

			// notnull + value + padding + len(value)
			encv := make([]byte, 1+maxLen+EncLenLen)
			encv[0] = KeyValPrefixNotNull
			copy(encv[1:], []byte(blobVal))
			binary.BigEndian.PutUint32(encv[len(encv)-EncLenLen:], uint32(len(blobVal)))

			return encv, nil
		}
	case TimestampType:
		{
			if maxLen != 8 {
				return nil, ErrCorruptedData
			}

			timeVal, ok := val.(time.Time)
			if !ok {
				return nil, fmt.Errorf(
					"value is not a timestamp: %w", ErrInvalidValue,
				)
			}

			// v
			var encv [9]byte
			encv[0] = KeyValPrefixNotNull
			binary.BigEndian.PutUint64(encv[1:], uint64(timeVal.UnixNano()))
			// map to unsigned integer space for lexical sorting order
			encv[1] ^= 0x80

			return encv[:], nil
		}

	}

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
			if vlen != 8 {
				return nil, 0, ErrCorruptedData
			}

			v := binary.BigEndian.Uint64(b[voff:])
			voff += vlen

			return &Number{val: int64(v)}, voff, nil
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
	case TimestampType:
		{
			if vlen != 8 {
				return nil, 0, ErrCorruptedData
			}

			v := binary.BigEndian.Uint64(b[voff:])
			voff += vlen

			return &Timestamp{val: TimeFromInt64(int64(v))}, voff, nil
		}
	}

	return nil, 0, ErrCorruptedData
}

func normalizeParams(params map[string]interface{}) (map[string]interface{}, error) {
	nparams := make(map[string]interface{}, len(params))

	for name, value := range params {
		nname := strings.ToLower(name)

		_, exists := nparams[nname]
		if exists {
			return nil, ErrDuplicatedParameters
		}

		nparams[nname] = value
	}

	return nparams, nil
}

func (e *Engine) Exec(sql string, params map[string]interface{}, tx *SQLTx) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	stmts, err := Parse(strings.NewReader(sql))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrParsingError, err)
	}

	return e.ExecPreparedStmts(stmts, params, tx)
}

func (e *Engine) ExecPreparedStmts(stmts []SQLStmt, params map[string]interface{}, tx *SQLTx) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	ntx, ctxs, pendingStmts, err := e.execPreparedStmts(stmts, params, tx)
	if err != nil {
		return ntx, ctxs, err
	}

	if len(pendingStmts) > 0 {
		// a different database was selected

		if e.multidbHandler == nil || ntx != nil {
			return ntx, ctxs, fmt.Errorf("%w: all statements should have been executed when not using a multidbHandler", ErrUnexpected)
		}

		var ctx context.Context

		if tx != nil {
			ctx = tx.ctx
		} else {
			ctx = context.Background()
		}

		ntx, hctxs, err := e.multidbHandler.ExecPreparedStmts(ctx, pendingStmts, params)

		return ntx, append(ctxs, hctxs...), err
	}

	return ntx, ctxs, nil
}

func (e *Engine) execPreparedStmts(stmts []SQLStmt, params map[string]interface{}, tx *SQLTx) (ntx *SQLTx, committedTxs []*SQLTx, pendingStmts []SQLStmt, err error) {
	if len(stmts) == 0 {
		return nil, nil, stmts, ErrIllegalArguments
	}

	// TODO: eval params at once
	nparams, err := normalizeParams(params)
	if err != nil {
		return nil, nil, stmts, err
	}

	currTx := tx

	execStmts := 0

	for _, stmt := range stmts {
		if stmt == nil {
			return nil, nil, stmts[execStmts:], ErrIllegalArguments
		}

		_, isDBSelectionStmt := stmt.(*UseDatabaseStmt)

		// handle the case when working in non-autocommit mode outside a transaction block
		if isDBSelectionStmt && (currTx != nil && !currTx.closed) && !currTx.explicitClose {
			err = currTx.commit()
			if err == nil {
				committedTxs = append(committedTxs, currTx)
			}
			if err != nil {
				return nil, committedTxs, stmts[execStmts:], err
			}
		}

		if currTx == nil || currTx.closed {
			var ctx context.Context

			if currTx != nil {
				ctx = currTx.ctx
			} else if tx != nil {
				ctx = tx.ctx
			} else {
				ctx = context.Background()
			}

			// begin tx with implicit commit
			currTx, err = e.NewTx(ctx)
			if err != nil {
				return nil, committedTxs, stmts[execStmts:], err
			}
		}

		ntx, err := stmt.execAt(currTx, nparams)
		if err != nil {
			currTx.Cancel()
			return nil, committedTxs, stmts[execStmts:], err
		}

		if !currTx.closed && !currTx.explicitClose && e.autocommit {
			err = currTx.commit()
			if err != nil {
				return nil, committedTxs, stmts[execStmts:], err
			}
		}

		if currTx.committed {
			committedTxs = append(committedTxs, currTx)
		}

		currTx = ntx

		execStmts++

		if isDBSelectionStmt && e.multidbHandler != nil {
			break
		}
	}

	if currTx != nil && !currTx.closed && !currTx.explicitClose {
		err = currTx.commit()
		if err != nil {
			return nil, committedTxs, stmts[execStmts:], err
		}

		committedTxs = append(committedTxs, currTx)
	}

	if currTx != nil && currTx.closed {
		currTx = nil
	}

	return currTx, committedTxs, stmts[execStmts:], nil
}

func (e *Engine) Query(sql string, params map[string]interface{}, tx *SQLTx) (RowReader, error) {
	stmts, err := Parse(strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParsingError, err)
	}
	if len(stmts) != 1 {
		return nil, ErrExpectingDQLStmt
	}

	stmt, ok := stmts[0].(DataSource)
	if !ok {
		return nil, ErrExpectingDQLStmt
	}

	return e.QueryPreparedStmt(stmt, params, tx)
}

func (e *Engine) QueryPreparedStmt(stmt DataSource, params map[string]interface{}, tx *SQLTx) (rowReader RowReader, err error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(context.Background())
		if err != nil {
			return nil, err
		}
	}

	// TODO: eval params at once
	nparams, err := normalizeParams(params)
	if err != nil {
		return nil, err
	}

	_, err = stmt.execAt(qtx, nparams)
	if err != nil {
		return nil, err
	}

	r, err := stmt.Resolve(qtx, nparams, nil)
	if err != nil {
		return nil, err
	}

	if tx == nil {
		r.onClose(func() {
			qtx.Cancel()
		})
	}

	return r, nil
}

func (e *Engine) Catalog(tx *SQLTx) (catalog *Catalog, err error) {
	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(context.Background())
		if err != nil {
			return nil, err
		}
		defer qtx.Cancel()
	}

	return qtx.catalog, nil
}

func (e *Engine) InferParameters(sql string, tx *SQLTx) (params map[string]SQLValueType, err error) {
	stmts, err := Parse(strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParsingError, err)
	}

	return e.InferParametersPreparedStmts(stmts, tx)
}

func (e *Engine) InferParametersPreparedStmts(stmts []SQLStmt, tx *SQLTx) (params map[string]SQLValueType, err error) {
	if len(stmts) == 0 {
		return nil, ErrIllegalArguments
	}

	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(context.Background())
		if err != nil {
			return nil, err
		}
		defer qtx.Cancel()
	}

	params = make(map[string]SQLValueType)

	for _, stmt := range stmts {
		err = stmt.inferParameters(qtx, params)
		if err != nil {
			return nil, err
		}
	}

	return params, nil
}
