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

package sql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

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

	// prefixes
	databasePrefix string
	tablePrefix    string
	columnPrefix   string
	indexPrefix    string

	currentDatabase string

	multidbHandler MultiDBHandler

	mutex sync.RWMutex
}

type MultiDBHandler interface {
	ListDatabases(ctx context.Context) ([]string, error)
	CreateDatabase(ctx context.Context, db string, ifNotExists bool) error
	UseDatabase(ctx context.Context, db string) error
	ExecPreparedStmts(ctx context.Context, opts *TxOptions, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error)
}

func NewEngine(store *store.ImmuStore, opts *Options) (*Engine, error) {
	if store == nil {
		return nil, ErrIllegalArguments
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	e := &Engine{
		store:         store,
		prefix:        make([]byte, len(opts.prefix)),
		distinctLimit: opts.distinctLimit,
		autocommit:    opts.autocommit,

		databasePrefix: opts.databasePrefix,
		tablePrefix:    opts.tablePrefix,
		columnPrefix:   opts.columnPrefix,
		indexPrefix:    opts.indexPrefix,
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

func (e *Engine) SetCurrentDatabase(ctx context.Context, dbName string) error {
	tx, err := e.NewTx(ctx, DefaultTxOptions().WithReadOnly(true))
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

func (e *Engine) NewTx(ctx context.Context, opts *TxOptions) (*SQLTx, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	var mode store.TxMode
	if opts.ReadOnly {
		mode = store.ReadOnlyTx
	} else {
		mode = store.ReadWriteTx
	}

	txOpts := &store.TxOptions{
		Mode:                    mode,
		SnapshotMustIncludeTxID: opts.SnapshotMustIncludeTxID,
		SnapshotRenewalPeriod:   opts.SnapshotRenewalPeriod,
	}

	e.mutex.RLock()
	defer e.mutex.RUnlock()

	tx, err := e.store.NewTx(ctx, txOpts)
	if err != nil {
		return nil, err
	}

	catalog := NewCatalog(e.databasePrefix, e.tablePrefix, e.columnPrefix, e.indexPrefix)

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
		opts:             opts,
		tx:               tx,
		catalog:          catalog,
		currentDB:        currentDB,
		lastInsertedPKs:  make(map[string]int64),
		firstInsertedPKs: make(map[string]int64),
	}, nil
}

func (e *Engine) Exec(ctx context.Context, tx *SQLTx, sql string, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	stmts, err := Parse(strings.NewReader(sql))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrParsingError, err)
	}

	return e.ExecPreparedStmts(ctx, tx, stmts, params)
}

func (e *Engine) ExecPreparedStmts(ctx context.Context, tx *SQLTx, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error) {
	ntx, ctxs, pendingStmts, err := e.execPreparedStmts(ctx, tx, stmts, params)
	if err != nil {
		return ntx, ctxs, err
	}

	if len(pendingStmts) > 0 {
		// a different database was selected

		if e.multidbHandler == nil || ntx != nil {
			return ntx, ctxs, fmt.Errorf("%w: all statements should have been executed when not using a multidbHandler", ErrUnexpected)
		}

		var opts *TxOptions

		if tx != nil {
			opts = tx.opts
		} else {
			opts = DefaultTxOptions()
		}

		ntx, hctxs, err := e.multidbHandler.ExecPreparedStmts(ctx, opts, pendingStmts, params)
		return ntx, append(ctxs, hctxs...), err
	}

	return ntx, ctxs, nil
}

func (e *Engine) execPreparedStmts(ctx context.Context, tx *SQLTx, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, pendingStmts []SQLStmt, err error) {
	if len(stmts) == 0 {
		return nil, nil, stmts, ErrIllegalArguments
	}

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
		if isDBSelectionStmt && (currTx != nil && !currTx.Closed()) && !currTx.IsExplicitCloseRequired() {
			err = currTx.Commit(ctx)
			if err == nil {
				committedTxs = append(committedTxs, currTx)
			}
			if err != nil {
				return nil, committedTxs, stmts[execStmts:], err
			}
		}

		if currTx == nil || currTx.Closed() {
			var opts *TxOptions

			if currTx != nil {
				opts = currTx.opts
			} else if tx != nil {
				opts = tx.opts
			} else {
				opts = DefaultTxOptions()
			}

			// begin tx with implicit commit
			currTx, err = e.NewTx(ctx, opts)
			if err != nil {
				return nil, committedTxs, stmts[execStmts:], err
			}
		}

		ntx, err := stmt.execAt(ctx, currTx, nparams)
		if err != nil {
			currTx.Cancel()
			return nil, committedTxs, stmts[execStmts:], err
		}

		if !currTx.Closed() && !currTx.IsExplicitCloseRequired() && e.autocommit {
			err = currTx.Commit(ctx)
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

	if currTx != nil && !currTx.Closed() && !currTx.IsExplicitCloseRequired() {
		err = currTx.Commit(ctx)
		if err != nil {
			return nil, committedTxs, stmts[execStmts:], err
		}

		committedTxs = append(committedTxs, currTx)
	}

	if currTx != nil && currTx.Closed() {
		currTx = nil
	}

	return currTx, committedTxs, stmts[execStmts:], nil
}

func (e *Engine) Query(ctx context.Context, tx *SQLTx, sql string, params map[string]interface{}) (RowReader, error) {
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

	return e.QueryPreparedStmt(ctx, tx, stmt, params)
}

func (e *Engine) QueryPreparedStmt(ctx context.Context, tx *SQLTx, stmt DataSource, params map[string]interface{}) (rowReader RowReader, err error) {
	if stmt == nil {
		return nil, ErrIllegalArguments
	}

	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(ctx, DefaultTxOptions().WithReadOnly(true))
		if err != nil {
			return nil, err
		}
		defer func() {
			if err != nil {
				qtx.Cancel()
			}
		}()
	}

	nparams, err := normalizeParams(params)
	if err != nil {
		return nil, err
	}

	_, err = stmt.execAt(ctx, qtx, nparams)
	if err != nil {
		return nil, err
	}

	r, err := stmt.Resolve(ctx, qtx, nparams, nil)
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

func (e *Engine) Catalog(ctx context.Context, tx *SQLTx) (catalog *Catalog, err error) {
	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(ctx, DefaultTxOptions().WithReadOnly(true))
		if err != nil {
			return nil, err
		}
		defer qtx.Cancel()
	}

	return qtx.Catalog(), nil
}

func (e *Engine) InferParameters(ctx context.Context, tx *SQLTx, sql string) (params map[string]SQLValueType, err error) {
	stmts, err := Parse(strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParsingError, err)
	}

	return e.InferParametersPreparedStmts(ctx, tx, stmts)
}

func (e *Engine) InferParametersPreparedStmts(ctx context.Context, tx *SQLTx, stmts []SQLStmt) (params map[string]SQLValueType, err error) {
	if len(stmts) == 0 {
		return nil, ErrIllegalArguments
	}

	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(ctx, DefaultTxOptions().WithReadOnly(true))
		if err != nil {
			return nil, err
		}
		defer qtx.Cancel()
	}

	params = make(map[string]SQLValueType)

	for _, stmt := range stmts {
		err = stmt.inferParameters(ctx, qtx, params)
		if err != nil {
			return nil, err
		}
	}

	return params, nil
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

// CopyCatalogToTx copies the current sql catalog to the ongoing transaction.
func (e *Engine) CopyCatalogToTx(ctx context.Context, tx *store.OngoingTx) error {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	catalog := NewCatalog(e.databasePrefix, e.tablePrefix, e.columnPrefix, e.indexPrefix)
	err := catalog.addSchemaToTx(e.prefix, tx)
	if err != nil {
		return err
	}

	return nil
}

// addSchemaToTx adds the schema to the ongoing transaction.
func (t *Table) addIndexesToTx(sqlPrefix []byte, tx *store.OngoingTx, indexPrefix string) error {
	initialKey := mapKey(sqlPrefix, indexPrefix, EncodeID(t.db.id), EncodeID(t.id))

	idxReaderSpec := store.KeyReaderSpec{
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

		dbID, tableID, _, err := unmapIndex(sqlPrefix, mkey, indexPrefix)
		if err != nil {
			return err
		}

		if t.id != tableID || t.db.id != dbID {
			return ErrCorruptedData
		}

		v, err := vref.Resolve()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		err = tx.Set(mkey, nil, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// addSchemaToTx adds the schema of the catalog to the given transaction.
func (d *Database) addTablesToTx(sqlPrefix []byte, tx *store.OngoingTx) error {
	dbReaderSpec := store.KeyReaderSpec{
		Prefix:  mapKey(sqlPrefix, d.tablePrefix, EncodeID(d.id)),
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

		dbID, tableID, err := unmapTableID(sqlPrefix, mkey, d.tablePrefix)
		if err != nil {
			return err
		}

		if dbID != d.id {
			return ErrCorruptedData
		}

		// read col specs into tx
		colSpecs, err := addColSpecsToTx(d.id, tableID, tx, sqlPrefix, d.columnPrefix)
		if err != nil {
			return err
		}

		v, err := vref.Resolve()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		err = tx.Set(mkey, nil, v)
		if err != nil {
			return err
		}

		table, err := d.newTable(string(v), colSpecs)
		if err != nil {
			return err
		}

		if tableID != table.id {
			return ErrCorruptedData
		}

		// read index specs into tx
		err = table.addIndexesToTx(sqlPrefix, tx, d.indexPrefix)
		if err != nil {
			return err
		}

	}

	return nil
}

// addSchemaToTx adds the schema of the catalog to the given transaction.
func (c *Catalog) addSchemaToTx(sqlPrefix []byte, tx *store.OngoingTx) error {
	dbReaderSpec := store.KeyReaderSpec{
		Prefix:  mapKey(sqlPrefix, c.databasePrefix),
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

		id, err := unmapDatabaseID(sqlPrefix, mkey, c.databasePrefix)
		if err != nil {
			return err
		}

		v, err := vref.Resolve()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		err = tx.Set(mkey, nil, v)
		if err != nil {
			return err
		}

		db, err := c.newDatabase(id, string(v))
		if err != nil {
			return err
		}

		// read tables and indexes into tx
		err = db.addTablesToTx(sqlPrefix, tx)
		if err != nil {
			return err
		}

	}

	return nil
}

// addColSpecsToTx adds the column specs of the given table to the given transaction.
func addColSpecsToTx(dbID, tableID uint32, tx *store.OngoingTx, sqlPrefix []byte, columnPrefix string) (specs []*ColSpec, err error) {
	initialKey := mapKey(sqlPrefix, columnPrefix, EncodeID(dbID), EncodeID(tableID))

	dbReaderSpec := store.KeyReaderSpec{
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

		mdbID, mtableID, colID, colType, err := unmapColSpec(sqlPrefix, mkey, columnPrefix)
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

		err = tx.Set(mkey, nil, v)
		if err != nil {
			return nil, err
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
