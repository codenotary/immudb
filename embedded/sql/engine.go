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
	"strings"

	"github.com/codenotary/immudb/embedded/store"
)

var ErrNoSupported = errors.New("not supported")
var ErrIllegalArguments = store.ErrIllegalArguments
var ErrMultiIndexingNotEnabled = fmt.Errorf("%w: multi-indexing must be enabled", store.ErrIllegalState)
var ErrParsingError = errors.New("parsing error")
var ErrDDLorDMLTxOnly = errors.New("transactions can NOT combine DDL and DML statements")
var ErrUnspecifiedMultiDBHandler = fmt.Errorf("%w: unspecified multidbHanlder", store.ErrIllegalState)
var ErrDatabaseDoesNotExist = errors.New("database does not exist")
var ErrDatabaseAlreadyExists = errors.New("database already exists")
var ErrTableAlreadyExists = errors.New("table already exists")
var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrColumnDoesNotExist = errors.New("column does not exist")
var ErrColumnAlreadyExists = errors.New("column already exists")
var ErrCantDropIndexedColumn = errors.New("can not drop indexed column")
var ErrSameOldAndNewNames = errors.New("same old and new names")
var ErrColumnNotIndexed = errors.New("column is not indexed")
var ErrFunctionDoesNotExist = errors.New("function does not exist")
var ErrLimitedKeyType = errors.New("indexed key of unsupported type or exceeded length")
var ErrLimitedAutoIncrement = errors.New("only INTEGER single-column primary keys can be set as auto incremental")
var ErrLimitedMaxLen = errors.New("only VARCHAR and BLOB types support max length")
var ErrDuplicatedColumn = errors.New("duplicated column")
var ErrInvalidColumn = errors.New("invalid column")
var ErrReservedWord = errors.New("reserved word")
var ErrPKCanNotBeNull = errors.New("primary key can not be null")
var ErrPKCanNotBeUpdated = errors.New("primary key can not be updated")
var ErrNotNullableColumnCannotBeNull = errors.New("not nullable column can not be null")
var ErrNewColumnMustBeNullable = errors.New("new column must be nullable")
var ErrIndexAlreadyExists = errors.New("index already exists")
var ErrMaxNumberOfColumnsInIndexExceeded = errors.New("number of columns in multi-column index exceeded")
var ErrNoAvailableIndex = errors.New("no available index")
var ErrIndexNotFound = errors.New("index not found")
var ErrInvalidNumberOfValues = errors.New("invalid number of values provided")
var ErrInvalidValue = errors.New("invalid value provided")
var ErrInferredMultipleTypes = errors.New("inferred multiple types")
var ErrExpectingDQLStmt = errors.New("illegal statement. DQL statement expected")
var ErrLimitedOrderBy = errors.New("order is limit to one indexed column")
var ErrLimitedGroupBy = errors.New("group by requires ordering by the grouping column")
var ErrIllegalMappedKey = errors.New("error illegal mapped key")
var ErrCorruptedData = store.ErrCorruptedData
var ErrBrokenCatalogColSpecExpirable = fmt.Errorf("%w: catalog column entry set as expirable", ErrCorruptedData)
var ErrNoMoreRows = store.ErrNoMoreEntries
var ErrInvalidTypes = errors.New("invalid types")
var ErrUnsupportedJoinType = errors.New("unsupported join type")
var ErrInvalidCondition = errors.New("invalid condition")
var ErrHavingClauseRequiresGroupClause = errors.New("having clause requires group clause")
var ErrNotComparableValues = errors.New("values are not comparable")
var ErrNumericTypeExpected = errors.New("numeric type expected")
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
var ErrLimitedIndexCreation = errors.New("unique index creation is only supported on empty tables")
var ErrTooManyRows = errors.New("too many rows")
var ErrAlreadyClosed = store.ErrAlreadyClosed
var ErrAmbiguousSelector = errors.New("ambiguous selector")
var ErrUnsupportedCast = fmt.Errorf("%w: unsupported cast", ErrInvalidValue)
var ErrColumnMismatchInUnionStmt = errors.New("column mismatch in union statement")

var MaxKeyLen = 512

const EncIDLen = 4
const EncLenLen = 4

const MaxNumberOfColumnsInIndex = 8

type Engine struct {
	store *store.ImmuStore

	prefix                        []byte
	distinctLimit                 int
	autocommit                    bool
	lazyIndexConstraintValidation bool

	multidbHandler MultiDBHandler
}

type MultiDBHandler interface {
	ListDatabases(ctx context.Context) ([]string, error)
	CreateDatabase(ctx context.Context, db string, ifNotExists bool) error
	UseDatabase(ctx context.Context, db string) error
	ExecPreparedStmts(ctx context.Context, opts *TxOptions, stmts []SQLStmt, params map[string]interface{}) (ntx *SQLTx, committedTxs []*SQLTx, err error)
}

func NewEngine(st *store.ImmuStore, opts *Options) (*Engine, error) {
	if st == nil {
		return nil, ErrIllegalArguments
	}

	if !st.MultiIndexingEnabled() {
		return nil, ErrMultiIndexingNotEnabled
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	e := &Engine{
		store:                         st,
		prefix:                        make([]byte, len(opts.prefix)),
		distinctLimit:                 opts.distinctLimit,
		autocommit:                    opts.autocommit,
		lazyIndexConstraintValidation: opts.lazyIndexConstraintValidation,
		multidbHandler:                opts.multidbHandler,
	}

	copy(e.prefix, opts.prefix)

	err = st.InitIndexing(&store.IndexSpec{
		SourcePrefix:     append(e.prefix, []byte(catalogPrefix)...),
		TargetPrefix:     append(e.prefix, []byte(catalogPrefix)...),
		InjectiveMapping: true,
	})
	if err != nil && !errors.Is(err, store.ErrIndexAlreadyInitialized) {
		return nil, err
	}

	// TODO: find a better way to handle parsing errors
	yyErrorVerbose = true

	return e, nil
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
		UnsafeMVCC:              opts.UnsafeMVCC,
	}

	tx, err := e.store.NewTx(ctx, txOpts)
	if err != nil {
		return nil, err
	}

	if len(opts.Extra) > 0 {
		txmd := store.NewTxMetadata()
		err := txmd.WithExtra(opts.Extra)
		if err != nil {
			return nil, err
		}

		tx.WithMetadata(txmd)
	}

	catalog := newCatalog(e.prefix)

	err = catalog.load(ctx, tx)
	if err != nil {
		return nil, err
	}

	for _, table := range catalog.GetTables() {
		primaryIndex := table.primaryIndex

		rowEntryPrefix := MapKey(
			e.prefix,
			RowPrefix,
			EncodeID(DatabaseID),
			EncodeID(table.id),
			EncodeID(primaryIndex.id),
		)

		mappedPKEntryPrefix := MapKey(
			e.prefix,
			MappedPrefix,
			EncodeID(table.id),
			EncodeID(primaryIndex.id),
		)

		err = e.store.InitIndexing(&store.IndexSpec{
			SourcePrefix: rowEntryPrefix,

			TargetEntryMapper: indexEntryMapperFor(primaryIndex, primaryIndex),
			TargetPrefix:      mappedPKEntryPrefix,

			InjectiveMapping: true,
		})
		if err != nil && !errors.Is(err, store.ErrIndexAlreadyInitialized) {
			return nil, err
		}

		for _, index := range table.indexes {
			if index.IsPrimary() {
				continue
			}

			mappedEntryPrefix := MapKey(
				e.prefix,
				MappedPrefix,
				EncodeID(table.id),
				EncodeID(index.id),
			)

			err = e.store.InitIndexing(&store.IndexSpec{
				SourcePrefix:      rowEntryPrefix,
				SourceEntryMapper: indexEntryMapperFor(primaryIndex, primaryIndex),
				TargetEntryMapper: indexEntryMapperFor(index, primaryIndex),
				TargetPrefix:      mappedEntryPrefix,

				InjectiveMapping: true,
			})
			if errors.Is(err, store.ErrIndexAlreadyInitialized) {
				continue
			}
			if err != nil {
				return nil, err
			}
		}

		if table.autoIncrementPK {
			encMaxPK, err := loadMaxPK(ctx, e.prefix, tx, table)
			if errors.Is(err, store.ErrNoMoreEntries) {
				continue
			}
			if err != nil {
				return nil, err
			}

			if len(encMaxPK) != 9 {
				return nil, ErrCorruptedData
			}

			if encMaxPK[0] != KeyValPrefixNotNull {
				return nil, ErrCorruptedData
			}

			// map to signed integer space
			encMaxPK[1] ^= 0x80

			table.maxPK = int64(binary.BigEndian.Uint64(encMaxPK[1:]))
		}
	}

	return &SQLTx{
		engine:           e,
		opts:             opts,
		tx:               tx,
		catalog:          catalog,
		lastInsertedPKs:  make(map[string]int64),
		firstInsertedPKs: make(map[string]int64),
	}, nil
}

func indexEntryMapperFor(index, primaryIndex *Index) store.EntryMapper {
	// value={count (colID valLen val)+})
	// key=M.{tableID}{indexID}({null}({val}{padding}{valLen})?)+({pkVal}{padding}{pkValLen})+

	valueExtractor := func(value []byte, valuesByColID map[uint32]TypedValue) error {
		voff := 0

		cols := int(binary.BigEndian.Uint32(value[voff:]))
		voff += EncLenLen

		for i := 0; i < cols; i++ {
			if len(value) < EncIDLen {
				return fmt.Errorf("key is lower than required")
			}

			colID := binary.BigEndian.Uint32(value[voff:])
			voff += EncIDLen

			col, err := index.table.GetColumnByID(colID)
			if errors.Is(err, ErrColumnDoesNotExist) {
				vlen := int(binary.BigEndian.Uint32(value[voff:]))
				voff += EncLenLen + vlen
				continue
			} else if err != nil {
				return err
			}

			val, n, err := DecodeValue(value[voff:], col.colType)
			if err != nil {
				return err
			}

			voff += n

			valuesByColID[colID] = val
		}

		return nil
	}

	return func(key, value []byte) ([]byte, error) {
		encodedValues := make([][]byte, 2+len(index.cols)+1)
		encodedValues[0] = EncodeID(index.table.id)
		encodedValues[1] = EncodeID(index.id)

		valuesByColID := make(map[uint32]TypedValue, len(index.cols))

		for _, col := range index.table.cols {
			valuesByColID[col.id] = &NullValue{t: col.colType}
		}

		err := valueExtractor(value, valuesByColID)
		if err != nil {
			return nil, err
		}

		for i, col := range index.cols {
			encKey, _, err := EncodeValueAsKey(valuesByColID[col.id], col.Type(), col.MaxLen())
			if err != nil {
				return nil, err
			}

			encodedValues[2+i] = encKey
		}

		pkEncVals, err := encodedKey(primaryIndex, valuesByColID)
		if err != nil {
			return nil, err
		}

		encodedValues[len(encodedValues)-1] = pkEncVals

		return MapKey(index.enginePrefix(), MappedPrefix, encodedValues...), nil
	}
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

		if currTx.Closed() {
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
	if tx == nil {
		return ErrIllegalArguments
	}

	catalog := newCatalog(e.prefix)

	err := catalog.addSchemaToTx(ctx, e.prefix, tx)
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) GetStore() *store.ImmuStore {
	return e.store
}

func (e *Engine) GetPrefix() []byte {
	return e.prefix
}
