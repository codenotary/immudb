package object

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

var maxKeyLen = 256
var objectPrefix = []byte{3}

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
	ExecPreparedStmts(ctx context.Context, opts *sql.TxOptions, stmts []Stmt, params map[string]interface{}) (ntx *ObjectTx, committedTxs []*ObjectTx, err error)
}

func NewEngine(store *store.ImmuStore, opts *sql.Options) (*Engine, error) {
	if store == nil {
		return nil, sql.ErrIllegalArguments
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	e := &Engine{
		store:         store,
		prefix:        make([]byte, len(opts.GetPrefix())),
		distinctLimit: opts.GetDistinctLimit(),
		autocommit:    opts.GetAutoCommit(),
	}

	copy(e.prefix, opts.GetPrefix())

	return e, nil
}

func newObjectCatalog() *sql.Catalog {
	return sql.NewCatalog(catalogDatabasePrefix, catalogTablePrefix, catalogColumnPrefix, catalogIndexPrefix)
}

func (e *Engine) SetMultiDBHandler(handler MultiDBHandler) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.multidbHandler = handler
}

func (e *Engine) SetCurrentDatabase(ctx context.Context, dbName string) error {
	tx, err := e.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return err
	}
	defer tx.Cancel()

	db, err := tx.catalog.GetDatabaseByName(dbName)
	if err != nil {
		return err
	}

	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.currentDatabase = db.Name()

	return nil
}

func (e *Engine) CurrentDatabase() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	return e.currentDatabase
}

func (e *Engine) NewTx(ctx context.Context, opts *sql.TxOptions) (*ObjectTx, error) {
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

	catalog := newObjectCatalog()

	err = catalog.Load(e.prefix, tx)
	if err != nil {
		return nil, err
	}

	var currentDB *sql.Database

	if e.currentDatabase != "" {
		db, err := catalog.GetDatabaseByName(e.currentDatabase)
		if err != nil {
			return nil, err
		}

		currentDB = db
	}

	return &ObjectTx{
		engine:           e,
		opts:             opts,
		tx:               tx,
		catalog:          catalog,
		currentDB:        currentDB,
		lastInsertedPKs:  make(map[string]int64),
		firstInsertedPKs: make(map[string]int64),
	}, nil
}

func (e *Engine) ExecPreparedStmts(ctx context.Context, tx *ObjectTx, stmts []Stmt, params map[string]interface{}) (ntx *ObjectTx, committedTxs []*ObjectTx, err error) {
	ntx, ctxs, pendingStmts, err := e.execPreparedStmts(ctx, tx, stmts, params)
	if err != nil {
		return ntx, ctxs, err
	}

	if len(pendingStmts) > 0 {
		// a different database was selected

		if e.multidbHandler == nil || ntx != nil {
			return ntx, ctxs, fmt.Errorf("%w: all statements should have been executed when not using a multidbHandler", sql.ErrUnexpected)
		}

		var opts *sql.TxOptions

		if tx != nil {
			opts = tx.opts
		} else {
			opts = sql.DefaultTxOptions()
		}

		ntx, hctxs, err := e.multidbHandler.ExecPreparedStmts(ctx, opts, pendingStmts, params)

		return ntx, append(ctxs, hctxs...), err
	}

	return ntx, ctxs, nil
}

func (e *Engine) execPreparedStmts(ctx context.Context, tx *ObjectTx, stmts []Stmt, params map[string]interface{}) (ntx *ObjectTx, committedTxs []*ObjectTx, pendingStmts []Stmt, err error) {
	if len(stmts) == 0 {
		return nil, nil, stmts, sql.ErrIllegalArguments
	}

	nparams, err := normalizeParams(params)
	if err != nil {
		return nil, nil, stmts, err
	}

	currTx := tx

	execStmts := 0

	for _, stmt := range stmts {
		if stmt == nil {
			return nil, nil, stmts[execStmts:], sql.ErrIllegalArguments
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
			var opts *sql.TxOptions

			if currTx != nil {
				opts = currTx.opts
			} else if tx != nil {
				opts = tx.opts
			} else {
				opts = sql.DefaultTxOptions()
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

		currTx = ntx.(*ObjectTx)

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

func (e *Engine) Catalog(ctx context.Context, tx *ObjectTx) (catalog *sql.Catalog, err error) {
	qtx := tx

	if qtx == nil {
		qtx, err = e.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
		if err != nil {
			return nil, err
		}
		defer qtx.Cancel()
	}

	return qtx.Catalog(), nil
}

func normalizeParams(params map[string]interface{}) (map[string]interface{}, error) {
	nparams := make(map[string]interface{}, len(params))

	for name, value := range params {
		nname := strings.ToLower(name)

		_, exists := nparams[nname]
		if exists {
			return nil, sql.ErrDuplicatedParameters
		}

		nparams[nname] = value
	}

	return nparams, nil
}

// // CopyCatalogToTx copies the current sql catalog to the ongoing transaction.
// func (e *Engine) CopyCatalogToTx(ctx context.Context, tx *store.OngoingTx) error {
// 	e.mutex.RLock()
// 	defer e.mutex.RUnlock()

// 	catalog := sql.newObjectCatalog()
// 	err := catalog.addSchemaToTx(e.prefix, tx)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }
