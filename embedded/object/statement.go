package object

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
)

type UseDatabaseStmt struct {
	DB string
}

func (stmt *UseDatabaseStmt) inferParameters(ctx context.Context, tx Tx, params map[string]sql.SQLValueType) error {
	return nil
}

func (stmt *UseDatabaseStmt) execAt(ctx context.Context, tx Tx, params map[string]interface{}) (Tx, error) {
	if stmt.DB == "" {
		return nil, fmt.Errorf("%w: no database name was provided", sql.ErrIllegalArguments)
	}

	otx := tx.(*ObjectTx)

	if otx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: database selection can NOT be executed within a transaction block", sql.ErrNonTransactionalStmt)
	}

	if otx.engine.multidbHandler != nil {
		return otx, otx.engine.multidbHandler.UseDatabase(ctx, stmt.DB)
	}

	_, err := otx.catalog.GetDatabaseByName(stmt.DB)
	if err != nil {
		return nil, err
	}

	otx.engine.mutex.Lock()
	otx.engine.currentDatabase = stmt.DB
	otx.engine.mutex.Unlock()

	return otx, otx.useDatabase(stmt.DB)
}
