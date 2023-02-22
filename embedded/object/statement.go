package object

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
)

const (
	nullableFlag      byte = 1 << iota
	autoIncrementFlag byte = 1 << iota
)

type UseDatabaseStmt struct {
	DB string
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

type CreateDatabaseStmt struct {
	DB          string
	ifNotExists bool
}

func (stmt *CreateDatabaseStmt) execAt(ctx context.Context, tx Tx, params map[string]interface{}) (Tx, error) {
	otx := tx.(*ObjectTx)

	if otx.IsExplicitCloseRequired() {
		return nil, fmt.Errorf("%w: collection creation can not be done within a transaction", sql.ErrNonTransactionalStmt)
	}

	if otx.engine.multidbHandler != nil {
		return nil, otx.engine.multidbHandler.CreateDatabase(ctx, stmt.DB, stmt.ifNotExists)
	}

	id := uint32(len(otx.catalog.Databases()) + 1)

	db, err := otx.catalog.NewDatabase(id, stmt.DB)
	if err == sql.ErrDatabaseAlreadyExists && stmt.ifNotExists {
		return otx, nil
	}
	if err != nil {
		return nil, err
	}

	err = otx.set(sql.MapKey(otx.Prefix(), catalogDatabasePrefix, sql.EncodeID(db.ID())), nil, []byte(stmt.DB))
	if err != nil {
		return nil, err
	}

	return otx, nil
}

type CreateCollectionStmt struct {
	collection  string
	ifNotExists bool
	colsSpec    []*sql.ColSpec
	pkColNames  []string
}

func (stmt *CreateCollectionStmt) execAt(ctx context.Context, tx Tx, params map[string]interface{}) (Tx, error) {
	otx := tx.(*ObjectTx)

	if otx.currentDB == nil {
		return nil, sql.ErrNoDatabaseSelected
	}

	if stmt.ifNotExists && otx.currentDB.ExistTable(stmt.collection) {
		return tx, nil
	}

	table, err := otx.currentDB.NewTable(stmt.collection, stmt.colsSpec)
	if err != nil {
		return nil, err
	}

	// createIndexStmt := &sql.CreateIndexStmt{unique: true, table: table.name, cols: stmt.pkColNames}
	// _, err = createIndexStmt.execAt(ctx, tx, params)
	// if err != nil {
	// 	return nil, err
	// }

	for _, col := range table.Cols() {
		if col.IsAutoIncremental() {
			tcols := table.PrimaryIndex().Cols()
			if len(tcols) > 1 || col.ID() != tcols[0].ID() {
				return nil, sql.ErrLimitedAutoIncrement
			}
		}

		err := persistColumn(col, otx)
		if err != nil {
			return nil, err
		}
	}

	mappedKey := sql.MapKey(otx.Prefix(), catalogTablePrefix, sql.EncodeID(otx.currentDB.ID()), sql.EncodeID(table.ID()))

	err = otx.set(mappedKey, nil, []byte(table.Name()))
	if err != nil {
		return nil, err
	}

	return otx, nil
}

func persistColumn(col *sql.Column, tx *ObjectTx) error {
	//{auto_incremental | nullable}{maxLen}{colNAME})
	v := make([]byte, 1+4+len(col.Name()))

	if col.IsAutoIncremental() {
		v[0] = v[0] | autoIncrementFlag
	}

	if col.IsNullable() {
		v[0] = v[0] | nullableFlag
	}

	binary.BigEndian.PutUint32(v[1:], uint32(col.MaxLen()))

	copy(v[5:], []byte(col.Name()))

	mappedKey := sql.MapKey(
		tx.Prefix(),
		catalogColumnPrefix,
		sql.EncodeID(col.Table().Database().ID()),
		sql.EncodeID(col.Table().ID()),
		sql.EncodeID(col.ID()),
		[]byte(col.Type()),
	)

	return tx.set(mappedKey, nil, v)
}

type tableRef struct {
	db    string
	table string
}

func (stmt *tableRef) referencedTable(tx *ObjectTx) (*sql.Table, error) {
	if tx.currentDB == nil {
		return nil, sql.ErrNoDatabaseSelected
	}

	if stmt.db != "" && stmt.db != tx.currentDB.Name() {
		return nil,
			fmt.Errorf(
				"%w: statements must only involve current selected database '%s' but '%s' was referenced",
				sql.ErrNoSupported, tx.currentDB.Name(), stmt.db,
			)
	}

	table, err := tx.currentDB.GetTableByName(stmt.table)
	if err != nil {
		return nil, err
	}

	return table, nil
}

type UpsertIntoStmt struct {
	tableRef *tableRef
	sql.UpsertIntoStmt
}

type RowSpec struct {
	Values []sql.ValueExp
}
