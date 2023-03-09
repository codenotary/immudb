package document

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func closeStore(t *testing.T, st *store.ImmuStore) {
	err := st.Close()
	if !t.Failed() {
		// Do not pollute error output if test has already failed
		require.NoError(t, err)
	}
}

func TestCreateCollection(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions())
	require.NoError(t, err)
	defer closeStore(t, st)

	opts := DefaultOptions()
	engine, err := sql.NewEngine(st, opts)
	require.NoError(t, err)

	_, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
		&sql.CreateDatabaseStmt{DB: "db1"},
	}, nil)
	require.NoError(t, err)

	_, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
		&sql.UseDatabaseStmt{DB: "db1"},
	}, nil)
	require.NoError(t, err)

	_, _, err = engine.ExecPreparedStmts(
		context.Background(),
		nil,
		[]sql.SQLStmt{sql.NewCreateTableStmt(
			"collection1",
			false, []*sql.ColSpec{
				sql.NewColSpec("id", sql.IntegerType, 0, false, false),
				sql.NewColSpec("name", sql.VarcharType, 50, false, false),
				sql.NewColSpec("encoded_obj", sql.BLOBType, 0, false, false),
			},
			[]string{"id", "name"},
		)},
		nil,
	)
	require.NoError(t, err)

	catalog, err := engine.Catalog(context.Background(), nil)
	require.NoError(t, err)

	table, err := catalog.GetTableByName("db1", "collection1")
	require.NoError(t, err)

	require.Equal(t, "collection1", table.Name())
	c, err := table.GetColumnByID(1)
	require.NoError(t, err)
	require.Equal(t, c.Name(), "id")

	c, err = table.GetColumnByID(2)
	require.NoError(t, err)
	require.Equal(t, c.Name(), "name")

}
