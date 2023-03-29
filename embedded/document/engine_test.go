package document

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type multidbHandlerMock struct {
	dbs    []string
	engine *Engine
}

func (h *multidbHandlerMock) ListDatabases(ctx context.Context) ([]string, error) {
	return h.dbs, nil
}

func (h *multidbHandlerMock) CreateDatabase(ctx context.Context, db string, ifNotExists bool) error {
	_, _, err := h.engine.ExecPreparedStmts(ctx, nil, []sql.SQLStmt{
		&sql.CreateDatabaseStmt{DB: db},
	}, nil)
	return err
}

func (h *multidbHandlerMock) UseDatabase(ctx context.Context, db string) error {
	_, _, err := h.engine.ExecPreparedStmts(ctx, nil, []sql.SQLStmt{
		&sql.UseDatabaseStmt{DB: db},
	}, nil)
	return err
}

func (h *multidbHandlerMock) ExecPreparedStmts(
	ctx context.Context,
	opts *sql.TxOptions,
	stmts []sql.SQLStmt,
	params map[string]interface{},
) (ntx *sql.SQLTx, committedTxs []*sql.SQLTx, err error) {
	return h.engine.ExecPreparedStmts(ctx, nil, stmts, params)
}

func makeEngine(t *testing.T) *Engine {

	st, err := store.Open(t.TempDir(), store.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := st.Close()
		if !t.Failed() {
			// Do not pollute error output if test has already failed
			require.NoError(t, err)
		}
	})

	handler := &multidbHandlerMock{}
	opts := sql.DefaultOptions().WithMultiDBHandler(handler)
	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	// _, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
	// 	&sql.CreateDatabaseStmt{DB: "db1"},
	// }, nil)
	// require.NoError(t, err)

	// _, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
	// 	&sql.UseDatabaseStmt{DB: "db1"},
	// }, nil)
	// require.NoError(t, err)

	return engine
}

func TestListCollections(t *testing.T) {
	engine := makeEngine(t)

	collections := []string{"mycollection1", "mycollection2", "mycollection3"}
	for _, collectionName := range collections {
		err := engine.CreateCollection(
			context.Background(),
			collectionName,
			map[string]sql.SQLValueType{
				"number":  sql.IntegerType,
				"name":    sql.BLOBType,
				"pin":     sql.IntegerType,
				"country": sql.VarcharType,
			},
		)
		require.NoError(t, err)
	}

	collectionList, err := engine.ListCollections(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(collections), len(collectionList))

	for _, indexes := range collectionList {
		for _, index := range indexes {
			if index.IsPrimary() {
				require.Equal(t, 1, len(index.Cols()))
			} else {
				require.Equal(t, 4, len(index.Cols()))
			}
		}
	}
}

func TestCreateCollection(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"
	err := engine.CreateCollection(
		context.Background(),
		collectionName,
		map[string]sql.SQLValueType{
			"number":  sql.IntegerType,
			"name":    sql.VarcharType,
			"pin":     sql.IntegerType,
			"country": sql.VarcharType,
		},
	)
	require.NoError(t, err)

	catalog, err := engine.Catalog(context.Background(), nil)
	require.NoError(t, err)

	table, err := catalog.GetTableByName(collectionName)
	require.NoError(t, err)

	require.Equal(t, collectionName, table.Name())

	pcols := []string{"_id"}
	idxcols := []string{"pin", "country", "number", "name"}

	// verify primary keys
	for _, col := range pcols {
		c, err := table.GetColumnByName(col)
		require.NoError(t, err)
		require.Equal(t, c.Name(), col)
	}

	// verify index keys
	for _, col := range idxcols {
		c, err := table.GetColumnByName(col)
		require.NoError(t, err)
		require.Equal(t, c.Name(), col)
	}

	// get collection
	indexes, err := engine.GetCollection(context.Background(), collectionName)
	require.NoError(t, err)
	require.Equal(t, 2, len(indexes))

	primaryKeyCount := 0
	indexKeyCount := 0
	for _, idx := range indexes {
		// check if primary key
		if idx.IsPrimary() {
			primaryKeyCount += len(idx.Cols())
		} else {
			indexKeyCount += len(idx.Cols())
		}
	}
	require.Equal(t, 1, primaryKeyCount)
	require.Equal(t, 4, indexKeyCount)
}

func newIndexOption(indexType schemav2.IndexType) *schemav2.IndexOption {
	return &schemav2.IndexOption{Type: indexType}
}

func TestGetDocument(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"pincode": sql.IntegerType,
		"country": sql.VarcharType,
		"data":    sql.BLOBType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add document to collection
	_, err = engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value1"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	expressions := []*Query{
		{
			Field:    "country",
			Operator: 0, // EQ
			Value: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
		},
		{
			Field:    "pincode",
			Operator: 0, // EQ
			Value: &structpb.Value{
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
		},
	}

	// invalid page number
	_, err = engine.GetDocument(context.Background(), collectionName, expressions, 0, 10)
	require.Error(t, err)

	// invalid page limit
	_, err = engine.GetDocument(context.Background(), collectionName, expressions, 1, 0)
	require.Error(t, err)

	doc, err := engine.GetDocument(context.Background(), collectionName, expressions, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(doc))
}

func TestDocumentAudit(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"pincode": sql.IntegerType,
		"country": sql.VarcharType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add document to collection
	docID, err := engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
		},
	})
	require.NoError(t, err)

	// get document audit
	res, err := engine.DocumentAudit(context.Background(), collectionName, docID, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(res))

	// verify audit result
	val := string(res[0].Value)
	require.Contains(t, val, "pincode")
	require.Contains(t, val, "country")
}
