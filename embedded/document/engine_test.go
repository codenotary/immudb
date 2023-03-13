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

	opts := DefaultOptions()
	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	_, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
		&sql.CreateDatabaseStmt{DB: "db1"},
	}, nil)
	require.NoError(t, err)

	_, _, err = engine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{
		&sql.UseDatabaseStmt{DB: "db1"},
	}, nil)
	require.NoError(t, err)

	return engine
}

func TestCreateCollection(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"
	err := engine.CreateCollection(
		context.Background(),
		collectionName,
		map[string]sql.SQLValueType{
			"id":     sql.IntegerType,
			"number": sql.IntegerType,
		},
		map[string]sql.SQLValueType{
			"pin": sql.IntegerType,
		},
	)
	require.NoError(t, err)

	catalog, err := engine.Catalog(context.Background(), nil)
	require.NoError(t, err)

	table, err := catalog.GetTableByName("db1", collectionName)
	require.NoError(t, err)

	require.Equal(t, collectionName, table.Name())

	c, err := table.GetColumnByName("id")
	require.NoError(t, err)
	require.Equal(t, c.Name(), "id")

	c, err = table.GetColumnByName("number")
	require.NoError(t, err)
	require.Equal(t, c.Name(), "number")

	c, err = table.GetColumnByName("pin")
	require.NoError(t, err)
	require.Equal(t, c.Name(), "pin")

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
	require.Equal(t, 2, primaryKeyCount)
	require.Equal(t, 1, indexKeyCount)
}

func newIndexOption(indexType schemav2.IndexType) *schemav2.IndexOption {
	return &schemav2.IndexOption{Type: indexType}
}

func TestGetDocument(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"id":         sql.IntegerType,
		"pincode":    sql.IntegerType,
		"country_id": sql.IntegerType,
	}, nil)
	require.NoError(t, err)
	require.NoError(t, err)

	// add document to collection
	err = engine.CreateDocument(context.Background(), collectionName, []*structpb.Struct{
		{
			Fields: map[string]*structpb.Value{
				"id": {
					Kind: &structpb.Value_NumberValue{NumberValue: 1},
				},
				"pincode": {
					Kind: &structpb.Value_NumberValue{NumberValue: 2},
				},
				"country_id": {
					Kind: &structpb.Value_NumberValue{NumberValue: 3},
				},
			},
		},
	},
	)
	require.NoError(t, err)

	expressions := []*Query{
		{
			Field:    "country_id",
			Operator: 0, // EQ
			Value: &structpb.Value{
				Kind: &structpb.Value_NumberValue{NumberValue: 3},
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

	doc, err := engine.GetDocument(context.Background(), collectionName, expressions, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(doc))
}
