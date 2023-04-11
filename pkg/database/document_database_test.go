package database

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/document"
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func newIndexOption(indexType schemav2.IndexType) *schemav2.IndexOption {
	return &schemav2.IndexOption{Type: indexType}
}

func makeDocumentDb(t *testing.T) *db {
	rootPath := t.TempDir()

	dbName := "doc_test_db"
	options := DefaultOption().WithDBRootPath(rootPath).WithCorruptionChecker(false)
	options.storeOpts.WithIndexOptions(options.storeOpts.IndexOpts.WithCompactionThld(2))
	d, err := NewDB(dbName, nil, options, logger.NewSimpleLogger("immudb ", os.Stderr))
	require.NoError(t, err)

	t.Cleanup(func() {
		err := d.Close()
		if !t.Failed() {
			require.NoError(t, err)
		}
	})

	db := d.(*db)

	// _, _, err = db.documentEngine.ExecPreparedStmts(context.Background(), nil, []sql.SQLStmt{&sql.CreateDatabaseStmt{DB: dbName}}, nil)
	// require.NoError(t, err)

	return db
}

func TestObjectDB_Collection(t *testing.T) {
	db := makeDocumentDb(t)

	// create collection
	collectionName := "mycollection"
	_, err := db.CreateCollection(context.Background(), &schemav2.CollectionCreateRequest{
		Name: collectionName,
		IndexKeys: map[string]*schemav2.IndexOption{
			"pincode": newIndexOption(schemav2.IndexType_INTEGER),
		},
	})
	require.NoError(t, err)

	// get collection
	cinfo, err := db.GetCollection(context.Background(), &schemav2.CollectionGetRequest{
		Name: collectionName,
	})
	require.NoError(t, err)
	resp := cinfo.Collection
	require.Equal(t, 2, len(resp.IndexKeys))
	require.Contains(t, resp.IndexKeys, "_id")
	require.Contains(t, resp.IndexKeys, "pincode")
	require.Equal(t, schemav2.IndexType_INTEGER, resp.IndexKeys["pincode"].Type)

	// add document to collection
	docRes, err := db.CreateDocument(context.Background(), &schemav2.DocumentInsertRequest{
		Collection: collectionName,
		Document: &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"pincode": {
					Kind: &structpb.Value_NumberValue{NumberValue: 123},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, docRes)

	// query collection for document
	docs, err := db.GetDocument(context.Background(), &schemav2.DocumentSearchRequest{
		Collection: collectionName,
		Page:       1,
		PerPage:    10,
		Query: []*schemav2.DocumentQuery{
			{
				Field: "pincode",
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 123},
				},
				Operator: schemav2.QueryOperator_EQ,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(docs.Results))
	res := docs.Results[0]
	data := map[string]interface{}{}
	err = json.Unmarshal([]byte(res.Fields[document.DocumentBLOBField].GetStringValue()), &data)
	require.NoError(t, err)
	require.Equal(t, 123.0, data["pincode"])
}
