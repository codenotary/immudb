package database

import (
	"context"
	"testing"

	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func newIndexOption(indexType schemav2.IndexType) *schemav2.IndexOption {
	return &schemav2.IndexOption{Type: indexType}
}

func TestObjectDB_Collection(t *testing.T) {
	db := makeDb(t)

	// create collection
	collectionName := "mycollection"
	err := db.CreateCollection(context.Background(), &schemav2.CollectionCreateRequest{
		Name: collectionName,
		PrimaryKeys: map[string]*schemav2.IndexOption{
			"id": newIndexOption(schemav2.IndexType_INTEGER),
		},
	})
	require.NoError(t, err)

	// get collection
	resp, err := db.GetCollection(context.Background(), &schemav2.CollectionGetRequest{
		Name: collectionName,
	})
	require.NoError(t, err)
	require.Equal(t, 0, len(resp.IndexKeys))
	require.Equal(t, 1, len(resp.PrimaryKeys))
	require.Contains(t, resp.PrimaryKeys, "id")
	require.Equal(t, schemav2.IndexType_INTEGER, resp.PrimaryKeys["id"].Type)

	// add document to collection
	_, err = db.CreateDocument(context.Background(), &schemav2.DocumentInsertRequest{
		Collection: collectionName,
		Document: []*structpb.Struct{
			{
				Fields: map[string]*structpb.Value{
					"id": {
						Kind: &structpb.Value_NumberValue{NumberValue: 123},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// query collection for document
	docs, err := db.GetDocument(context.Background(), &schemav2.DocumentSearchRequest{
		Collection: collectionName,
		Query: []*schemav2.DocumentQuery{
			{
				Field: "id",
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
	require.Equal(t, 123, int(res.Fields["id"].GetNumberValue()))
}
