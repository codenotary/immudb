package database

import (
	"context"
	"testing"

	schemav2 "github.com/codenotary/immudb/pkg/api/schemav2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func Test_db_CreateCollection(t *testing.T) {
	db := makeDb(t)

	// create collection
	collectionName := "mycollection"
	err := db.CreateCollection(context.Background(), &schemav2.CollectionCreateRequest{
		Name: collectionName,
		PrimaryKeys: map[string]schemav2.PossibleIndexType{
			"id": schemav2.PossibleIndexType_INTEGER,
		},
	})
	require.NoError(t, err)

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
	resp, err := db.GetDocument(context.Background(), &schemav2.DocumentSearchRequest{
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
	require.Equal(t, 1, len(resp.Results))
}
