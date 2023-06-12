/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
package database

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/verification"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

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

	return db
}

func TestDocumentDB_WithCollections(t *testing.T) {
	db := makeDocumentDb(t)
	// create collection
	defaultCollectionName := "mycollection"

	t.Run("should pass when creating a collection", func(t *testing.T) {

		_, err := db.CreateCollection(context.Background(), &protomodel.CreateCollectionRequest{
			Name: defaultCollectionName,
			Fields: []*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_INTEGER},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := db.GetCollection(context.Background(), &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		expectedFieldKeys := []*protomodel.Field{
			{Name: "_id", Type: protomodel.FieldType_STRING},
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
		}

		collection := cinfo.Collection

		for i, idxType := range expectedFieldKeys {
			require.Equal(t, idxType.Name, collection.Fields[i].Name)
			require.Equal(t, idxType.Type, collection.Fields[i].Type)
		}

	})

	t.Run("should pass when adding an index to the collection", func(t *testing.T) {
		_, err := db.CreateIndex(context.Background(), &protomodel.CreateIndexRequest{
			CollectionName: defaultCollectionName,
			Fields:         []string{"number"},
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := db.GetCollection(context.Background(), &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		collection := cinfo.Collection

		expectedIndexKeys := []*protomodel.Index{
			{Fields: []string{"_id"}, IsUnique: true},
			{Fields: []string{"number"}, IsUnique: false},
		}

		for i, idxType := range expectedIndexKeys {
			require.Equal(t, idxType.Fields, collection.Indexes[i].Fields)
			require.Equal(t, idxType.IsUnique, collection.Indexes[i].IsUnique)
		}
	})

	t.Run("should pass when deleting an index to the collection", func(t *testing.T) {
		_, err := db.DeleteIndex(context.Background(), &protomodel.DeleteIndexRequest{
			CollectionName: defaultCollectionName,
			Fields:         []string{"number"},
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := db.GetCollection(context.Background(), &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		collection := cinfo.Collection
		require.Len(t, collection.Indexes, 1)

		expectedIndexKeys := []*protomodel.Index{
			{Fields: []string{"_id"}, IsUnique: true},
		}

		for i, idxType := range expectedIndexKeys {
			require.Equal(t, idxType.Fields, collection.Indexes[i].Fields)
			require.Equal(t, idxType.IsUnique, collection.Indexes[i].IsUnique)
		}
	})

	t.Run("should pass when updating a collection", func(t *testing.T) {
		_, err := db.UpdateCollection(context.Background(), &protomodel.UpdateCollectionRequest{
			Name:                defaultCollectionName,
			DocumentIdFieldName: "foo",
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := db.GetCollection(context.Background(), &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		collection := cinfo.Collection
		require.Equal(t, "foo", collection.Fields[0].Name)
	})

	t.Run("should pass when deleting collection", func(t *testing.T) {
		_, err := db.DeleteCollection(context.Background(), &protomodel.DeleteCollectionRequest{
			Name: "mycollection",
		})
		require.NoError(t, err)

		resp, err := db.GetCollections(context.Background(), &protomodel.GetCollectionsRequest{})
		require.NoError(t, err)

		require.Len(t, resp.Collections, 0)
	})

	t.Run("should pass when creating multiple collections", func(t *testing.T) {
		// create collection
		collections := []string{"mycollection1", "mycollection2", "mycollection3"}

		for _, collectionName := range collections {
			_, err := db.CreateCollection(context.Background(), &protomodel.CreateCollectionRequest{
				Name: collectionName,
				Fields: []*protomodel.Field{
					{Name: "number", Type: protomodel.FieldType_INTEGER},
					{Name: "country", Type: protomodel.FieldType_STRING},
				},
			})
			require.NoError(t, err)
		}

		expectedFieldKeys := []*protomodel.Field{
			{Name: "_id", Type: protomodel.FieldType_STRING},
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
		}

		// verify collection
		resp, err := db.GetCollections(context.Background(), &protomodel.GetCollectionsRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Collections, len(resp.Collections))

		for i, collection := range resp.Collections {
			require.Equal(t, collections[i], collection.Name)
			for i, idxType := range expectedFieldKeys {
				require.Equal(t, idxType.Name, collection.Fields[i].Name)
				require.Equal(t, idxType.Type, collection.Fields[i].Type)
			}
		}
	})
}

func TestDocumentDB_WithDocuments(t *testing.T) {
	db := makeDocumentDb(t)

	// create collection
	collectionName := "mycollection"
	_, err := db.CreateCollection(context.Background(), &protomodel.CreateCollectionRequest{
		Name: collectionName,
		Fields: []*protomodel.Field{
			{
				Name: "pincode",
				Type: protomodel.FieldType_INTEGER,
			},
		},
		Indexes: []*protomodel.Index{
			{
				Fields: []string{"pincode"},
			},
		},
	})
	require.NoError(t, err)

	t.Run("should fail with empty document", func(t *testing.T) {
		// add document to collection
		_, err := db.InsertDocuments(context.Background(), &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents:      nil,
		})
		require.Error(t, err)
	})

	var res *protomodel.InsertDocumentsResponse
	var docID string
	t.Run("should pass when adding documents", func(t *testing.T) {
		var err error
		// add document to collection
		res, err = db.InsertDocuments(context.Background(), &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"pincode": {
							Kind: &structpb.Value_NumberValue{NumberValue: 123},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res.DocumentIds, 1)
		docID = res.DocumentIds[0]
	})

	var doc *structpb.Struct
	t.Run("should pass when querying documents", func(t *testing.T) {
		// query collection for document
		reader, err := db.SearchDocuments(context.Background(), &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(123),
						},
					},
				},
			},
		},
			0,
		)
		require.NoError(t, err)

		defer reader.Close()

		revision, err := reader.Read(context.Background())
		require.NoError(t, err)

		doc = revision.Document
		require.Equal(t, 123.0, doc.Fields["pincode"].GetNumberValue())
	})

	t.Run("should pass when querying documents with proof", func(t *testing.T) {
		proofRes, err := db.ProofDocument(context.Background(), &protomodel.ProofDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
		})
		require.NoError(t, err)
		require.NotNil(t, proofRes)

		newState, err := verification.VerifyDocument(context.Background(), proofRes, doc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, newState.TxId)
	})

	t.Run("should pass when replacing document", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(123),
						},
					},
				},
			},
		}

		resp, err := db.ReplaceDocuments(context.Background(), &protomodel.ReplaceDocumentsRequest{
			Query: query,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": structpb.NewNumberValue(321),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 1)
		rev := resp.Revisions[0]
		require.Equal(t, docID, rev.DocumentId)

		reader, err := db.SearchDocuments(context.Background(), &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_EQ,
							Value:    structpb.NewNumberValue(321),
						},
					},
				},
			},
		},
			0,
		)
		require.NoError(t, err)

		defer reader.Close()

		revision, err := reader.Read(context.Background())
		require.NoError(t, err)

		doc = revision.Document
		require.Equal(t, 321.0, doc.Fields["pincode"].GetNumberValue())
	})

	t.Run("should pass when auditing document", func(t *testing.T) {
		resp, err := db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
			Page:           1,
			PageSize:       10,
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 2)

		for _, rev := range resp.Revisions {
			require.Equal(t, docID, rev.Document.Fields["_id"].GetStringValue())
		}
	})
}

func TestDocumentDB_WithSerializedJsonDocument(t *testing.T) {
	db := makeDocumentDb(t)

	collectionName := "mycollection"

	_, err := db.CreateCollection(context.Background(), &protomodel.CreateCollectionRequest{
		Name:    collectionName,
		Fields:  []*protomodel.Field{},
		Indexes: []*protomodel.Index{},
	})
	require.NoError(t, err)

	jsonDoc := `{
        "old_record": null,
        "record": {
            "access_code": "1b86ff6b189f4c36a50b9073f6dfed17ee0388568a4f4651a68bf67a7c7aaf45",
            "badge_uuid": "4ee1dbb2-544a-4e34-b99a-8003379f5d88",
            "created_at": "2023-06-11T10:43:31.032008+00:00",
            "id": 20,
            "is_public": false,
            "project_id": 1,
            "sbom": {
                "str": "sU2tZ3NC31H3WzlzfOvs7EsGYwLBSKTGwn3ooopNdiK4pf8eF75XWNe1aFYRGEiXwTeCc6vLFrGxAonWrMFN2AC840Wb6"
            },
            "vault_uuid": null
        },
        "schema": "public",
        "table": "sbom",
        "type": "INSERT"
    }`

	doc := &structpb.Struct{}

	err = json.Unmarshal([]byte(jsonDoc), doc)
	require.NoError(t, err)

	_, err = db.InsertDocuments(context.Background(), &protomodel.InsertDocumentsRequest{
		CollectionName: collectionName,
		Documents: []*structpb.Struct{
			doc,
		},
	})
	require.NoError(t, err)
}
