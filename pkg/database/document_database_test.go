/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	https://mariadb.com/bsl11/

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
	"time"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/verification"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func makeDocumentDb(t *testing.T) *db {
	rootPath := t.TempDir()

	dbName := "doc_test_db"
	options := DefaultOptions().
		WithDBRootPath(rootPath)

	options.storeOpts.IndexOpts.WithCompactionThld(2)

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

func TestDocumentDB_InvalidParameters(t *testing.T) {
	db := makeDocumentDb(t)

	_, err := db.CreateCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.GetCollection(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.UpdateCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.DeleteCollection(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.CreateIndex(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.DeleteIndex(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.InsertDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.ReplaceDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.AuditDocument(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.CountDocuments(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.DeleteDocuments(context.Background(), "admin", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.ProofDocument(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestDocumentDB_WritesOnReplica(t *testing.T) {
	db := makeDocumentDb(t)

	db.AsReplica(true, false, 0)

	_, err := db.CreateCollection(context.Background(), "admin", &protomodel.CreateCollectionRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.UpdateCollection(context.Background(), "admin", &protomodel.UpdateCollectionRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.DeleteCollection(context.Background(), "admin", &protomodel.DeleteCollectionRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.AddField(context.Background(), "admin", &protomodel.AddFieldRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.RemoveField(context.Background(), "admin", &protomodel.RemoveFieldRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.CreateIndex(context.Background(), "admin", &protomodel.CreateIndexRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.DeleteIndex(context.Background(), "admin", &protomodel.DeleteIndexRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.InsertDocuments(context.Background(), "admin", &protomodel.InsertDocumentsRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.ReplaceDocuments(context.Background(), "admin", &protomodel.ReplaceDocumentsRequest{})
	require.ErrorIs(t, err, ErrIsReplica)

	_, err = db.DeleteDocuments(context.Background(), "admin", &protomodel.DeleteDocumentsRequest{})
	require.ErrorIs(t, err, ErrIsReplica)
}

func TestDocumentDB_WithCollections(t *testing.T) {
	db := makeDocumentDb(t)

	defaultCollectionName := "mycollection"

	t.Run("should pass when creating a collection", func(t *testing.T) {
		_, err := db.CreateCollection(context.Background(), "admin", &protomodel.CreateCollectionRequest{
			Name: defaultCollectionName,
			Fields: []*protomodel.Field{
				{Name: "uuid", Type: protomodel.FieldType_UUID},
				{Name: "number", Type: protomodel.FieldType_INTEGER},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
		})
		require.NoError(t, err)

		_, err = db.AddField(context.Background(), "admin", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = db.AddField(context.Background(), "admin", &protomodel.AddFieldRequest{
			CollectionName: defaultCollectionName,
		})
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = db.AddField(context.Background(), "admin", &protomodel.AddFieldRequest{
			CollectionName: defaultCollectionName,
			Field:          &protomodel.Field{Name: "extra_field", Type: protomodel.FieldType_UUID},
		})
		require.NoError(t, err)

		_, err = db.AddField(context.Background(), "admin", &protomodel.AddFieldRequest{
			CollectionName: defaultCollectionName,
			Field:          &protomodel.Field{Name: "extra_field", Type: protomodel.FieldType_UUID},
		})
		require.ErrorIs(t, err, document.ErrFieldAlreadyExists)

		cinfo, err := db.GetCollection(context.Background(), &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		collection := cinfo.Collection

		expectedFieldKeys := []*protomodel.Field{
			{Name: "_id", Type: protomodel.FieldType_STRING},
			{Name: "uuid", Type: protomodel.FieldType_UUID},
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "extra_field", Type: protomodel.FieldType_UUID},
		}

		for i, idxType := range expectedFieldKeys {
			require.Equal(t, idxType.Name, collection.Fields[i].Name)
			require.Equal(t, idxType.Type, collection.Fields[i].Type)
		}

		_, err = db.RemoveField(context.Background(), "admin", nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, err = db.RemoveField(context.Background(), "admin", &protomodel.RemoveFieldRequest{
			CollectionName: defaultCollectionName,
			FieldName:      "extra_field",
		})
		require.NoError(t, err)

		_, err = db.RemoveField(context.Background(), "admin", &protomodel.RemoveFieldRequest{
			CollectionName: defaultCollectionName,
			FieldName:      "extra_field",
		})
		require.ErrorIs(t, err, document.ErrFieldDoesNotExist)

		expectedFieldKeys = []*protomodel.Field{
			{Name: "_id", Type: protomodel.FieldType_STRING},
			{Name: "uuid", Type: protomodel.FieldType_UUID},
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
		}

		for i, idxType := range expectedFieldKeys {
			require.Equal(t, idxType.Name, collection.Fields[i].Name)
			require.Equal(t, idxType.Type, collection.Fields[i].Type)
		}

		countResp, err := db.CountDocuments(context.Background(), &protomodel.CountDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: defaultCollectionName,
			},
		})
		require.NoError(t, err)
		require.Zero(t, countResp.Count)

		_, err = db.CountDocuments(context.Background(), &protomodel.CountDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: "1invalidCollectionName",
			},
		})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("should pass when adding an index to the collection", func(t *testing.T) {
		_, err := db.CreateIndex(context.Background(), "admin", &protomodel.CreateIndexRequest{
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
		_, err := db.DeleteIndex(context.Background(), "admin", &protomodel.DeleteIndexRequest{
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
		_, err := db.UpdateCollection(context.Background(), "admin", &protomodel.UpdateCollectionRequest{
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

	t.Run("should pass when deleting documents", func(t *testing.T) {
		_, err := db.DeleteDocuments(context.Background(), "admin", &protomodel.DeleteDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: defaultCollectionName,
			},
		})
		require.NoError(t, err)

		_, err = db.DeleteDocuments(context.Background(), "admin", &protomodel.DeleteDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: "1invalidCollectionName",
			},
		})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("should pass when deleting collection", func(t *testing.T) {
		_, err := db.DeleteCollection(context.Background(), "admin", &protomodel.DeleteCollectionRequest{
			Name: defaultCollectionName,
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
			_, err := db.CreateCollection(context.Background(), "admin", &protomodel.CreateCollectionRequest{
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
	_, err := db.CreateCollection(context.Background(), "admin", &protomodel.CreateCollectionRequest{
		Name: collectionName,
		Fields: []*protomodel.Field{
			{Name: "uuid", Type: protomodel.FieldType_UUID},
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
		},
		Indexes: []*protomodel.Index{
			{Fields: []string{"pincode"}},
		},
	})
	require.NoError(t, err)

	t.Run("should fail with empty document", func(t *testing.T) {
		// add document to collection
		_, err := db.InsertDocuments(context.Background(), "admin", &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents:      nil,
		})
		require.Error(t, err)
	})

	var res *protomodel.InsertDocumentsResponse
	var docID string

	u, err := uuid.NewUUID()
	require.NoError(t, err)

	t.Run("should pass when adding documents", func(t *testing.T) {
		// add document to collection
		res, err = db.InsertDocuments(context.Background(), "admin", &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"uuid": {
							Kind: &structpb.Value_StringValue{StringValue: u.String()},
						},
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

		countResp, err := db.CountDocuments(context.Background(), &protomodel.CountDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: collectionName,
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, countResp.Count)
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
		require.Equal(t, u.String(), doc.Fields["uuid"].GetStringValue())
		require.Equal(t, 123.0, doc.Fields["pincode"].GetNumberValue())
	})

	var knownState *schema.ImmutableState

	t.Run("should pass when querying documents with proof", func(t *testing.T) {
		proofRes, err := db.ProofDocument(context.Background(), &protomodel.ProofDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
		})
		require.NoError(t, err)
		require.NotNil(t, proofRes)

		knownState, err = verification.VerifyDocument(context.Background(), proofRes, doc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, knownState.TxId)
	})

	var updatedDoc *structpb.Struct

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

		resp, err := db.ReplaceDocuments(context.Background(), "admin", &protomodel.ReplaceDocumentsRequest{
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

		updatedDoc = revision.Document
		require.Equal(t, 321.0, updatedDoc.Fields["pincode"].GetNumberValue())

		countResp, err := db.CountDocuments(context.Background(), &protomodel.CountDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: collectionName,
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 1, countResp.Count)
	})

	t.Run("should pass when auditing document without requesting payloads", func(t *testing.T) {
		resp, err := db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
			Page:           1,
			PageSize:       10,
			OmitPayload:    true,
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 2)

		for _, rev := range resp.Revisions {
			require.Nil(t, rev.Document)
			require.Equal(t, docID, rev.DocumentId)
			require.Equal(t, "admin", rev.Username)
		}
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
			require.Equal(t, "admin", rev.Username)
		}
	})

	t.Run("should pass when querying updated document with proof", func(t *testing.T) {
		proofRes, err := db.ProofDocument(context.Background(), &protomodel.ProofDocumentRequest{
			CollectionName:          collectionName,
			DocumentId:              docID,
			ProofSinceTransactionId: knownState.TxId,
		})
		require.NoError(t, err)
		require.NotNil(t, proofRes)

		newState, err := verification.VerifyDocument(context.Background(), proofRes, updatedDoc, knownState, nil)
		require.NoError(t, err)
		require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, newState.TxId)
	})

	t.Run("should pass when querying updated document with proof", func(t *testing.T) {
		proofRes, err := db.ProofDocument(context.Background(), &protomodel.ProofDocumentRequest{
			CollectionName:          collectionName,
			DocumentId:              docID,
			TransactionId:           knownState.TxId,
			ProofSinceTransactionId: knownState.TxId,
		})
		require.NoError(t, err)
		require.NotNil(t, proofRes)

		newState, err := verification.VerifyDocument(context.Background(), proofRes, doc, knownState, nil)
		require.NoError(t, err)
		require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, newState.TxId)
	})

	t.Run("should fail when verifying a document with invalid id", func(t *testing.T) {
		proofRes, err := db.ProofDocument(context.Background(), &protomodel.ProofDocumentRequest{
			CollectionName:          collectionName,
			DocumentId:              docID,
			TransactionId:           knownState.TxId,
			ProofSinceTransactionId: knownState.TxId,
		})
		require.NoError(t, err)
		require.NotNil(t, proofRes)

		_, err = verification.VerifyDocument(context.Background(), proofRes, doc, &schema.ImmutableState{
			TxId: proofRes.VerifiableTx.DualProof.TargetTxHeader.Id + 1,
		}, nil)
		require.ErrorIs(t, err, store.ErrInvalidProof)

		doc.Fields[proofRes.DocumentIdFieldName] = structpb.NewNullValue()

		_, err = verification.VerifyDocument(context.Background(), proofRes, doc, knownState, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})
}

func TestDocumentDB_AuditDocuments_CornerCases(t *testing.T) {
	db := makeDocumentDb(t)

	_, err := db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
		CollectionName: "mycollection",
		DocumentId:     "",
		Page:           0,
		PageSize:       0,
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
		CollectionName: "mycollection",
		DocumentId:     "",
		Page:           1,
		PageSize:       MaxKeyScanLimit + 1,
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
		CollectionName: "mycollection",
		DocumentId:     "",
		Page:           1,
		PageSize:       1,
	})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.AuditDocument(context.Background(), &protomodel.AuditDocumentRequest{
		CollectionName: "1invalidCollectionName",
		DocumentId:     document.NewDocumentIDFromTimestamp(time.Now(), 1).EncodeToHexString(),
		Page:           1,
		PageSize:       1,
	})
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestDocumentDB_WithSerializedJsonDocument(t *testing.T) {
	db := makeDocumentDb(t)

	collectionName := "mycollection"

	_, err := db.CreateCollection(context.Background(), "admin", &protomodel.CreateCollectionRequest{
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

	_, err = db.InsertDocuments(context.Background(), "admin", &protomodel.InsertDocumentsRequest{
		CollectionName: collectionName,
		Documents: []*structpb.Struct{
			doc,
		},
	})
	require.NoError(t, err)
}
