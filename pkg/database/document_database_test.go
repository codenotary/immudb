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

func TestDocumentDB_Collection(t *testing.T) {
	db := makeDocumentDb(t)

	// create collection
	collectionName := "mycollection"

	_, err := db.CreateCollection(context.Background(), &protomodel.CollectionCreateRequest{
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

	// get collection
	cinfo, err := db.GetCollection(context.Background(), &protomodel.CollectionGetRequest{
		Name: collectionName,
	})
	require.NoError(t, err)

	resp := cinfo.Collection
	require.Equal(t, 2, len(resp.Fields))
	require.Equal(t, 2, len(resp.Indexes))
	require.Equal(t, resp.Fields[0].Name, "_id")
	require.Contains(t, resp.Fields[1].Name, "pincode")
	require.Equal(t, protomodel.FieldType_STRING, resp.Fields[0].Type)
	require.Equal(t, protomodel.FieldType_INTEGER, resp.Fields[1].Type)

	// add document to collection
	docRes, err := db.InsertDocument(context.Background(), &protomodel.DocumentInsertRequest{
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
	reader, err := db.SearchDocuments(context.Background(), &protomodel.Query{
		Collection: collectionName,
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

	doc := revision.Document

	require.Equal(t, 123.0, doc.Fields["pincode"].GetNumberValue())

	proofRes, err := db.DocumentProof(context.Background(), &protomodel.DocumentProofRequest{
		Collection: collectionName,
		DocumentId: docRes.DocumentId,
	})
	require.NoError(t, err)
	require.NotNil(t, proofRes)

	newState, err := verification.VerifyDocument(context.Background(), proofRes, doc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, newState.TxId)
}
