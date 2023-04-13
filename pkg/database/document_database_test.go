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

	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/verification"
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

func TestDocumentDB_Collection(t *testing.T) {
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
	rawDoc := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 123},
			},
		},
	}

	docRes, err := db.InsertDocument(context.Background(), &schemav2.DocumentInsertRequest{
		Collection: collectionName,
		Document:   rawDoc,
	})
	require.NoError(t, err)
	require.NotNil(t, docRes)

	// query collection for document
	docs, err := db.SearchDocuments(context.Background(), &schemav2.DocumentSearchRequest{
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
	require.Equal(t, 123.0, res.Fields["pincode"].GetNumberValue())

	proofRes, err := db.DocumentProof(context.Background(), &schemav2.DocumentProofRequest{
		Collection: collectionName,
		DocumentId: docRes.DocumentId,
	})
	require.NoError(t, err)
	require.NotNil(t, proofRes)

	newState, err := verification.VerifyDocument(context.Background(), proofRes, docRes.DocumentId, rawDoc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, proofRes.VerifiableTx.DualProof.TargetTxHeader.Id, newState.TxId)
}
