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

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestV2Authentication(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)

	s.Initialize()

	ctx := context.Background()

	_, err := s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.UpdateCollection(ctx, &protomodel.UpdateCollectionRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.GetCollection(ctx, &protomodel.GetCollectionRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.GetCollections(ctx, &protomodel.GetCollectionsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DeleteCollection(ctx, &protomodel.DeleteCollectionRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.AddField(ctx, &protomodel.AddFieldRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.RemoveField(ctx, &protomodel.RemoveFieldRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CreateIndex(ctx, &protomodel.CreateIndexRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DeleteIndex(ctx, &protomodel.DeleteIndexRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.ReplaceDocuments(ctx, &protomodel.ReplaceDocumentsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.AuditDocument(ctx, &protomodel.AuditDocumentRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CountDocuments(ctx, &protomodel.CountDocumentsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DeleteDocuments(ctx, &protomodel.DeleteDocumentsRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.ProofDocument(ctx, &protomodel.ProofDocumentRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	authServiceImp := &authenticationServiceImp{server: s}

	_, err = authServiceImp.KeepAlive(context.Background(), &protomodel.KeepAliveRequest{})
	require.Error(t, err)

	_, err = authServiceImp.CloseSession(context.Background(), &protomodel.CloseSessionRequest{})
	require.Error(t, err)

	_, err = authServiceImp.OpenSession(ctx, &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "wrongPassword",
		Database: "defaultdb",
	})
	require.Error(t, err)

	logged, err := authServiceImp.OpenSession(ctx, &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)
	require.True(t, logged.InactivityTimestamp > 0)
	require.True(t, logged.ExpirationTimestamp >= 0)
	require.True(t, len(logged.ServerUUID) > 0)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx = metadata.NewIncomingContext(context.Background(), md)

	_, err = authServiceImp.KeepAlive(ctx, &protomodel.KeepAliveRequest{})
	require.NoError(t, err)

	_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DeleteCollection(ctx, &protomodel.DeleteCollectionRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.GetCollections(ctx, &protomodel.GetCollectionsRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.GetCollection(ctx, &protomodel.GetCollectionRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)
}

func TestPaginationOnReader(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authenticationServiceImp := &authenticationServiceImp{s}

	logged, err := authenticationServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	collectionName := "mycollection"

	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
		Name: collectionName,
		Fields: []*protomodel.Field{
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "idx", Type: protomodel.FieldType_INTEGER},
		},
		Indexes: []*protomodel.Index{
			{Fields: []string{"pincode"}},
			{Fields: []string{"country"}},
			{Fields: []string{"idx"}},
		},
	})
	require.NoError(t, err)

	for i := 1.0; i <= 20; i++ {
		_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"pincode": structpb.NewNumberValue(i),
						"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
						"idx":     structpb.NewNumberValue(i),
					},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test with search id and query should fail", func(t *testing.T) {
		_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
			SearchId: "foobar",
			Query: &protomodel.Query{
				CollectionName: collectionName,
				Expressions: []*protomodel.QueryExpression{
					{
						FieldComparisons: []*protomodel.FieldComparison{
							{
								Field:    "pincode",
								Operator: protomodel.ComparisonOperator_GE,
								Value:    structpb.NewNumberValue(0),
							},
						},
					},
				},
			},
			Page:     1,
			PageSize: 5,
		})
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	_, err = s.SearchDocuments(ctx, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	t.Run("test with invalid search id should fail", func(t *testing.T) {
		_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
			SearchId: "foobar",
			Page:     1,
			PageSize: 5,
		})
		require.ErrorIs(t, err, sessions.ErrPaginatedDocumentReaderNotFound)
	})

	t.Run("test reader for multiple paginated reads", func(t *testing.T) {
		results := make([]*protomodel.DocumentAtRevision, 0)

		var searchID string

		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(0),
						},
					},
				},
			},
		}

		for i := 1; i <= 4; i++ {
			resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 5,
				KeepOpen: true,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 5)
			results = append(results, resp.Revisions...)
			searchID = resp.SearchId
			query = nil
		}

		for i := 1.0; i <= 20; i++ {
			docAtRev := results[int(i-1)]
			require.Equal(t, i, docAtRev.Document.Fields["idx"].GetNumberValue())
		}

		// ensure there is only one reader in the session for the request and it is being reused
		// get the session from the context
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		require.NoError(t, err)

		sess, err := s.SessManager.GetSession(sessionID)
		require.NoError(t, err)
		require.Equal(t, 1, sess.GetDocumentReadersCount())

		t.Run("test reader should throw no more entries when reading more entries from a reader", func(t *testing.T) {
			_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: searchID,
				Page:     5,
				PageSize: 5,
			})
			require.NoError(t, err)
		})
	})

	t.Run("test reader should throw error on reading backwards", func(t *testing.T) {
		var searchID string

		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(0),
						},
					},
				},
			},
		}

		for i := 1; i <= 3; i++ {
			resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 5,
				KeepOpen: true,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 5)
			searchID = resp.SearchId
			query = nil
		}

		_, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
			SearchId: searchID,
			Page:     2, // read upto page 3, check if we can read backwards
			PageSize: 5,
		})
		require.NoError(t, err)
	})

	// close session and ensure that all paginated readers are closed
	_, err = authenticationServiceImp.CloseSession(ctx, &protomodel.CloseSessionRequest{})
	require.NoError(t, err)
}

func TestPaginationWithoutSearchID(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authServiceImp := &authenticationServiceImp{server: s}

	logged, err := authServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	collectionName := "mycollection"

	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
		Name: collectionName,
		Fields: []*protomodel.Field{
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "idx", Type: protomodel.FieldType_INTEGER},
		},
		Indexes: []*protomodel.Index{
			{Fields: []string{"pincode"}},
			{Fields: []string{"country"}},
			{Fields: []string{"idx"}},
		},
	})
	require.NoError(t, err)

	for i := 1.0; i <= 20; i++ {
		_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"pincode": structpb.NewNumberValue(i),
						"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
						"idx":     structpb.NewNumberValue(i),
					},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test reader for multiple paginated reads without search ID should have no open readers", func(t *testing.T) {
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		require.NoError(t, err)

		sess, err := s.SessManager.GetSession(sessionID)
		require.NoError(t, err)

		results := make([]*protomodel.DocumentAtRevision, 0)

		for i := 1; i <= 4; i++ {
			resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				Query: &protomodel.Query{
					CollectionName: collectionName,
					Expressions: []*protomodel.QueryExpression{
						{
							FieldComparisons: []*protomodel.FieldComparison{
								{
									Field:    "pincode",
									Operator: protomodel.ComparisonOperator_GE,
									Value:    structpb.NewNumberValue(0),
								},
							},
						},
					},
				},
				Page:     uint32(i),
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 5)
			results = append(results, resp.Revisions...)
		}

		for i := 1.0; i <= 20; i++ {
			docAtRev := results[int(i-1)]
			require.Equal(t, i, docAtRev.Document.Fields["idx"].GetNumberValue())
		}

		require.Zero(t, sess.GetDocumentReadersCount())
	})

	// close session and ensure that all paginated readers are closed
	_, err = authServiceImp.CloseSession(ctx, &protomodel.CloseSessionRequest{})
	require.NoError(t, err)
}

func TestPaginatedReader_NoMoreDocsFound(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authenticationServiceImp := &authenticationServiceImp{s}

	logged, err := authenticationServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	collectionName := "mycollection"

	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
		Name: collectionName,
		Fields: []*protomodel.Field{
			{Name: "pincode", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "idx", Type: protomodel.FieldType_INTEGER},
		},
		Indexes: []*protomodel.Index{
			{Fields: []string{"pincode"}},
			{Fields: []string{"country"}},
			{Fields: []string{"idx"}},
		},
	})
	require.NoError(t, err)

	for i := 1.0; i <= 10; i++ {
		_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"pincode": structpb.NewNumberValue(i),
						"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
						"idx":     structpb.NewNumberValue(i),
					},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("document count without conditions should return the total number of documents", func(t *testing.T) {
		resp, err := s.CountDocuments(ctx, &protomodel.CountDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: collectionName,
			},
		})
		require.NoError(t, err)
		require.EqualValues(t, 10, resp.Count)
	})

	t.Run("test reader with multiple paginated reads", func(t *testing.T) {
		results := make([]*protomodel.DocumentAtRevision, 0)

		var searchID string

		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(0),
						},
					},
				},
			},
		}

		for i := 1; i <= 2; i++ {
			resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 4,
				KeepOpen: true,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 4)
			results = append(results, resp.Revisions...)
			searchID = resp.SearchId
			query = nil
		}

		// ensure there is only one reader in the session for the request and it is being reused
		// get the session from the context
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		require.NoError(t, err)

		sess, err := s.SessManager.GetSession(sessionID)
		require.NoError(t, err)
		require.Equal(t, 1, sess.GetDocumentReadersCount())

		t.Run("test reader should throw no more entries when reading more entries from a reader", func(t *testing.T) {
			resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: searchID,
				Page:     3,
				PageSize: 4,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 2)
			results = append(results, resp.Revisions...)
		})

		for i := 1.0; i <= 10; i++ {
			docAtRev := results[int(i-1)]
			require.Equal(t, i, docAtRev.Document.Fields["idx"].GetNumberValue())
		}
	})

	t.Run("test reader with single read", func(t *testing.T) {
		query := &protomodel.Query{
			CollectionName: collectionName,
			Expressions: []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						{
							Field:    "pincode",
							Operator: protomodel.ComparisonOperator_GE,
							Value:    structpb.NewNumberValue(0),
						},
					},
				},
			},
		}

		resp, err := s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
			Query:    query,
			Page:     1,
			PageSize: 11,
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 10)
		require.Len(t, resp.SearchId, 0)

		// ensure there is only one reader in the session for the request and it is being reused
		// get the session from the context
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		require.NoError(t, err)

		sess, err := s.SessManager.GetSession(sessionID)
		require.NoError(t, err)
		require.Equal(t, 0, sess.GetDocumentReadersCount())

		t.Run("test reader should throw error when search id is invalid", func(t *testing.T) {
			_, err = s.SearchDocuments(ctx, &protomodel.SearchDocumentsRequest{
				SearchId: "invalid-searchId",
				Page:     2,
				PageSize: 5,
			})
			require.ErrorIs(t, err, sessions.ErrPaginatedDocumentReaderNotFound)
		})

	})

	t.Run("document deletion should succeed", func(t *testing.T) {
		_, err = s.DeleteDocuments(ctx, &protomodel.DeleteDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: collectionName,
			},
		})
		require.NoError(t, err)
	})

	// close session and ensure that all paginated readers are closed
	_, err = authenticationServiceImp.CloseSession(ctx, &protomodel.CloseSessionRequest{})
	require.NoError(t, err)
}

func TestDocumentInsert_WithEmptyDocument(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authenticationServiceImp := &authenticationServiceImp{s}

	logged, err := authenticationServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	collectionName := "employees"

	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
		Name:                collectionName,
		DocumentIdFieldName: "emp_no",
		Fields: []*protomodel.Field{
			{Name: "birth_date", Type: protomodel.FieldType_STRING},
			{Name: "first_name", Type: protomodel.FieldType_STRING},
			{Name: "last_name", Type: protomodel.FieldType_STRING},
			{Name: "gender", Type: protomodel.FieldType_STRING},
			{Name: "hire_date", Type: protomodel.FieldType_STRING},
		},
		Indexes: []*protomodel.Index{
			{Fields: []string{"last_name"}},
		},
	})
	require.NoError(t, err)

	t.Run("#272: insert with empty document should not panic", func(t *testing.T) {
		_, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents:      []*structpb.Struct{{}},
		})
		require.NoError(t, err)
	})
}

func TestCollections(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authenticationServiceImp := &authenticationServiceImp{s}

	logged, err := authenticationServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// create collection
	defaultCollectionName := "mycollection"

	t.Run("should pass when creating a collection", func(t *testing.T) {
		_, err := s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
			Name: defaultCollectionName,
			Fields: []*protomodel.Field{
				{Name: "number", Type: protomodel.FieldType_INTEGER},
				{Name: "name", Type: protomodel.FieldType_STRING},
				{Name: "pin", Type: protomodel.FieldType_INTEGER},
				{Name: "country", Type: protomodel.FieldType_STRING},
			},
		})
		require.NoError(t, err)

		_, err = s.AddField(ctx, &protomodel.AddFieldRequest{
			CollectionName: defaultCollectionName,
			Field: &protomodel.Field{
				Name: "extra_field",
				Type: protomodel.FieldType_UUID,
			},
		})
		require.NoError(t, err)

		_, err = s.AddField(ctx, &protomodel.AddFieldRequest{
			CollectionName: defaultCollectionName,
			Field: &protomodel.Field{
				Name: "extra_field1",
				Type: protomodel.FieldType_STRING,
			},
		})
		require.NoError(t, err)

		_, err = s.RemoveField(ctx, &protomodel.RemoveFieldRequest{
			CollectionName: defaultCollectionName,
			FieldName:      "extra_field1",
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := s.GetCollection(ctx, &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		expectedFieldKeys := []*protomodel.Field{
			{Name: "_id", Type: protomodel.FieldType_STRING},
			{Name: "number", Type: protomodel.FieldType_INTEGER},
			{Name: "name", Type: protomodel.FieldType_STRING},
			{Name: "pin", Type: protomodel.FieldType_INTEGER},
			{Name: "country", Type: protomodel.FieldType_STRING},
			{Name: "extra_field", Type: protomodel.FieldType_UUID},
		}

		collection := cinfo.Collection

		for i, idxType := range expectedFieldKeys {
			require.Equal(t, idxType.Name, collection.Fields[i].Name)
			require.Equal(t, idxType.Type, collection.Fields[i].Type)
		}

	})

	t.Run("should pass when adding an index to the collection", func(t *testing.T) {
		_, err := s.CreateIndex(ctx, &protomodel.CreateIndexRequest{
			CollectionName: defaultCollectionName,
			Fields:         []string{"number"},
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := s.GetCollection(ctx, &protomodel.GetCollectionRequest{
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
		_, err := s.DeleteIndex(ctx, &protomodel.DeleteIndexRequest{
			CollectionName: defaultCollectionName,
			Fields:         []string{"number"},
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := s.GetCollection(ctx, &protomodel.GetCollectionRequest{
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
		_, err := s.UpdateCollection(ctx, &protomodel.UpdateCollectionRequest{
			Name:                defaultCollectionName,
			DocumentIdFieldName: "foo",
		})
		require.NoError(t, err)

		// get collection
		cinfo, err := s.GetCollection(ctx, &protomodel.GetCollectionRequest{
			Name: defaultCollectionName,
		})
		require.NoError(t, err)

		collection := cinfo.Collection
		require.Equal(t, "foo", collection.Fields[0].Name)
	})

	t.Run("should pass when deleting collection", func(t *testing.T) {
		_, err := s.DeleteCollection(ctx, &protomodel.DeleteCollectionRequest{
			Name: "mycollection",
		})
		require.NoError(t, err)

		resp, err := s.GetCollections(ctx, &protomodel.GetCollectionsRequest{})
		require.NoError(t, err)

		require.Len(t, resp.Collections, 0)
	})

	t.Run("should pass when creating multiple collections", func(t *testing.T) {
		// create collection
		collections := []string{"mycollection1", "mycollection2", "mycollection3"}

		for _, collectionName := range collections {
			_, err := s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
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
		resp, err := s.GetCollections(ctx, &protomodel.GetCollectionsRequest{})
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

func TestDocuments(t *testing.T) {
	dir := t.TempDir()

	serverOptions := DefaultOptions().
		WithDir(dir).
		WithPort(0).
		WithMetricsServer(false).
		WithAdminPassword(auth.SysAdminPassword).
		WithSigningKey("./../../test/signer/ec1.key")

	s := DefaultServer().WithOptions(serverOptions).(*ImmuServer)
	require.NoError(t, s.Initialize())

	authenticationServiceImp := &authenticationServiceImp{s}

	logged, err := authenticationServiceImp.OpenSession(context.Background(), &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// create collection
	collectionName := "mycollection"
	_, err = s.CreateCollection(ctx, &protomodel.CreateCollectionRequest{
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
		_, err := s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
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
		res, err = s.InsertDocuments(ctx, &protomodel.InsertDocumentsRequest{
			CollectionName: collectionName,
			Documents: []*structpb.Struct{
				{
					Fields: map[string]*structpb.Value{
						"pincode": structpb.NewNumberValue(123),
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, res.DocumentIds, 1)
		docID = res.DocumentIds[0]
	})

	time.Sleep(100 * time.Millisecond)

	t.Run("should pass when auditing document", func(t *testing.T) {
		resp, err := s.AuditDocument(ctx, &protomodel.AuditDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
			Page:           1,
			PageSize:       10,
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 1)

		for _, rev := range resp.Revisions {
			require.Equal(t, docID, rev.Document.Fields["_id"].GetStringValue())
		}
	})

	t.Run("should pass when replacing document", func(t *testing.T) {
		resp, err := s.ReplaceDocuments(ctx, &protomodel.ReplaceDocumentsRequest{
			Query: &protomodel.Query{
				CollectionName: collectionName,
				Limit:          1,
			},
			Document: &structpb.Struct{Fields: map[string]*structpb.Value{
				"pincode": structpb.NewNumberValue(321),
			}},
		})
		require.NoError(t, err)
		require.Len(t, resp.Revisions, 1)
	})

	t.Run("should pass when requesting document proof", func(t *testing.T) {
		_, err := s.ProofDocument(ctx, &protomodel.ProofDocumentRequest{
			CollectionName: collectionName,
			DocumentId:     docID,
		})
		require.NoError(t, err)
	})

}
