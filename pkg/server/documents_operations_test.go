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

	_, err := s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &protomodel.CollectionDeleteRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &protomodel.CollectionListRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &protomodel.CollectionGetRequest{})
	require.ErrorIs(t, err, ErrNotLoggedIn)

	authServiceImp := &authenticationServiceImp{server: s}

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
	_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &protomodel.CollectionDeleteRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &protomodel.CollectionListRequest{})
	require.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &protomodel.CollectionGetRequest{})
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

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{
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
		_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{
			Collection: collectionName,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": structpb.NewNumberValue(i),
					"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
					"idx":     structpb.NewNumberValue(i),
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test with search id and query should fail", func(t *testing.T) {
		_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
			SearchId: "foobar",
			Query: &protomodel.Query{
				Collection: collectionName,
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

	t.Run("test with invalid search id should fail", func(t *testing.T) {
		_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
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
			Collection: collectionName,
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
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 5,
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
		require.Equal(t, 1, sess.GetPaginatedDocumentReadersCount())

		t.Run("test reader should throw no more entries when reading more entries from a reader", func(t *testing.T) {
			_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
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
			Collection: collectionName,
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
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 5,
			})
			require.NoError(t, err)
			require.Len(t, resp.Revisions, 5)
			searchID = resp.SearchId
			query = nil
		}

		_, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
			SearchId: searchID,
			Page:     2, // read upto page 3, check if we can read backwards
			PageSize: 5,
		})
		require.NoError(t, err)
	})
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

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{
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
		_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{
			Collection: collectionName,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": structpb.NewNumberValue(i),
					"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
					"idx":     structpb.NewNumberValue(i),
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test reader for multiple paginated reads without search ID should have multiple readers", func(t *testing.T) {
		sessionID, err := sessions.GetSessionIDFromContext(ctx)
		require.NoError(t, err)

		sess, err := s.SessManager.GetSession(sessionID)
		require.NoError(t, err)

		results := make([]*protomodel.DocumentAtRevision, 0)

		for i := 1; i <= 4; i++ {
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				Query: &protomodel.Query{
					Collection: collectionName,
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

		require.Equal(t, 4, sess.GetPaginatedDocumentReadersCount())
	})
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

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{
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
		_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{
			Collection: collectionName,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": structpb.NewNumberValue(i),
					"country": structpb.NewStringValue(fmt.Sprintf("country-%d", int(i))),
					"idx":     structpb.NewNumberValue(i),
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test reader with multiple paginated reads", func(t *testing.T) {
		results := make([]*protomodel.DocumentAtRevision, 0)

		var searchID string

		query := &protomodel.Query{
			Collection: collectionName,
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
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				SearchId: searchID,
				Query:    query,
				Page:     uint32(i),
				PageSize: 4,
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
		require.Equal(t, 1, sess.GetPaginatedDocumentReadersCount())

		t.Run("test reader should throw no more entries when reading more entries from a reader", func(t *testing.T) {
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
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
			Collection: collectionName,
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

		resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
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
		require.Equal(t, 0, sess.GetPaginatedDocumentReadersCount())

		t.Run("test reader should throw error when search id is invalid", func(t *testing.T) {
			_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				SearchId: "invalid-searchId",
				Page:     2,
				PageSize: 5,
			})
			require.ErrorIs(t, err, sessions.ErrPaginatedDocumentReaderNotFound)
		})

	})

}
