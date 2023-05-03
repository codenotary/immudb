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

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/stretchr/testify/assert"
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
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &protomodel.CollectionDeleteRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &protomodel.CollectionListRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &protomodel.CollectionGetRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	authServiceImp := &authenticationServiceImp{server: s}

	logged, err := authServiceImp.OpenSession(ctx, &protomodel.OpenSessionRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, logged.SessionID)
	fmt.Println(logged.ExpirationTimestamp)
	assert.True(t, logged.InactivityTimestamp > 0)
	assert.True(t, logged.ExpirationTimestamp >= 0)
	assert.True(t, len(logged.ServerUUID) > 0)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &protomodel.CollectionCreateRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &protomodel.CollectionDeleteRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &protomodel.CollectionListRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &protomodel.CollectionGetRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

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
	assert.NoError(t, err)
	assert.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// create collection
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

	// add documents to collection
	for i := 1.0; i <= 20; i++ {
		_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{
			Collection: collectionName,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": {
						Kind: &structpb.Value_NumberValue{NumberValue: i},
					},
					"country": {
						Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("country-%d", int(i))},
					},
					"idx": {
						Kind: &structpb.Value_NumberValue{NumberValue: i},
					},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test reader for multiple paginated reads", func(t *testing.T) {
		results := make([]*protomodel.DocumentAtRevision, 0)

		var searchID string
		for i := 1; i <= 4; i++ {
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				Collection: collectionName,
				Query: &protomodel.Query{
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
				PerPage:  5,
				SearchID: searchID,
			})
			require.NoError(t, err)
			require.Equal(t, 5, len(resp.Revisions))
			results = append(results, resp.Revisions...)
			searchID = resp.SearchID
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

		t.Run("test reader should throw no more entries when reading more entries", func(t *testing.T) {
			_, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				Collection: collectionName,
				Query: &protomodel.Query{
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
				Page:     5,
				PerPage:  5,
				SearchID: searchID,
			})
			require.ErrorIs(t, err, document.ErrNoMoreDocuments)
		})
	})

	t.Run("test reader should throw error on reading backwards", func(t *testing.T) {

		var searchID string
		for i := 1; i <= 3; i++ {
			resp, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
				Collection: collectionName,
				Query: &protomodel.Query{
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
				PerPage:  5,
				SearchID: searchID,
			})
			require.NoError(t, err)
			require.Equal(t, 5, len(resp.Revisions))
			searchID = resp.SearchID
		}

		_, err := s.DocumentSearch(ctx, &protomodel.DocumentSearchRequest{
			Collection: collectionName,
			Query: &protomodel.Query{
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
			Page:     2, // read upto page 3, check if we can read backwards
			PerPage:  5,
			SearchID: searchID,
		})

		require.ErrorIs(t, err, ErrInvalidPreviousPage)
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
	assert.NoError(t, err)
	assert.NotEmpty(t, logged.SessionID)

	md := metadata.Pairs("sessionid", logged.SessionID)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// create collection
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

	// add documents to collection
	for i := 1.0; i <= 20; i++ {
		_, err = s.DocumentInsert(ctx, &protomodel.DocumentInsertRequest{
			Collection: collectionName,
			Document: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"pincode": {
						Kind: &structpb.Value_NumberValue{NumberValue: i},
					},
					"country": {
						Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("country-%d", int(i))},
					},
					"idx": {
						Kind: &structpb.Value_NumberValue{NumberValue: i},
					},
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
				Collection: collectionName,
				Query: &protomodel.Query{
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
				Page:    uint32(i),
				PerPage: 5,
			})
			require.NoError(t, err)
			require.Equal(t, 5, len(resp.Revisions))
			results = append(results, resp.Revisions...)
		}

		for i := 1.0; i <= 20; i++ {
			docAtRev := results[int(i-1)]
			require.Equal(t, i, docAtRev.Document.Fields["idx"].GetNumberValue())
		}

		require.Equal(t, 4, sess.GetPaginatedDocumentReadersCount())
	})

}
