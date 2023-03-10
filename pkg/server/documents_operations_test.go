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

	"github.com/codenotary/immudb/pkg/api/authorizationschema"
	"github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"
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

	_, err := s.DocumentInsert(ctx, &documentschema.DocumentInsertRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &documentschema.DocumentSearchRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &documentschema.CollectionCreateRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &documentschema.CollectionDeleteRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &documentschema.CollectionListRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &documentschema.CollectionGetRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	logged, err := s.OpenSessionV2(ctx, &authorizationschema.OpenSessionRequestV2{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, logged.Token)
	fmt.Println(logged.ExpirationTimestamp)
	assert.True(t, logged.InactivityTimestamp > 0)
	assert.True(t, logged.ExpirationTimestamp >= 0)
	assert.True(t, len(logged.ServerUUID) > 0)

	md := metadata.Pairs("sessionid", logged.Token)
	ctx = metadata.NewIncomingContext(context.Background(), md)
	_, err = s.DocumentInsert(ctx, &documentschema.DocumentInsertRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &documentschema.DocumentSearchRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &documentschema.CollectionCreateRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &documentschema.CollectionDeleteRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &documentschema.CollectionListRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &documentschema.CollectionGetRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

}
