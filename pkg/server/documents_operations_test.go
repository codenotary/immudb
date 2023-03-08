package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/api/authorizationschema"
	"github.com/codenotary/immudb/pkg/api/documentsschema"
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

	_, err := s.DocumentInsert(ctx, &documentsschema.DocumentInsertRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &documentsschema.DocumentSearchRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &documentsschema.CollectionCreateRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &documentsschema.CollectionDeleteRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &documentsschema.CollectionListRequest{})
	assert.ErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &documentsschema.CollectionGetRequest{})
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
	_, err = s.DocumentInsert(ctx, &documentsschema.DocumentInsertRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.DocumentSearch(ctx, &documentsschema.DocumentSearchRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionCreate(ctx, &documentsschema.CollectionCreateRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionDelete(ctx, &documentsschema.CollectionDeleteRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionList(ctx, &documentsschema.CollectionListRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

	_, err = s.CollectionGet(ctx, &documentsschema.CollectionGetRequest{})
	assert.NotErrorIs(t, err, ErrNotLoggedIn)

}
