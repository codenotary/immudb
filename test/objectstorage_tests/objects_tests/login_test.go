package main

import (
	"context"
	"testing"

	authorizationClient "github.com/codenotary/immudb/test/objectstorage_tests/immudbhttpclient/immudbauth"

	"github.com/stretchr/testify/assert"
)

func TestLogin(t *testing.T) {
	authClient := getAuthorizationClient()

	badLogin := "immudbXXX"
	badPassword := "immudbXXX"
	badDatabase := "defaultdbXXX"

	defaultLogin := "immudb"
	defaultPassword := "immudb"
	defaultDatabase := "defaultdb"
	response, _ := authClient.AuthorizationServiceOpenSessionV2WithResponse(context.Background(), authorizationClient.AuthorizationServiceOpenSessionV2JSONRequestBody{
		Username: &badLogin,
		Password: &badPassword,
		Database: &badDatabase,
	})
	assert.True(t, *response.JSONDefault.Message == "invalid user name or password")

	response, _ = authClient.AuthorizationServiceOpenSessionV2WithResponse(context.Background(), authorizationClient.AuthorizationServiceOpenSessionV2JSONRequestBody{
		Username: &defaultLogin,
		Password: &defaultPassword,
		Database: &defaultDatabase,
	})
	assert.True(t, response.StatusCode() == 200)
	assert.True(t, len(*response.JSON200.Token) > 0)
}
