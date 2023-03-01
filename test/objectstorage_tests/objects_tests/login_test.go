package main

import (
	"context"
	"testing"
	"time"

	apiclient "github.com/codenotary/immudb/objectstester/go-client"
	"github.com/stretchr/testify/assert"
)

func TestBadLogin(t *testing.T) {
	client := GetObjectsClient()

	// Bad credentials, good database
	resp, http, err := client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "obviouslybad",
		Password: "usernamandpassword",
		Database: "defaultdb",
	})
	assert.Equal(t, http.StatusCode, 401)
	assert.True(t, err != nil)
	assert.True(t, resp.ExpirationTimestamp == 0)

	// Bad credentials, good database
	resp, http, err = client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "",
		Password: "",
		Database: "defaultdb",
	})
	assert.Equal(t, http.StatusCode, 401)
	assert.True(t, err != nil)
	assert.True(t, resp.ExpirationTimestamp == 0)

	// Good credentials, bad database
	resp, http, err = client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "obviouslydatabasethatdoesntexist",
	})
	assert.Equal(t, http.StatusCode, 403)
	assert.True(t, err != nil)
	assert.True(t, resp.ExpirationTimestamp == 0)

	// bad credentials, bad database
	resp, http, err = client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "obviouslydatabasethatdoesntexist",
		Password: "obviouslydatabasethatdoesntexist",
		Database: "obviouslydatabasethatdoesntexist",
	})
	assert.Equal(t, http.StatusCode, 401)
	assert.True(t, err != nil)
	assert.True(t, resp.ExpirationTimestamp == 0)
}

func TestProperLogin(t *testing.T) {
	client := GetObjectsClient()
	current := int32(time.Now().Unix())
	resp, http, err := client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	assert.Equal(t, http.StatusCode, 200)
	assert.True(t, err == nil)

	assert.True(t, resp.ExpirationTimestamp > current)
	assert.True(t, resp.Token != "" && len(resp.Token) > 0) // Token would be probably a session ID, need to decide still

}
