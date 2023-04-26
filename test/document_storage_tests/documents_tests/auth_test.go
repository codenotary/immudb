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

package main

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/httpclient"

	"github.com/stretchr/testify/assert"
)

func TestAuthentication(t *testing.T) {
	authClient := getAuthorizedClient()

	badLogin := "immudbXXX"
	badPassword := "immudbXXX"
	badDatabase := "defaultdbXXX"

	response, _ := authClient.OpenSessionWithResponse(context.Background(), httpclient.OpenSessionJSONRequestBody{
		Username: &badLogin,
		Password: &badPassword,
		Database: &badDatabase,
	})
	assert.True(t, *response.JSONDefault.Message == "invalid user name or password")

	defaultLogin := "immudb"
	defaultPassword := "immudb"
	defaultDatabase := "defaultdb"

	response, _ = authClient.OpenSessionWithResponse(context.Background(), httpclient.OpenSessionJSONRequestBody{
		Username: &defaultLogin,
		Password: &defaultPassword,
		Database: &defaultDatabase,
	})
	assert.True(t, response.StatusCode() == 200)
	assert.True(t, len(*response.JSON200.SessionID) > 0)
}
