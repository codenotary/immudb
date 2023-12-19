/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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

	authorizationClient "github.com/codenotary/immudb/test/documents_storage_tests/immudbhttpclient/immudbauth"

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
