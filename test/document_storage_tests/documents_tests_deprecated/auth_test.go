/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

	"github.com/codenotary/immudb/pkg/api/httpclient"

	"github.com/stretchr/testify/require"
)

func TestAuthentication(t *testing.T) {
	client := getClient()

	badLogin := "immudbXXX"
	badPassword := "immudbXXX"
	badDatabase := "defaultdbXXX"

	response, err := client.OpenSessionWithResponse(context.Background(), httpclient.OpenSessionJSONRequestBody{
		Username: &badLogin,
		Password: &badPassword,
		Database: &badDatabase,
	})
	require.NoError(t, err)
	require.True(t, *response.JSONDefault.Message == "invalid user name or password")

	defaultLogin := "immudb"
	defaultPassword := "immudb"
	defaultDatabase := "defaultdb"

	response, err = client.OpenSessionWithResponse(context.Background(), httpclient.OpenSessionJSONRequestBody{
		Username: &defaultLogin,
		Password: &defaultPassword,
		Database: &defaultDatabase,
	})
	require.NoError(t, err)
	require.True(t, response.StatusCode() == 200)
	require.True(t, len(*response.JSON200.SessionID) > 0)
}
