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
	"fmt"
	"net/http"
	"os"

	"github.com/codenotary/immudb/pkg/api/httpclient"

	"github.com/google/uuid"
)

var baseURL = GetEnv("DOCUMENTS_TEST_BASEURL", "http://localhost:8080/api/v2")

func GetEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}
func getStandarizedRandomString() string {
	return uuid.New().String()
}

func getClient() *httpclient.ClientWithResponses {
	client, err := httpclient.NewClientWithResponses(baseURL)
	if err != nil {
		panic(err)
	}
	return client
}

func getAuthorizedClient() *httpclient.ClientWithResponses {
	client := getClient()

	defaultLogin := "immudb"
	defaultPassword := "immudb"
	defaultDatabase := "defaultdb"

	response, err := client.OpenSessionWithResponse(context.Background(), httpclient.OpenSessionJSONRequestBody{
		Username: &defaultLogin,
		Password: &defaultPassword,
		Database: &defaultDatabase,
	})
	if err != nil {
		panic(err)
	}

	if response.StatusCode() != 200 {
		panic("Could not login")
	}

	authClient, err := httpclient.NewClientWithResponses(
		baseURL,
		httpclient.WithRequestEditorFn(
			func(ctx context.Context, req *http.Request) error {
				req.Header.Set("sessionid", *response.JSON200.SessionID)
				return nil
			},
		),
	)
	if err != nil {
		panic(err)
	}

	return authClient
}

func createCollection(collectionName string, client *httpclient.ClientWithResponses) error {
	req := httpclient.CollectionCreateJSONRequestBody{
		Name: &collectionName,
	}

	response, err := client.CollectionCreateWithResponse(context.Background(), req)
	if err != nil {
		return err
	}
	if response.StatusCode() != 200 {
		return fmt.Errorf("no 200 response: %d", response.StatusCode())
	}

	return nil
}
