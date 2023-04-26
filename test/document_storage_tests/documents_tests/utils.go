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
	"fmt"
	"net/http"
	"os"

	"github.com/codenotary/immudb/pkg/api/httpclient"

	"github.com/google/uuid"
)

var baseURL = GetEnv("DOCUMENTS_TEST_BASEURL", "http://localhost:8091/api/v2")

func GetEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return defaultValue
	}
	return value
}
func GetStandarizedRandomString() string {
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
				req.Header.Set("grpc-metadata-sessionid", *response.JSON200.SessionID)
				return nil
			},
		),
	)
	if err != nil {
		panic(err)
	}

	return authClient
}

func createRandomCollection(client *httpclient.ClientWithResponses) (*httpclient.ModelCollection, error) {
	collectionName := GetStandarizedRandomString()

	idField := "_docid"

	req := httpclient.CollectionCreateJSONRequestBody{
		Collection: &httpclient.ModelCollection{
			Name:        &collectionName,
			IdFieldName: &idField,
		},
	}

	response, err := client.CollectionCreateWithResponse(context.Background(), req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != 200 {
		return nil, fmt.Errorf("no 200 response: %d", response.StatusCode())
	}

	return response.JSON200.Collection, nil
}
