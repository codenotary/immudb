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
	"github.com/stretchr/testify/require"
)

func TestCreateCollection(t *testing.T) {
	client := getAuthorizedClient()

	collection, err := createRandomCollection(client)
	require.NoError(t, err)
	require.NotNil(t, collection)
}

func TestGetCollection(t *testing.T) {
	client := getAuthorizedClient()

	collection, err := createRandomCollection(client)
	require.NoError(t, err)

	response, err := client.CollectionGetWithResponse(context.Background(), &httpclient.CollectionGetParams{
		Name: collection.Name,
	})
	require.NoError(t, err)
	require.True(t, response.StatusCode() == 200)
	require.True(t, *response.JSON200.Collection.Name == *collection.Name)
}

func TestListCollections(t *testing.T) {
	client := getAuthorizedClient()

	newCollection, err := createRandomCollection(client)
	require.NoError(t, err)

	response, _ := client.CollectionListWithResponse(context.Background(), httpclient.CollectionListJSONRequestBody{})
	require.True(t, response.StatusCode() == 200)

	collectionFound := false

	for _, collection := range *response.JSON200.Collections {
		if *collection.Name == *newCollection.Name {
			collectionFound = true
			break
		}
	}

	require.True(t, collectionFound)
}
