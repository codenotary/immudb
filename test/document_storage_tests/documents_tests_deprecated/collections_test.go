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

	"github.com/codenotary/immudb/test/documents_storage_tests/immudbhttpclient/immudbdocuments"
	"github.com/stretchr/testify/assert"
)

func TestCreateCollection(t *testing.T) {
	client := getAuthorizedDocumentsClient()
	collection := GetStandarizedRandomString()
	indexKeys := make(map[string]immudbdocuments.DocumentschemaIndexOption)
	primaryKeys := make(map[string]immudbdocuments.DocumentschemaIndexOption)
	stringType := immudbdocuments.STRING
	primaryKeys["test"] = immudbdocuments.DocumentschemaIndexOption{
		Type: &stringType,
	}
	req := immudbdocuments.DocumentServiceCollectionCreateJSONRequestBody{
		IndexKeys:   &indexKeys,
		PrimaryKeys: &primaryKeys,
		Name:        &collection,
	}
	response, _ := client.DocumentServiceCollectionCreateWithResponse(context.Background(), req)
	assert.True(t, response.StatusCode() == 200)
	assert.True(t, *response.JSON200.Collection.Name == collection)

	response, _ = client.DocumentServiceCollectionCreateWithResponse(context.Background(), req)
	assert.True(t, response.JSONDefault.Error != nil)
}

func TestListCollections(t *testing.T) {
	client := getAuthorizedDocumentsClient()
	collectionName := CreateAndGetStandardTestCollection(client)

	response, _ := client.DocumentServiceCollectionListWithResponse(context.Background(), immudbdocuments.DocumentServiceCollectionListJSONRequestBody{})
	assert.True(t, response.StatusCode() == 200)
	collectionFound := false
	for _, collection := range *response.JSON200.Collections {
		if *collection.Name == collectionName {
			collectionFound = true
			assert.True(t, collection.PrimaryKeys != nil)
			assert.True(t, collection.IndexKeys == nil)
			break
		}
	}
	assert.True(t, collectionFound)
}

func TestGetCollection(t *testing.T) {
	client := getAuthorizedDocumentsClient()
	collectionName := CreateAndGetStandardTestCollection(client)

	response, _ := client.DocumentServiceCollectionGetWithResponse(context.Background(), &immudbdocuments.DocumentServiceCollectionGetParams{
		Name: &collectionName,
	})
	assert.True(t, response.StatusCode() == 200)
	assert.True(t, *response.JSON200.Collection.Name == collectionName)
}
