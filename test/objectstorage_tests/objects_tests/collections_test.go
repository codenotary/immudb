package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/codenotary/immudb/test/objectstorage_tests/immudbhttpclient/immudbdocuments"
	"github.com/gavv/httpexpect/v2"
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
