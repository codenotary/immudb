package main

import (
	"context"
	"testing"

	"github.com/antihax/optional"
	apiclient "github.com/codenotary/immudb/test/objectstorage_tests/go-client"

	"github.com/stretchr/testify/assert"
)

func TestCreateCollection(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	primaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	primaryKeysMap["uuid"] = apiclient.STRING_
	indexKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	resp, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.Schemav2CollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, resp.Name, collection)
	assert.Equal(t, resp.PrimaryKeys, primaryKeysMap)
	assert.Empty(t, resp.IndexKeys)
}

// Assumption here is to automate creation of a collection with default primary key
func TestCreateCollectionWithoutPrimaryIndex(t *testing.T) {

	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	primaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	indexKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	resp, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.Schemav2CollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	expectedPrimaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	expectedPrimaryKeysMap["_uuid"] = apiclient.STRING_
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, resp.Name, collection)
	assert.Equal(t, resp.PrimaryKeys, expectedPrimaryKeysMap)
	assert.Empty(t, resp.IndexKeys)
}

func TestCreateCollectionWithIndexes(t *testing.T) {

	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	primaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	indexKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	indexKeysMap["name"] = apiclient.STRING_
	resp, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.Schemav2CollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	expectedPrimaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	expectedPrimaryKeysMap["_uuid"] = apiclient.STRING_
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, resp.Name, collection)
	assert.Equal(t, resp.PrimaryKeys, expectedPrimaryKeysMap)
	assert.Equal(t, resp.IndexKeys, indexKeysMap)
}

func TestCreateCollectionWithDifferentIndexes(t *testing.T) {

	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	primaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	indexKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	indexKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	indexKeysMap["height"] = apiclient.DOUBLE
	primaryKeysMap["uuid"] = apiclient.STRING_
	resp, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.Schemav2CollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, resp.Name, collection)
	assert.Equal(t, resp.PrimaryKeys, primaryKeysMap)
	assert.Equal(t, resp.IndexKeys, indexKeysMap)
}

func TestDeleteCollection(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)

	getResponse, http, err := client.CollectionsApi.ImmuServiceV2CollectionGet(context.Background(), &apiclient.CollectionsApiImmuServiceV2CollectionGetOpts{
		Name: optional.NewString(collection),
	})
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, getResponse.Name, collection)

	deletionResponse, http, err := client.CollectionsApi.ImmuServiceV2CollectionDelete(context.Background(), &apiclient.CollectionsApiImmuServiceV2CollectionDeleteOpts{
		Name: optional.NewString(collection),
	})

	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	// Google protobuf empty on success
	assert.Empty(t, deletionResponse)

	getResponse, http, err = client.CollectionsApi.ImmuServiceV2CollectionGet(context.Background(), &apiclient.CollectionsApiImmuServiceV2CollectionGetOpts{
		Name: optional.NewString(collection),
	})
	assert.True(t, err != nil)
	assert.Equal(t, http.StatusCode, 404)

}

func TestGetCollection(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)
	getResponse, http, err := client.CollectionsApi.ImmuServiceV2CollectionGet(context.Background(), &apiclient.CollectionsApiImmuServiceV2CollectionGetOpts{
		Name: optional.NewString(collection),
	})
	expectedPrimaryKeysMap := make(map[string]apiclient.Schemav2PossibleIndexType)
	expectedPrimaryKeysMap["_uuid"] = apiclient.STRING_
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, getResponse.Name, collection)
	assert.Equal(t, getResponse.PrimaryKeys, expectedPrimaryKeysMap)
	assert.Empty(t, getResponse.IndexKeys)
}

func TestListCollections(t *testing.T) {
	client := GetAuthorizedClient()
	collectionNameToFind := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collectionNameToFind)

	collectionList, http, err := client.CollectionsApi.ImmuServiceV2CollectionList(context.Background(), apiclient.Schemav2CollectionListRequest{})
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	collectionFound := false
	for _, collection := range collectionList.Collections {
		if collection.Name == collectionNameToFind {
			collectionFound = true
			break
		}
	}
	assert.True(t, collectionFound)

	collectionNameToFind = GetStandarizedRandomString()
	CreateStandardTestCollection(client, collectionNameToFind)

	collectionList, http, err = client.CollectionsApi.ImmuServiceV2CollectionList(context.Background(), apiclient.Schemav2CollectionListRequest{})
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	collectionFound = false
	for _, collection := range collectionList.Collections {
		if collection.Name == collectionNameToFind {
			collectionFound = true
			break
		}
	}
	assert.True(t, collectionFound)

}
