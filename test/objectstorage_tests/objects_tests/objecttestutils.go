package main

import (
	"context"

	apiclient "github.com/codenotary/immudb/objectstester/go-client"

	"github.com/google/uuid"
)

var baseURL = "http://localhost:8091"

func GetObjectsClient() *apiclient.APIClient {

	config := apiclient.NewConfiguration()
	config.BasePath = baseURL
	client := apiclient.NewAPIClient(config)
	return client

}

func GetAuthorizedClient() *apiclient.APIClient {

	config := apiclient.NewConfiguration()
	config.BasePath = baseURL
	client := apiclient.NewAPIClient(config)
	resp, http, err := client.AuthorizationApi.ImmuServiceV2LoginV2(context.Background(), apiclient.SchemaLoginRequest{
		Username: "immudb",
		Password: "immudb",
		Database: "defaultdb",
	})
	if err != nil {
		panic("Error while logging in, tests not properly prepared")
	}
	if http.StatusCode != 200 {
		panic("Error while logging in, tests not properly prepared")
	}
	config.AddDefaultHeader("Authorization", "Bearer "+resp.Token) // To change if we will change to session ID
	return client
}

func GetStandarizedRandomString() string {
	return uuid.New().String()
}

func CreateStandardTestCollection(client *apiclient.APIClient, name string) {
	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        name,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})
	if err != nil {
		panic("Error while creating collection, tests not properly prepared")
	}
	if http.StatusCode != 200 {
		panic("Error while creating collection, tests not properly prepared")
	}

}
