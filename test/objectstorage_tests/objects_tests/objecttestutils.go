package main

import (
	"context"
	"net/http"
	"os"

	authorizationClient "github.com/codenotary/immudb/test/objectstorage_tests/immudbhttpclient/immudbauth"
	documentsClient "github.com/codenotary/immudb/test/objectstorage_tests/immudbhttpclient/immudbdocuments"

	"github.com/google/uuid"
)

var baseURL = GetEnv("OBJECTS_TEST_BASEURL", "http://localhost:8091/api/v2")

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

func getAuthorizationClient() *authorizationClient.ClientWithResponses {
	client, err := authorizationClient.NewClientWithResponses(baseURL)
	if err != nil {
		panic(err)
	}
	return client
}

func getDocumentsClient(opts ...documentsClient.ClientOption) *documentsClient.ClientWithResponses {
	client, err := documentsClient.NewClientWithResponses(baseURL, opts...)
	if err != nil {
		panic(err)
	}
	return client
}

func getAuthorizedDocumentsClient() *documentsClient.ClientWithResponses {
	authClient := getAuthorizationClient()
	defaultLogin := "immudb"
	defaultPassword := "immudb"
	defaultDatabase := "defaultdb"
	response, err := authClient.AuthorizationServiceOpenSessionV2WithResponse(context.Background(), authorizationClient.AuthorizationServiceOpenSessionV2JSONRequestBody{
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

	client := getDocumentsClient(documentsClient.WithRequestEditorFn(
		func(ctx context.Context, req *http.Request) error {
			req.Header.Set("grpc-metadata-sessionid", *response.JSON200.Token)
			return nil
		},
	))

	return client
}

func CreateAndGetStandardTestCollection(client *documentsClient.ClientWithResponses) string {
	collectionName := GetStandarizedRandomString()
	indexKeys := make(map[string]documentsClient.DocumentschemaIndexOption)
	primaryKeys := make(map[string]documentsClient.DocumentschemaIndexOption)
	stringType := documentsClient.STRING
	primaryKeys["_id"] = documentsClient.DocumentschemaIndexOption{
		Type: &stringType,
	}
	req := documentsClient.DocumentServiceCollectionCreateJSONRequestBody{
		Name:        &collectionName,
		IndexKeys:   &indexKeys,
		PrimaryKeys: &primaryKeys,
	}
	response, err := client.DocumentServiceCollectionCreateWithResponse(context.Background(), req)
	if err != nil {
		panic(err)
	}
	if response.StatusCode() != 200 {
		panic("No 200 response")
	}
	return collectionName
}
