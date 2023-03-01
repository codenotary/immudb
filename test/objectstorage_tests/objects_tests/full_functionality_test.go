package main

import (
	"context"
	"encoding/json"
	"testing"

	apiclient "github.com/codenotary/immudb/objectstester/go-client"
	"github.com/stretchr/testify/assert"
)

func TestSimpleApplication(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)
	for i := 0; i < 1000; i++ {

		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John"
		documentToInsert["surname"] = "Doe"
		documentToInsert["age"] = 30
		documentToInsert[GetStandarizedRandomString()] = GetStandarizedRandomString()
		if i%5 == 0 {
			documentToInsert["xxx"] = 20
		}

		documentsToInsert := []interface{}{documentToInsert}

		documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
			Collection: collection,
			Document:   documentsToInsert,
		}
		_, http, err := client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
		assert.Equal(t, err, nil)
		assert.Equal(t, http.StatusCode, 200)
	}

	query := make([]apiclient.SchemaDocumentQuery, 3)
	gtOperator := apiclient.GT
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    10,
	}
	likeOperator := apiclient.LIKE
	query[1] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "name",
		Value:    "John",
	}
	query[2] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    "Doe",
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 100)
	xxxCount := 0
	for _, v := range searchResponse.Results {
		mapped := v.(map[string]interface{})
		assert.Equal(t, mapped["name"], "John")
		assert.Equal(t, mapped["surname"], "Doe")
		assert.Equal(t, mapped["age"], float64(30))
		if _, ok := mapped["xxx"]; ok {
			xxxCount++
		}

	}
	assert.Equal(t, xxxCount, 20)

}

type SimpleDataStruct struct {
	Name    string  `json:"name"`
	Surname string  `json:"surname"`
	Age     int     `json:"age"`
	Cash    float64 `json:"cash"`
}

func TestSimpleApplicationStructApproach(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)
	for i := 0; i < 1000; i++ {

		documentToInsert := SimpleDataStruct{
			Name:    "John",
			Surname: "Doe",
			Age:     30,
			Cash:    100.0,
		}

		documentsToInsert := []interface{}{documentToInsert}

		documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
			Collection: collection,
			Document:   documentsToInsert,
		}
		_, http, err := client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
		assert.Equal(t, err, nil)
		assert.Equal(t, http.StatusCode, 200)
	}

	query := make([]apiclient.SchemaDocumentQuery, 3)
	gtOperator := apiclient.GT
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    29,
	}
	likeOperator := apiclient.LIKE
	query[1] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "name",
		Value:    "John",
	}
	query[2] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    "Doe",
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 100)

	// Here we are using the same struct to unmarshal the result
	// The reason why i needed to marshal and unmarshal here is, that we are returning values
	// as interface{} and we need to convert them to the struct type
	// in general if user wants to user his own struct, he just needs to change a bit his API that will unmarshal
	// json properly in GO
	// In python for example it would be much easier

	// unmarshalling is done in api_documents.go
	for _, v := range searchResponse.Results {
		bytes, err := json.Marshal(v)
		assert.Equal(t, err, nil)
		newStruct := SimpleDataStruct{}
		err = json.Unmarshal(bytes, &newStruct)
		assert.Equal(t, err, nil)
		assert.Equal(t, newStruct.Name, "John")
		assert.Equal(t, newStruct.Surname, "Doe")
		assert.Equal(t, newStruct.Age, 30)
	}

}
