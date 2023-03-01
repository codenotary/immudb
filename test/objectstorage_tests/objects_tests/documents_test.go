package main

import (
	"context"
	"strconv"
	"testing"

	apiclient "github.com/codenotary/immudb/objectstester/go-client"
	"github.com/stretchr/testify/assert"
)

func TestCreateDocument(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)
	documentToInsert := make(map[string]interface{})
	documentToInsert["name"] = "John"
	documentToInsert["surname"] = "Doe"
	documentToInsert["age"] = 30

	documentsToInsert := []interface{}{documentToInsert}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documentsToInsert,
	}
	_, http, err := client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
}

func TestCreateDocumentAndSearch(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()
	CreateStandardTestCollection(client, collection)
	documentToInsert := make(map[string]interface{})
	documentToInsert["name"] = "John"
	documentToInsert["surname"] = "Doe"
	documentToInsert["age"] = 30

	documentsToInsert := []interface{}{documentToInsert}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documentsToInsert,
	}
	_, http, err := client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)

	documentToInsert = make(map[string]interface{})
	documentToInsert["_uuid"] = "X"
	documentToInsert["name"] = "John"
	documentToInsert["surname"] = "Doe"
	documentToInsert["age"] = 30

	documentsToInsert = []interface{}{documentToInsert}

	documentInsertRequest = apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documentsToInsert,
	}
	client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	query := make([]apiclient.SchemaDocumentQuery, 1)
	eqOperator := apiclient.EQ
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &eqOperator,
		Field:    "name",
		Value:    "John",
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(1))
	mapped := searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["name"], "John")
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) == 30)
}

func TestGTSearchOperators(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i <= 100; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 1)
	gtOperator := apiclient.GT
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    10,
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(89))
	mapped := searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) > 10)

	gtOperator = apiclient.GT
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    11,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(88))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) > 11)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    99,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(0))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) > 99)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    100,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(0))
	assert.Equal(t, len(searchResponse.Results), 0)
}

func TestGTESearchOperators(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i <= 100; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 1)
	gtOperator := apiclient.GTE
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    10,
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(90))
	mapped := searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) >= 10)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    11,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(89))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) >= 11)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    99,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(1))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) >= 99)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &gtOperator,
		Field:    "age",
		Value:    100,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(0))
	assert.Equal(t, len(searchResponse.Results), 1)
}

func TestLTSearchOperators(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i <= 100; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 1)
	ltOperator := apiclient.LT
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    10,
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(9))
	mapped := searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) < 10)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    11,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(10))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) < 11)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    99,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(98))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) < 99)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    100,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(99))
	assert.Equal(t, len(searchResponse.Results), 1)
}

func TestLTESearchOperators(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i <= 100; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 1)
	ltOperator := apiclient.LTE
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    10,
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(10))
	mapped := searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) <= 10)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    11,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(11))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) <= 11)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    99,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(99))
	mapped = searchResponse.Results[0].(map[string]interface{})
	assert.Equal(t, mapped["surname"], "Doe")
	assert.True(t, mapped["age"].(float64) < 100)

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &ltOperator,
		Field:    "age",
		Value:    100,
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(100))
	assert.Equal(t, len(searchResponse.Results), 1)
}

func TestLIKESearchOperators(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i <= 100; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		if i%2 == 0 {
			documentToInsert["surname"] = "Marston" + strconv.FormatInt(int64(i), 10)
		}
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 1)
	likeOperator := apiclient.LIKE
	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    ".*",
	}
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(100))

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    "Do.*",
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(49))

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    ".*o.*",
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(100))

	query[0] = apiclient.SchemaDocumentQuery{
		Operator: &likeOperator,
		Field:    "surname",
		Value:    ".*n.*",
	}
	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, searchResponse.EntriesLeft, int64(50))

}

func TestPagination(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i < 1000; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.FormatInt(int64(i), 10)
		documentToInsert["surname"] = "Doe"
		if i%2 == 0 {
			documentToInsert["surname"] = "Marston" + strconv.FormatInt(int64(i), 10)
		}
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)

	query := make([]apiclient.SchemaDocumentQuery, 0)
	searchRequest := apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err := client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 1)
	assert.Equal(t, searchResponse.EntriesLeft, int64(999))

	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       2,
		PerPage:    1,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 1)
	assert.Equal(t, searchResponse.EntriesLeft, int64(998))

	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       1,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 100)
	assert.Equal(t, searchResponse.EntriesLeft, int64(900))

	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       2,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 100)
	assert.Equal(t, searchResponse.EntriesLeft, int64(800))

	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       10,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), 100)
	assert.Equal(t, searchResponse.EntriesLeft, int64(0))

	searchRequest = apiclient.SchemaDocumentSearchRequest{
		Collection: collection,
		Page:       11,
		PerPage:    100,
		Query:      query,
	}
	searchResponse, http, err = client.DocumentsApi.ImmuServiceV2DocumentSearch(context.Background(), searchRequest)
	assert.Equal(t, err, nil)
	assert.Equal(t, http.StatusCode, 200)
	assert.Equal(t, len(searchResponse.Results), int(0))
	assert.Equal(t, searchResponse.EntriesLeft, int64(0))

}

func TestPrimaryKeyViolations(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i < 1000; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John"
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err != nil)
	assert.True(t, http.StatusCode == 409)

	documents = make([]interface{}, 0)
	documentToInsert := make(map[string]interface{})
	documentToInsert["name"] = "John1"
	documentToInsert["age"] = 1
	documents = append(documents, documentToInsert)

	documentInsertRequest = apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err == nil)
	assert.True(t, http.StatusCode == 200)

	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err != nil)
	assert.True(t, http.StatusCode == 409)

}

func TestMultiplePrimaryKeyViolations(t *testing.T) {
	client := GetAuthorizedClient()
	collection := GetStandarizedRandomString()

	primaryKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	indexKeysMap := make(map[string]apiclient.SchemaPossibleIndexType)
	primaryKeysMap["name"] = apiclient.STRING_
	primaryKeysMap["test"] = apiclient.STRING_
	indexKeysMap["age"] = apiclient.INTEGER
	_, http, err := client.CollectionsApi.ImmuServiceV2CollectionCreate(context.Background(), apiclient.SchemaCollectionCreateRequest{
		Name:        collection,
		PrimaryKeys: primaryKeysMap,
		IndexKeys:   indexKeysMap,
	})

	documents := make([]interface{}, 0)
	for i := 0; i < 1000; i++ {
		documentToInsert := make(map[string]interface{})
		documentToInsert["name"] = "John" + strconv.Itoa(i)
		documentToInsert["test"] = strconv.Itoa(i)
		documentToInsert["age"] = i
		documents = append(documents, documentToInsert)

	}

	documentInsertRequest := apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err == nil)
	assert.True(t, http.StatusCode == 200)

	documents = make([]interface{}, 0)
	documentToInsert := make(map[string]interface{})
	documentToInsert["name"] = "John-1"
	documentToInsert["age"] = 1
	documents = append(documents, documentToInsert)

	documentInsertRequest = apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err == nil)
	assert.True(t, http.StatusCode == 200)

	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err != nil)
	assert.True(t, http.StatusCode == 409)

	documents = make([]interface{}, 0)
	documentToInsert = make(map[string]interface{})
	documentToInsert["name"] = "John-2"
	documentToInsert["age"] = 1
	documentToInsert["test"] = "3"
	documents = append(documents, documentToInsert)

	documentInsertRequest = apiclient.SchemaDocumentInsertRequest{
		Collection: collection,
		Document:   documents,
	}
	_, http, err = client.DocumentsApi.ImmuServiceV2DocumentInsert(context.Background(), documentInsertRequest)
	assert.True(t, err != nil)
	assert.True(t, http.StatusCode == 409)

}
