package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateCollectionsTestSuite struct {
	suite.Suite
	expect *httpexpect.Expect
	token  string
}

func (s *CreateCollectionsTestSuite) SetupTest() {
	baseURL, exists := os.LookupEnv("OBJECTS_TEST_BASEURL")

	if !exists {
		baseURL = "http://localhost:8091/api/v2"
	}

	user := map[string]interface{}{
		"username": "immudb",
		"password": "immudb",
		"database": "defaultdb",
	}

	s.expect = httpexpect.Default(s.T(), baseURL)
	obj := s.expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusOK).JSON().Object()
	s.token = obj.Value("token").Raw().(string)

}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithName() {
	payloadModel := `{
       	"name": "string"
	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	collection := s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(payload["name"])
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneIndexKey() {
	payloadModel := `{
		"name": "string",
		"indexKeys": {
			"customers": {
			  "type": "DOUBLE"
			}
		}
 	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()
	payload["indexKeys"] = map[string]interface{}{
		"birth_date": map[string]interface{}{
			"type": "STRING",
		},
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	collection := s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(payload["name"])
	collection.Value("collection").Object().Value("indexKeys").Object().Keys().ContainsOnly("_id", "birth_date")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("_id").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("birth_date").Object().Value("type").IsEqual("STRING")
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndMultipleIndexKeys() {
	payloadModel := `{
		"name": "string",
		"indexKeys": {
			"employees": {
			  "type": "INTEGER"
			}
		}
 	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()
	payload["indexKeys"] = map[string]interface{}{
		"birth_date": map[string]interface{}{
			"type": "STRING",
		},
		"first_name": map[string]interface{}{
			"type": "STRING",
		},
		"last_name": map[string]interface{}{
			"type": "STRING",
		},
		"gender": map[string]interface{}{
			"type": "STRING",
		},
		"hire_date": map[string]interface{}{
			"type": "STRING",
		},
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	collection := s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(payload["name"])
	collection.Value("collection").Object().Value("indexKeys").Object().Keys().ContainsOnly("_id", "birth_date", "first_name", "last_name", "gender", "hire_date")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("_id").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("birth_date").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("first_name").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("last_name").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("gender").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("hire_date").Object().Value("type").IsEqual("STRING")
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithoutNameAndIndexKeys() {
	payloadModel := `{}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithoutNameButWithIndexKeys() {
	payloadModel := `{
		"indexKeys": {
			"employees": {
			  "type": "INTEGER"
			}
		}
	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["indexKeys"] = map[string]interface{}{
		"birth_date": map[string]interface{}{
			"type": "STRING",
		},
		"first_name": map[string]interface{}{
			"type": "STRING",
		},
		"last_name": map[string]interface{}{
			"type": "STRING",
		},
		"gender": map[string]interface{}{
			"type": "STRING",
		},
		"hire_date": map[string]interface{}{
			"type": "STRING",
		},
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithIntegerName() {
	payloadModel := `{
       	"name": 123
	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New()

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	collection := s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(payload["name"])
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneInvalidIndexKey() {
	payloadModel := `{
		"name": "string",
		"indexKeys": "string"
 	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()
	payload["indexKeys"] = "birth_date"

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneEmptyIndexKey() {
	payloadModel := `{
		"name": "string",
		"indexKeys": "string"
 	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()
	payload["indexKeys"] = ""

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithExistingName() {
	payloadModel := `{
		"name": "string"
 	}`
	var payload map[string]interface{}
	json.Unmarshal([]byte(payloadModel), &payload)
	payload["name"] = uuid.New().String()

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().NotEmpty()

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object().NotEmpty()

	payloadModel = `{}`
	json.Unmarshal([]byte(payloadModel), &payload)

	collections := s.expect.POST("/collections/list").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collectionsFound := collections.Value("collections").Array().FindAll(func(index int, value *httpexpect.Value) bool {
		return value.Object().Value("name").Raw() == payload["name"]
	})

	assert.Equal(s.T(), len(collectionsFound), 1)
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(CreateCollectionsTestSuite))
}
