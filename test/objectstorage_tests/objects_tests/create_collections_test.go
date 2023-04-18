package main

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
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

func TestSuite(t *testing.T) {
	suite.Run(t, new(CreateCollectionsTestSuite))
}
