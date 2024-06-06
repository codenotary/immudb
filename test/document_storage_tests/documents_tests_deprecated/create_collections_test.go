/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/codenotary/immudb/test/documents_storage_tests/documents_tests/actions"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateCollectionsTestSuite struct {
	suite.Suite
	expect          *httpexpect.Expect
	token           string
	collection_name string
}

func (s *CreateCollectionsTestSuite) SetupTest() {
	s.expect, s.token = actions.OpenSession(s.T())
	s.collection_name = uuid.New().String()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithName() {
	collection := actions.CreateCollectionWithName(s.expect, s.token, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneIndexKey() {
	collection := actions.CreateCollectionWithNameAndOneIndexKey(s.expect, s.token, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("indexKeys").Object().Keys().ContainsOnly("_id", "birth_date")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("_id").Object().Value("type").IsEqual("STRING")
	collection.Value("collection").Object().Value("indexKeys").Object().Value("birth_date").Object().Value("type").IsEqual("STRING")
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndMultipleIndexKeys() {
	collection := actions.CreateCollectionWithNameAndMultipleIndexKeys(s.expect, s.token, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
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
	collection := actions.CreateCollectionWithIntegerName(s.expect, s.token, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
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
