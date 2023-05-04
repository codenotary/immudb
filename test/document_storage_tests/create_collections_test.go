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
	"net/http"
	"testing"

	"github.com/codenotary/immudb/test/documents_storage_tests/actions"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type CreateCollectionsTestSuite struct {
	suite.Suite
	expect          *httpexpect.Expect
	sessionID       string
	collection_name string
}

func (s *CreateCollectionsTestSuite) SetupTest() {
	s.expect, s.sessionID = actions.OpenSession(s.T())
	s.collection_name = uuid.New().String()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithName() {
	collection := actions.CreateCollectionWithName(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneField() {
	collection := actions.CreateCollectionWithNameAndOneField(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(2)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Value(1).Object().Value("name").IsEqual("first_name")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndIdFieldName() {
	collection := actions.CreateCollectionWithNameAndIdFieldName(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("emp_no")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("emp_no")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("emp_no")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameIdFieldNameAndOneField() {
	collection := actions.CreateCollectionWithNameIdFieldNameAndOneField(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("emp_no")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(2)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("emp_no")
	collection.Value("collection").Object().Value("fields").Array().Value(1).Object().Value("name").IsEqual("hire_date")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("emp_no")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameOneFieldAndOneUniqueIndex() {
	collection := actions.CreateCollectionWithNameOneFieldAndOneUniqueIndex(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(2)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Value(1).Object().Value("name").IsEqual("id_number")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(2)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
	collection.Value("collection").Object().Value("indexes").Array().Value(1).Object().Value("fields").Array().Value(0).IsEqual("id_number")
	collection.Value("collection").Object().Value("indexes").Array().Value(1).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndMultipleFields() {
	collection := actions.CreateCollectionWithNameAndMultipleFields(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(6)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Value(1).Object().Value("name").IsEqual("birth_date")
	collection.Value("collection").Object().Value("fields").Array().Value(2).Object().Value("name").IsEqual("first_name")
	collection.Value("collection").Object().Value("fields").Array().Value(3).Object().Value("name").IsEqual("last_name")
	collection.Value("collection").Object().Value("fields").Array().Value(4).Object().Value("name").IsEqual("gender")
	collection.Value("collection").Object().Value("fields").Array().Value(5).Object().Value("name").IsEqual("hire_date")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameMultipleFieldsAndMultipleIndexes() {
	collection := actions.CreateCollectionWithNameMultipleFieldsAndMultipleIndexes(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "idFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("idFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(6)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Value(1).Object().Value("name").IsEqual("birth_date")
	collection.Value("collection").Object().Value("fields").Array().Value(2).Object().Value("name").IsEqual("first_name")
	collection.Value("collection").Object().Value("fields").Array().Value(3).Object().Value("name").IsEqual("last_name")
	collection.Value("collection").Object().Value("fields").Array().Value(4).Object().Value("name").IsEqual("gender")
	collection.Value("collection").Object().Value("fields").Array().Value(5).Object().Value("name").IsEqual("hire_date")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(2)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
	collection.Value("collection").Object().Value("indexes").Array().Value(1).Object().Value("fields").Array().Value(0).IsEqual("birth_date")
	collection.Value("collection").Object().Value("indexes").Array().Value(1).Object().Value("fields").Array().Value(1).IsEqual("last_name")
	collection.Value("collection").Object().Value("indexes").Array().Value(1).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithEmptyBody() {
	payload := map[string]interface{}{}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithoutNameButWithFields() {
	payload := map[string]interface{}{
		"fields": []interface{}{
			map[string]interface{}{
				"name": "birth_date",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "first_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "last_name",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "gender",
				"type": "STRING",
			},
			map[string]interface{}{
				"name": "hire_date",
				"type": "STRING",
			},
		},
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithIntegerName() {
	payload := map[string]interface{}{
		"name": 123,
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty().
		Value("error").IsEqual("json: cannot unmarshal number into Go value of type string")
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneInvalidField() {
	payload := map[string]interface{}{
		"name":   uuid.New().String(),
		"fields": "birth_date",
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneEmptyField() {
	payload := map[string]interface{}{
		"name":   uuid.New().String(),
		"fields": "",
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithQuery("name", payload["name"]).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithExistingName() {
	payload := map[string]interface{}{
		"name": uuid.New().String(),
	}

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().Empty()

	s.expect.PUT("/collections/create").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object().NotEmpty()

	listPayload := map[string]interface{}{}

	collections := s.expect.POST("/collections/list").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(listPayload).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collectionsFound := collections.Value("collections").Array().FindAll(func(index int, value *httpexpect.Value) bool {
		return value.Object().Value("name").Raw() == payload["name"]
	})

	assert.Equal(s.T(), len(collectionsFound), 1)
}

func TestCreateCollectionsSuite(t *testing.T) {
	suite.Run(t, new(CreateCollectionsTestSuite))
}
