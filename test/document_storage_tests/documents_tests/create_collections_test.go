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
	"fmt"
	"net/http"
	"testing"

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/actions"
	"github.com/gavv/httpexpect/v2"
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
	s.collection_name = "a" + uuid.New().String()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithName() {
	collection := actions.CreateCollectionWithName(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("_id")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("_id")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneField() {
	collection := actions.CreateCollectionWithNameAndOneField(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("_id")
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
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("emp_no")
	collection.Value("collection").Object().Value("fields").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("fields").Array().Value(0).Object().Value("name").IsEqual("emp_no")
	collection.Value("collection").Object().Value("indexes").Array().Length().IsEqual(1)
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("fields").Array().Value(0).IsEqual("emp_no")
	collection.Value("collection").Object().Value("indexes").Array().Value(0).Object().Value("isUnique").Boolean().IsTrue()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameIdFieldNameAndOneField() {
	collection := actions.CreateCollectionWithNameIdFieldNameAndOneField(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("emp_no")
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
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("_id")
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
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("_id")
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
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
	collection.Value("collection").Object().Value("documentIdFieldName").IsEqual("_id")
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

	s.expect.POST(fmt.Sprintf("/collection/%s", s.collection_name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusOK).JSON().Object().IsEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneInvalidField() {
	name := "a" + uuid.New().String()
	payload := map[string]interface{}{
		"fields": "birth_date",
	}

	s.expect.POST(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithNameAndOneEmptyField() {
	name := "a" + uuid.New().String()
	payload := map[string]interface{}{
		"fields": "",
	}

	s.expect.POST(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		WithJSON(payload).
		Expect().
		Status(http.StatusBadRequest).JSON().Object().NotEmpty()

	s.expect.GET(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object().NotEmpty()
}

func (s *CreateCollectionsTestSuite) TestCreateCollectionWithExistingName() {
	name := "a" + uuid.New().String()

	s.expect.POST(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		Expect().
		Status(http.StatusOK).JSON().Object().IsEmpty()

	s.expect.POST(fmt.Sprintf("/collection/%s", name)).
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object().NotEmpty()

	collections := s.expect.GET("/collections").
		WithHeader("grpc-metadata-sessionid", s.sessionID).
		Expect().
		Status(http.StatusOK).
		JSON().Object()

	collectionsFound := collections.Value("collections").Array().FindAll(func(index int, value *httpexpect.Value) bool {
		return value.Object().Value("name").Raw() == name
	})

	assert.Equal(s.T(), len(collectionsFound), 1)
}

func TestCreateCollectionsSuite(t *testing.T) {
	suite.Run(t, new(CreateCollectionsTestSuite))
}
