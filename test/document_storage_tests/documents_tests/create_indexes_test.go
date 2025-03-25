/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"testing"

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/actions"
	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/models"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type IndexTestSuite struct {
	suite.Suite
	expect          *httpexpect.Expect
	sessionID       string
	collection_name string
}

func (s *IndexTestSuite) SetupTest() {
	s.expect, s.sessionID = actions.OpenSession(s.T())
	s.collection_name = "a" + uuid.New().String()
}

func (s *IndexTestSuite) TestCreateIndexOnCollectionCreatedWithNameAndOneField() {
	actions.CreateCollectionWithNameAndOneField(s.expect, s.sessionID, s.collection_name)

	payload := models.CreateIndex{
		Fields: []string{"first_name"},
	}

	actions.CreateIndex(s.expect, s.sessionID, s.collection_name, payload)

	collection := actions.GetCollection(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")

	var employees models.Collection

	collection.Value("collection").Object().Decode(&employees)

	assert.Equal(s.T(), employees.Name, s.collection_name)
	assert.Equal(s.T(), employees.DocumentIdFieldName, "_id")
	assert.Equal(s.T(), len(employees.Fields), 2)
	assert.Equal(s.T(), employees.Fields[0].Name, "_id")
	assert.Equal(s.T(), employees.Fields[1].Name, "first_name")
	assert.Equal(s.T(), len(employees.Indexes), 2)
	assert.Equal(s.T(), employees.Indexes[0].Fields[0], "_id")
	assert.Equal(s.T(), employees.Indexes[0].IsUnique, true)
	assert.Equal(s.T(), employees.Indexes[1].Fields[0], "first_name")
}

func (s *IndexTestSuite) TestCreateIndexOnCollectionCreatedWithNameIdFieldNameAndOneField() {
	actions.CreateCollectionWithNameIdFieldNameAndOneField(s.expect, s.sessionID, s.collection_name)

	payload := models.CreateIndex{
		Fields: []string{"hire_date"},
	}

	actions.CreateIndex(s.expect, s.sessionID, s.collection_name, payload)

	collection := actions.GetCollection(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")

	var employees models.Collection

	collection.Value("collection").Object().Decode(&employees)

	assert.Equal(s.T(), employees.Name, s.collection_name)
	assert.Equal(s.T(), employees.DocumentIdFieldName, "emp_no")
	assert.Equal(s.T(), len(employees.Fields), 2)
	assert.Equal(s.T(), employees.Fields[0].Name, "emp_no")
	assert.Equal(s.T(), employees.Fields[1].Name, "hire_date")
	assert.Equal(s.T(), len(employees.Indexes), 2)
	assert.Equal(s.T(), employees.Indexes[0].Fields[0], "emp_no")
	assert.Equal(s.T(), employees.Indexes[1].Fields[0], "hire_date")
}

func (s *IndexTestSuite) TestCreateUniqueIndexOnCollectionCreatedWithNameAndMultipleFields() {
	actions.CreateCollectionWithNameAndMultipleFields(s.expect, s.sessionID, s.collection_name)

	payload := models.CreateIndex{
		Fields:   []string{"gender"},
		IsUnique: true,
	}

	actions.CreateIndex(s.expect, s.sessionID, s.collection_name, payload)

	collection := actions.GetCollection(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")

	var employees models.Collection

	collection.Value("collection").Object().Decode(&employees)

	assert.Equal(s.T(), employees.Name, s.collection_name)
	assert.Equal(s.T(), employees.DocumentIdFieldName, "_id")
	assert.Equal(s.T(), len(employees.Fields), 6)
	assert.Equal(s.T(), employees.Fields[0].Name, "_id")
	assert.Equal(s.T(), employees.Fields[1].Name, "birth_date")
	assert.Equal(s.T(), employees.Fields[2].Name, "first_name")
	assert.Equal(s.T(), employees.Fields[3].Name, "last_name")
	assert.Equal(s.T(), employees.Fields[4].Name, "gender")
	assert.Equal(s.T(), employees.Fields[5].Name, "hire_date")
	assert.Equal(s.T(), len(employees.Indexes), 2)
	assert.Equal(s.T(), employees.Indexes[0].Fields[0], "_id")
	assert.Equal(s.T(), employees.Indexes[0].IsUnique, true)
	assert.Equal(s.T(), employees.Indexes[1].Fields[0], "gender")
	assert.Equal(s.T(), employees.Indexes[1].IsUnique, true)
}

func (s *IndexTestSuite) TestCreateUniqueIndexesOnCollectionCreatedWithNameMultipleFieldsAndMultipleIndexes() {
	actions.CreateCollectionWithNameMultipleFieldsAndMultipleIndexes(s.expect, s.sessionID, s.collection_name)

	payload := models.CreateIndex{
		Fields:   []string{"gender", "hire_date"},
		IsUnique: true,
	}

	actions.CreateIndex(s.expect, s.sessionID, s.collection_name, payload)

	collection := actions.GetCollection(s.expect, s.sessionID, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Keys().ContainsOnly("name", "documentIdFieldName", "fields", "indexes")

	var employees models.Collection

	collection.Value("collection").Object().Decode(&employees)

	assert.Equal(s.T(), employees.Name, s.collection_name)
	assert.Equal(s.T(), employees.DocumentIdFieldName, "_id")
	assert.Equal(s.T(), len(employees.Fields), 6)
	assert.Equal(s.T(), employees.Fields[0].Name, "_id")
	assert.Equal(s.T(), employees.Fields[1].Name, "birth_date")
	assert.Equal(s.T(), employees.Fields[2].Name, "first_name")
	assert.Equal(s.T(), employees.Fields[3].Name, "last_name")
	assert.Equal(s.T(), employees.Fields[4].Name, "gender")
	assert.Equal(s.T(), employees.Fields[5].Name, "hire_date")
	assert.Equal(s.T(), len(employees.Indexes), 3)
	assert.Equal(s.T(), employees.Indexes[0].Fields[0], "_id")
	assert.Equal(s.T(), employees.Indexes[0].IsUnique, true)
	assert.Equal(s.T(), employees.Indexes[1].Fields[0], "birth_date")
	assert.Equal(s.T(), employees.Indexes[1].Fields[1], "last_name")
	assert.Equal(s.T(), employees.Indexes[1].IsUnique, true)
	assert.Equal(s.T(), employees.Indexes[2].Fields[0], "gender")
	assert.Equal(s.T(), employees.Indexes[2].Fields[1], "hire_date")
	assert.Equal(s.T(), employees.Indexes[2].IsUnique, true)
}

func TestIndexTestSuite(t *testing.T) {
	suite.Run(t, new(IndexTestSuite))
}
