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

type InsertDocumentsTestSuite struct {
	suite.Suite
	expect     *httpexpect.Expect
	sessionID  string
	collection *httpexpect.Object
}

func (s *InsertDocumentsTestSuite) SetupTest() {
	s.expect, s.sessionID = actions.OpenSession(s.T())
	s.collection = actions.CreateCollectionWithNameAndMultipleFields(s.expect, s.sessionID, uuid.New().String())
}

func (s *InsertDocumentsTestSuite) TestInsertOneDocumentWithMultipleFields() {
	documentModel := actions.InsertOneDocumentWithMultipleFields(s.expect, s.sessionID, s.collection)

	fieldComparison := models.FieldComparison{}
	fieldComparison.Field = "first_name"
	fieldComparison.Operator = "EQ"
	fieldComparison.Value = documentModel.FirstName
	documentFound := actions.SearchDocuments(s.expect, s.sessionID, s.collection, fieldComparison)

	documentFound.Keys().ContainsOnly("_id", "birth_date", "first_name", "last_name", "gender", "hire_date")

	var employee models.Employee

	documentFound.Decode(&employee)

	assert.Equal(s.T(), employee.BirthDate, "1964-06-02")
	assert.Equal(s.T(), employee.FirstName, "Bezalel")
	assert.Equal(s.T(), employee.LastName, "Simmel")
	assert.Equal(s.T(), employee.Gender, "F")
	assert.Equal(s.T(), employee.HireDate, "1985-11-21")
}

func TestInsertDocumentsSuite(t *testing.T) {
	suite.Run(t, new(InsertDocumentsTestSuite))
}
