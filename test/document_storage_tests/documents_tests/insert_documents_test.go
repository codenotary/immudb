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
	"testing"

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/actions"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
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
	document := map[string]interface{}{
		"birth_date": "1964-06-02",
		"first_name": "Bezalel",
		"last_name":  "Simmel",
		"gender":     "F",
		"hire_date":  "1985-11-21",
	}

	documentFound := actions.InsertOneDocumentWithMultipleFields(s.expect, s.sessionID, s.collection, document)

	documentFound.Keys().ContainsOnly("_id", "birth_date", "first_name", "last_name", "gender", "hire_date")
	documentFound.Value("birth_date").IsEqual(document["birth_date"])
	documentFound.Value("first_name").IsEqual(document["first_name"])
	documentFound.Value("last_name").IsEqual(document["last_name"])
	documentFound.Value("gender").IsEqual(document["gender"])
	documentFound.Value("hire_date").IsEqual(document["hire_date"])
}

func TestInsertDocumentsSuite(t *testing.T) {
	suite.Run(t, new(InsertDocumentsTestSuite))
}
