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

	"github.com/codenotary/immudb/test/documents_storage_tests/actions"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type InsertDocumentsTestSuite struct {
	suite.Suite
	expect          *httpexpect.Expect
	token           string
	collection_name string
}

func (s *InsertDocumentsTestSuite) SetupTest() {
	s.expect, s.token = actions.OpenSession(s.T())
	s.collection_name = uuid.New().String()
}

func (s *InsertDocumentsTestSuite) TestInsertDocument() {
	collection := actions.CreateCollectionWithName(s.expect, s.token, s.collection_name)

	collection.Keys().ContainsOnly("collection")
	collection.Value("collection").Object().Value("name").IsEqual(s.collection_name)
}

func TestInsertDocumentsSuite(t *testing.T) {
	suite.Run(t, new(InsertDocumentsTestSuite))
}
