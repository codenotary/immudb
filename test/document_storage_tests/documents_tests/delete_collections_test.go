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

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/actions"
	"github.com/gavv/httpexpect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type DeleteCollectionsTestSuite struct {
	suite.Suite
	expect          *httpexpect.Expect
	token           string
	collection_name string
}

func (s *DeleteCollectionsTestSuite) SetupTest() {
	s.expect, s.token = actions.OpenSession(s.T())
	s.collection_name = uuid.New().String()
}

func (s *DeleteCollectionsTestSuite) TestDeleteCollectionCreatedWithName() {
	actions.CreateCollectionWithName(s.expect, s.token, s.collection_name)

	s.expect.DELETE("/collections/delete").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusOK).
		JSON().Object().Empty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusInternalServerError)
}

func (s *DeleteCollectionsTestSuite) TestDeleteCollectionCreatedWithNameAndOneField() {
	actions.CreateCollectionWithNameAndOneField(s.expect, s.token, s.collection_name)

	s.expect.DELETE("/collections/delete").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusOK).
		JSON().Object().Empty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusInternalServerError)
}

func (s *DeleteCollectionsTestSuite) TestDeleteCollectionCreatedWithNameAndMultipleFields() {
	actions.CreateCollectionWithNameAndMultipleFields(s.expect, s.token, s.collection_name)

	s.expect.DELETE("/collections/delete").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusOK).
		JSON().Object().Empty()

	s.expect.GET("/collections/get").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusInternalServerError)
}

func (s *DeleteCollectionsTestSuite) TestDeleteCollectionCreatedWithIntegerName() {
	s.expect.DELETE("/collections/delete").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", 123).
		Expect().
		Status(http.StatusInternalServerError)
}

func (s *DeleteCollectionsTestSuite) TestDeleteNonExistingCollection() {
	error := s.expect.DELETE("/collections/delete").
		WithHeader("grpc-metadata-sessionid", s.token).
		WithQuery("name", s.collection_name).
		Expect().
		Status(http.StatusInternalServerError).
		JSON().Object()

	error.Keys().ContainsAll("code", "error", "message")
	error.Value("code").IsEqual(2)
	error.Value("error").IsEqual("collection does not exist")
	error.Value("message").IsEqual("collection does not exist")
}

func TestDeleteCollectionsTestSuite(t *testing.T) {
	suite.Run(t, new(DeleteCollectionsTestSuite))
}
