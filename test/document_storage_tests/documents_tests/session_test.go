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
	"net/http"
	"testing"

	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/actions"
	"github.com/codenotary/immudb/test/document_storage_tests/documents_tests/models"
	"github.com/gavv/httpexpect/v2"
	"github.com/stretchr/testify/suite"
)

type SessionTestSuite struct {
	suite.Suite
	expect *httpexpect.Expect
}

func (s *SessionTestSuite) TestOpenSessionWithInvalidUsername() {
	baseURL := actions.GetBaseUrl()

	user := models.User{
		Username: "jon_snow",
		Password: "immudb",
		Database: "defaultdb",
	}

	expect := httpexpect.Default(s.T(), baseURL)
	response := expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object()

	response.Keys().ContainsOnly("code", "details", "error", "message")
	response.Value("error").IsEqual("invalid user name or password")
	response.Value("code").IsEqual(2)
	response.Value("message").IsEqual("invalid user name or password")
}

func (s *SessionTestSuite) TestOpenSessionWithInvalidPassword() {
	baseURL := actions.GetBaseUrl()

	user := models.User{
		Username: "immudb",
		Password: "know_n0thinG",
		Database: "defaultdb",
	}

	expect := httpexpect.Default(s.T(), baseURL)
	response := expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object()

	response.Keys().ContainsOnly("code", "details", "error", "message")
	response.Value("error").IsEqual("invalid user name or password")
	response.Value("code").IsEqual(2)
	response.Value("message").IsEqual("invalid user name or password")
}

func (s *SessionTestSuite) TestOpenSessionWithInvalidCredentials() {
	baseURL := actions.GetBaseUrl()

	user := models.User{
		Username: "jon_snow",
		Password: "know_n0thinG",
		Database: "defaultdb",
	}

	expect := httpexpect.Default(s.T(), baseURL)
	response := expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object()

	print(response)
	response.Keys().ContainsOnly("code", "details", "error", "message")
	response.Value("error").IsEqual("invalid user name or password")
	response.Value("code").IsEqual(2)
	response.Value("message").IsEqual("invalid user name or password")
}

func (s *SessionTestSuite) TestOpenSessionWithNonExistingDatabase() {
	baseURL := actions.GetBaseUrl()

	user := models.User{
		Username: "immudb",
		Password: "immudb",
		Database: "mydb",
	}

	expect := httpexpect.Default(s.T(), baseURL)
	response := expect.POST("/authorization/session/open").
		WithJSON(user).
		Expect().
		Status(http.StatusInternalServerError).JSON().Object()

	print(response)
	response.Keys().ContainsOnly("code", "error", "message")
	response.Value("error").IsEqual("database does not exist")
	response.Value("code").IsEqual(2)
	response.Value("message").IsEqual("database does not exist")
}

func TestSessionTestSuite(t *testing.T) {
	suite.Run(t, new(SessionTestSuite))
}
