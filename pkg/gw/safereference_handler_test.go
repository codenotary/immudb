/*
Copyright 2019-2020 vChain, Inc.

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
package gw

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

var safeReferenceHandlerTestCases = []struct {
	name     string
	payload  string
	testFunc func(*testing.T, string, int, map[string]interface{})
}{
	{
		"Sending correct request",
		fmt.Sprintf(
			"{\"ro\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeReferenceKey1")),
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusOK, status)
			requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
		},
	},
	{
		"Sending correct request with non-existent key",
		fmt.Sprintf(
			"{\"ro\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeReferenceKey1")),
			base64.StdEncoding.EncodeToString([]byte("safeReferenceUnknownKey")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusNotFound, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "Key not found"}, body)
		},
	},
	{
		"Sending incorrect json field",
		fmt.Sprintf(
			"{\"data\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeReferenceKey1")),
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "incorrect JSON payload"}, body)
		},
	},
	{
		"Missing Key field",
		fmt.Sprintf(
			"{\"ro\": {\"reference\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeReferenceKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "invalid key"}, body)
		},
	},
	{
		"Sending plain text instead of base64 encoded",
		fmt.Sprintf(
			"{\"ro\": {\"reference\": \"safeReferenceKey1\", \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "illegal base64 data at input byte 16"}, body)
		},
	},
}

func testSafeReferenceHandler(t *testing.T, safeReferenceHandler SafeReferenceHandler) {
	prefixPattern := "SafeReferenceHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/safe/reference"
	handlerFunc := func(res http.ResponseWriter, req *http.Request) {
		safeReferenceHandler.SafeReference(res, req, nil)
	}
	for _, tc := range safeReferenceHandlerTestCases {
		err := testHandler(
			t,
			fmt.Sprintf(prefixPattern, tc.name),
			method,
			path,
			tc.payload,
			handlerFunc,
			tc.testFunc,
		)
		require.NoError(t, err)
	}
}
