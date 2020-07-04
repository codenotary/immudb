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

var safeGetHandlerTestCases = []struct {
	name     string
	payload  string
	testFunc func(*testing.T, string, int, map[string]interface{})
}{
	{
		"Sending correct request",
		fmt.Sprintf(
			"{\"key\": \"%s\"}",
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusOK, status)
			requireResponseFields(
				t, testCase, []string{"index", "key", "value", "time"}, body)
			requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
		},
	},
	{
		"Sending incorrect json field",
		fmt.Sprintf(
			"{\"keyX\": \"%s\"}",
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			expected := map[string]interface{}{"error": "invalid key"}
			requireResponseFieldsEqual(t, testCase, expected, body)
		},
	},
	{
		"Sending plain text instead of base64 encoded",
		`{"key": "setKey1"}`,
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			expected :=
				map[string]interface{}{"error": "illegal base64 data at input byte 4"}
			requireResponseFieldsEqual(t, testCase, expected, body)
		},
	},
	{
		"Missing key field",
		`{}`,
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			expected := map[string]interface{}{"error": "invalid key"}
			requireResponseFieldsEqual(t, testCase, expected, body)
		},
	},
}

func testSafeGetHandler(t *testing.T, safeGetHandler SafegetHandler) {
	prefixPattern := "SafeGetHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/item/safe/get"
	handlerFunc := func(res http.ResponseWriter, req *http.Request) {
		safeGetHandler.Safeget(res, req, nil)
	}
	for _, tc := range safeGetHandlerTestCases {
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
