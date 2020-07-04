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

var historyHandlerTestCases = []struct {
	name     string
	key      string
	testFunc func(*testing.T, string, int, map[string]interface{})
}{
	{
		"Sending correct request",
		base64.StdEncoding.EncodeToString([]byte("setKey1")),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusOK, status)
			items, ok := body["items"]
			require.True(t, ok, "%sfield \"items\" not found in response %v", testCase, body)
			notEmptyMsg := "%sexpected more than on item in response %v"
			require.True(t, len(items.([]interface{})) > 0, notEmptyMsg, testCase, body)
		},
	},
	{
		"Sending correct request with non-existent key",
		base64.StdEncoding.EncodeToString([]byte("historyNonExistentKey1")),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusOK, status)
			notEmptyMsg := "%sexpected empty response, actual response length is %d"
			require.Equal(t, 0, len(body), notEmptyMsg, testCase, len(body))
		},
	},
	{
		"Missing key path param",
		"",
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "invalid key"}, body)
		},
	},
	{
		"Sending plain text instead of base64 encoded",
		"setKey1",
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			expectedError :=
				"type mismatch, parameter: key, error: illegal base64 data at input byte 4"
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": expectedError}, body)
		},
	},
}

func testHistoryHandler(t *testing.T, historyHandler HistoryHandler) {
	prefixPattern := "HistoryHandler - Test case: %s"
	method := "GET"
	for _, tc := range historyHandlerTestCases {
		path := "/v1/immurestproxy/history/" + tc.key
		handlerFunc := func(res http.ResponseWriter, req *http.Request) {
			historyHandler.History(res, req, map[string]string{"key": tc.key})
		}
		err := testHandler(
			t,
			fmt.Sprintf(prefixPattern, tc.name),
			method,
			path,
			"",
			handlerFunc,
			tc.testFunc,
		)
		require.NoError(t, err)
	}
}
