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

var safeZAddHandlerTestCases = []struct {
	name     string
	payload  string
	testFunc func(*testing.T, string, int, map[string]interface{})
}{
	{
		"Sending correct request",
		fmt.Sprintf(
			"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeZAddSet1")),
			1.0,
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusOK, status)
			requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
		},
	},
	{
		"Sending request with non-existent key",
		fmt.Sprintf(
			"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeZAddSet1")),
			1.0,
			base64.StdEncoding.EncodeToString([]byte("safeZAddUnknownKey")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusNotFound, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "Key not found"}, body)
		},
	},
	{
		"Sending request with incorrect JSON field",
		fmt.Sprintf(
			"{\"zoptsi\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeZAddSet1")),
			1.0,
			base64.StdEncoding.EncodeToString([]byte("setKey1")),
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "incorrect JSON payload"}, body)
		},
	},
	{
		"Missing key field",
		fmt.Sprintf(
			"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f}}",
			base64.StdEncoding.EncodeToString([]byte("safeZAddSet1")),
			1.0,
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "invalid key"}, body)
		},
	},
	{
		"Send plain text instead of base64 encoded",
		fmt.Sprintf(
			"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"setKey1\"}}",
			base64.StdEncoding.EncodeToString([]byte("safeZAddSet1")),
			1.0,
		),
		func(t *testing.T, testCase string, status int, body map[string]interface{}) {
			requireResponseStatus(t, testCase, http.StatusBadRequest, status)
			requireResponseFieldsEqual(
				t, testCase, map[string]interface{}{"error": "illegal base64 data at input byte 4"}, body)
		},
	},
}

func testSafeZAddHandler(t *testing.T, safeZAddHandler SafeZAddHandler) {
	prefixPattern := "SafeZAddHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/safe/zadd"
	handlerFunc := func(res http.ResponseWriter, req *http.Request) {
		safeZAddHandler.SafeZAdd(res, req, nil)
	}
	for _, tc := range safeZAddHandlerTestCases {
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
