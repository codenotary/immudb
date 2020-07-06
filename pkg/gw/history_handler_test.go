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
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
)

func testHistoryHandler(t *testing.T, mux *runtime.ServeMux, ic immuclient.ImmuClient) {
	prefixPattern := "HistoryHandler - Test case: %s"
	method := "GET"
	for _, tc := range historyHandlerTestCases(mux, ic) {
		path := "/v1/immurestproxy/history/" + tc.key
		handlerFunc := func(res http.ResponseWriter, req *http.Request) {
			var pathParams map[string]string
			if tc.key != "" {
				pathParams = map[string]string{"key": tc.key}
			}
			tc.historyHandler.History(res, req, pathParams)
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

type historyHandlerTestCase struct {
	name           string
	historyHandler HistoryHandler
	key            string
	testFunc       func(*testing.T, string, int, map[string]interface{})
}

func historyHandlerTestCases(mux *runtime.ServeMux, ic immuclient.ImmuClient) []historyHandlerTestCase {
	rt := newDefaultRuntime()
	json := newDefaultJSON()
	hh := NewHistoryHandler(mux, ic, rt, json)
	icd := client.DefaultClient()
	historyWErr := func(context.Context, []byte) (*schema.StructuredItemList, error) {
		return nil, errors.New("history error")
	}
	validKey := base64.StdEncoding.EncodeToString([]byte("setKey1"))

	return []historyHandlerTestCase{
		{
			"Sending correct request",
			hh,
			validKey,
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
			hh,
			base64.StdEncoding.EncodeToString([]byte("historyNonExistentKey1")),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusOK, status)
				notEmptyMsg := "%sexpected empty response, actual response length is %d"
				require.Equal(t, 0, len(body), notEmptyMsg, testCase, len(body))
			},
		},
		{
			"Missing key path param",
			hh,
			"",
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "missing parameter key"}, body)
			},
		},
		{
			"Sending plain text instead of base64 encoded",
			hh,
			"setKey1",
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				expectedError :=
					"type mismatch, parameter: key, error: illegal base64 data at input byte 4"
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": expectedError}, body)
			},
		},
		{
			"AnnotateContext error",
			NewHistoryHandler(mux, ic, newTestRuntimeWithAnnotateContextErr(), json),
			validKey,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "annotate context error"}, body)
			},
		},
		{
			"History error",
			NewHistoryHandler(mux, &immuClientMock{ImmuClient: icd, history: historyWErr}, rt, json),
			validKey,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "history error"}, body)
			},
		},
		{
			"JSON marshal error",
			NewHistoryHandler(mux, ic, rt, newTestJSONWithMarshalErr()),
			validKey,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "JSON marshal error"}, body)
			},
		},
	}
}
