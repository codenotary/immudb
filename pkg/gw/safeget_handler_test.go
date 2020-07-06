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

	"github.com/codenotary/immudb/pkg/client"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func testSafeGetHandler(t *testing.T, mux *runtime.ServeMux, ic immuclient.ImmuClient) {
	prefixPattern := "SafeGetHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/item/safe/get"
	for _, tc := range safeGetHandlerTestCases(mux, ic) {
		handlerFunc := func(res http.ResponseWriter, req *http.Request) {
			tc.safeGetHandler.Safeget(res, req, nil)
		}
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

type safeGetHandlerTestCase struct {
	name           string
	safeGetHandler SafegetHandler
	payload        string
	testFunc       func(*testing.T, string, int, map[string]interface{})
}

func safeGetHandlerTestCases(mux *runtime.ServeMux, ic immuclient.ImmuClient) []safeGetHandlerTestCase {
	rt := newDefaultRuntime()
	json := newDefaultJSON()
	sgh := NewSafegetHandler(mux, ic, rt, json)
	icd := client.DefaultClient()
	safeGetWErr := func(context.Context, []byte, ...grpc.CallOption) (*client.VerifiedItem, error) {
		return nil, errors.New("safeget error")
	}
	validKey := base64.StdEncoding.EncodeToString([]byte("setKey1"))
	validPayload := fmt.Sprintf("{\"key\": \"%s\"}", validKey)

	return []safeGetHandlerTestCase{
		{
			"Sending correct request",
			sgh,
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusOK, status)
				requireResponseFields(
					t, testCase, []string{"index", "key", "value", "time"}, body)
				requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
			},
		},
		{
			"Sending incorrect json field",
			sgh,
			fmt.Sprintf("{\"keyX\": \"%s\"}", validKey),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				expected := map[string]interface{}{"error": "invalid key"}
				requireResponseFieldsEqual(t, testCase, expected, body)
			},
		},
		{
			"Sending plain text instead of base64 encoded",
			sgh,
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
			sgh,
			`{}`,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				expected := map[string]interface{}{"error": "invalid key"}
				requireResponseFieldsEqual(t, testCase, expected, body)
			},
		},
		{
			"AnnotateContext error",
			NewSafegetHandler(mux, ic, newTestRuntimeWithAnnotateContextErr(), json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "annotate context error"}, body)
			},
		},
		{
			"Safeget error",
			NewSafegetHandler(mux, &immuClientMock{ImmuClient: icd, safeGet: safeGetWErr}, rt, json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "safeget error"}, body)
			},
		},
		{
			"JSON marshal error",
			NewSafegetHandler(mux, ic, rt, newTestJSONWithMarshalErr()),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "JSON marshal error"}, body)
			},
		},
	}
}
