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
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/json"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
)

func testSafeZAddHandler(t *testing.T, mux *runtime.ServeMux, ic immuclient.ImmuClient) {
	prefixPattern := "SafeZAddHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/safe/zadd"
	for _, tc := range safeZAddHandlerTestCases(mux, ic) {
		handlerFunc := func(res http.ResponseWriter, req *http.Request) {
			tc.safeZAddHandler.SafeZAdd(res, req, nil)
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

type safeZAddHandlerTestCase struct {
	name            string
	safeZAddHandler SafeZAddHandler
	payload         string
	testFunc        func(*testing.T, string, int, map[string]interface{})
}

func safeZAddHandlerTestCases(mux *runtime.ServeMux, ic immuclient.ImmuClient) []safeZAddHandlerTestCase {
	rt := newDefaultRuntime()
	json := json.DefaultJSON()
	szh := NewSafeZAddHandler(mux, ic, rt, json)
	icd := client.DefaultClient()
	safeZAddWErr := func(context.Context, []byte, float64, []byte) (*client.VerifiedIndex, error) {
		return nil, errors.New("safezadd error")
	}

	validSet := base64.StdEncoding.EncodeToString([]byte("safeZAddSet1"))
	validKey := base64.StdEncoding.EncodeToString([]byte("setKey1"))
	validPayload := fmt.Sprintf(
		"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
		validSet,
		1.0,
		validKey,
	)

	return []safeZAddHandlerTestCase{
		{
			"Sending correct request",
			szh,
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusOK, status)
				requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
			},
		},
		{
			"Sending request with non-existent key",
			szh,
			fmt.Sprintf(
				"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
				validSet,
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
			szh,
			fmt.Sprintf(
				"{\"zoptsi\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"%s\"}}",
				validSet,
				1.0,
				validKey,
			),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "incorrect JSON payload"}, body)
			},
		},
		{
			"Missing key field",
			szh,
			fmt.Sprintf(
				"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f}}",
				validSet,
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
			szh,
			fmt.Sprintf(
				"{\"zopts\": {\"set\": \"%s\", \"score\": %.1f, \"key\": \"setKey1\"}}",
				validSet,
				1.0,
			),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "illegal base64 data at input byte 4"}, body)
			},
		},
		{
			"AnnotateContext error",
			NewSafeZAddHandler(mux, ic, newTestRuntimeWithAnnotateContextErr(), json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "annotate context error"}, body)
			},
		},
		{
			"SafeZAdd error",
			NewSafeZAddHandler(mux, &clienttest.ImmuClientMock{ImmuClient: icd, SafeZAddF: safeZAddWErr}, rt, json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "safezadd error"}, body)
			},
		},
		{
			"JSON marshal error",
			NewSafeZAddHandler(mux, ic, rt, newTestJSONWithMarshalErr()),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "JSON marshal error"}, body)
			},
		},
	}
}
