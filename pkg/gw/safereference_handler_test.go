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

func testSafeReferenceHandler(t *testing.T, mux *runtime.ServeMux, ic immuclient.ImmuClient) {
	prefixPattern := "SafeReferenceHandler - Test case: %s"
	method := "POST"
	path := "/v1/immurestproxy/safe/reference"
	for _, tc := range safeReferenceHandlerTestCases(mux, ic) {
		handlerFunc := func(res http.ResponseWriter, req *http.Request) {
			tc.safeReferenceHandler.SafeReference(res, req, nil)
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

type safeReferenceHandlerTestCase struct {
	name                 string
	safeReferenceHandler SafeReferenceHandler
	payload              string
	testFunc             func(*testing.T, string, int, map[string]interface{})
}

func safeReferenceHandlerTestCases(mux *runtime.ServeMux, ic immuclient.ImmuClient) []safeReferenceHandlerTestCase {
	rt := newDefaultRuntime()
	json := json.DefaultJSON()
	srh := NewSafeReferenceHandler(mux, ic, rt, json)
	icd := client.DefaultClient()
	safeReferenceWErr := func(context.Context, []byte, []byte) (*client.VerifiedIndex, error) {
		return nil, errors.New("safereference error")
	}
	validRefKey := base64.StdEncoding.EncodeToString([]byte("safeReferenceKey1"))
	validKey := base64.StdEncoding.EncodeToString([]byte("setKey1"))
	validPayload := fmt.Sprintf(
		"{\"ro\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
		validRefKey,
		validKey,
	)

	return []safeReferenceHandlerTestCase{
		{
			"Sending correct request",
			srh,
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusOK, status)
				requireResponseFieldsTrue(t, testCase, []string{"verified"}, body)
			},
		},
		{
			"Sending correct request with non-existent key",
			srh,
			fmt.Sprintf(
				"{\"ro\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
				validRefKey,
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
			srh,
			fmt.Sprintf(
				"{\"data\": {\"reference\": \"%s\", \"key\": \"%s\"}}",
				validRefKey,
				validKey,
			),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "incorrect JSON payload"}, body)
			},
		},
		{
			"Missing Key field",
			srh,
			fmt.Sprintf(
				"{\"ro\": {\"reference\": \"%s\"}}",
				validRefKey,
			),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "invalid key"}, body)
			},
		},
		{
			"Sending plain text instead of base64 encoded",
			srh,
			fmt.Sprintf(
				"{\"ro\": {\"reference\": \"safeReferenceKey1\", \"key\": \"%s\"}}",
				validKey,
			),
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusBadRequest, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "illegal base64 data at input byte 16"}, body)
			},
		},
		{
			"AnnotateContext error",
			NewSafeReferenceHandler(mux, ic, newTestRuntimeWithAnnotateContextErr(), json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "annotate context error"}, body)
			},
		},
		{
			"SafeReference error",
			NewSafeReferenceHandler(mux, &clienttest.ImmuClientMock{ImmuClient: icd, SafeReferenceF: safeReferenceWErr}, rt, json),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "safereference error"}, body)
			},
		},
		{
			"JSON marshal error",
			NewSafeReferenceHandler(mux, ic, rt, newTestJSONWithMarshalErr()),
			validPayload,
			func(t *testing.T, testCase string, status int, body map[string]interface{}) {
				requireResponseStatus(t, testCase, http.StatusInternalServerError, status)
				requireResponseFieldsEqual(
					t, testCase, map[string]interface{}{"error": "JSON marshal error"}, body)
			},
		},
	}
}
