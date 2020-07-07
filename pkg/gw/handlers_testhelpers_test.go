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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/json"
	"github.com/stretchr/testify/require"
)

func testHandler(
	t *testing.T,
	name string,
	method string,
	path string,
	body string,
	handlerFunc func(http.ResponseWriter, *http.Request),
	testFunc func(*testing.T, string, int, map[string]interface{}),
) error {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(method, path, strings.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	handler := http.HandlerFunc(handlerFunc)
	handler.ServeHTTP(w, req)
	testCase := fmt.Sprintf("%s - %s %s %s - ", name, method, path, body)
	respBytes := w.Body.Bytes()
	var respBody map[string]interface{}
	if err := json.DefaultJSON().Unmarshal(respBytes, &respBody); err != nil {
		return fmt.Errorf(
			"%s - error unmarshaling JSON from response %s", testCase, respBytes)
	}
	testFunc(t, testCase, w.Code, respBody)
	return nil
}

func newTestJSONWithMarshalErr() json.JSON {
	return &json.StandardJSON{
		MarshalF: func(v interface{}) ([]byte, error) {
			return nil, errors.New("JSON marshal error")
		},
	}
}

func requireResponseStatus(
	t *testing.T,
	testCase string,
	expected int,
	actual int,
) {
	require.Equal(
		t,
		expected,
		actual,
		"%sexpected HTTP status %d, actual %d", testCase, expected, actual)
}

func getMissingResponseFieldPattern(testCase string) string {
	return testCase + "\"%s\" field is missing from response %v"
}

func requireResponseFields(
	t *testing.T,
	testCase string,
	fields []string,
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	for _, field := range fields {
		_, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
	}
}

func requireResponseFieldsTrue(
	t *testing.T,
	testCase string,
	fields []string,
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	isFalsePattern := testCase + "\"%s\" field is false in response %v"
	for _, field := range fields {
		fieldValue, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
		require.True(t, fieldValue.(bool), isFalsePattern, field, body)
	}
}

func requireResponseFieldsEqual(
	t *testing.T,
	testCase string,
	fields map[string]interface{},
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	notEqPattern := testCase +
		"expected response %v to have field \"%s\" = \"%v\", but actual field value is \"%v\""
	for field, expected := range fields {
		fieldValue, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
		require.Equal(
			t, expected, fieldValue, notEqPattern, body, field, expected, fieldValue)
	}
}
