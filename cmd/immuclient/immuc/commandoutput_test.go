/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package immuc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func toJsonString(t *testing.T, obj interface{}) string {
	data, err := json.MarshalIndent(obj, "", "  ")
	require.NoError(t, err)
	return string(data)
}

func TestErrorOutput(t *testing.T) {
	var o CommandOutput = &errorOutput{
		err: "Test error",
	}

	require.Equal(t, "Test error", o.Plain())
	require.Equal(t, "Test error", o.ValueOnly())
	require.JSONEq(t, `{"error": "Test error"}`, toJsonString(t, o.Json()))
}

func TestResultOutput(t *testing.T) {
	var o CommandOutput = &resultOutput{
		Result:  "Test result",
		Warning: "Test warning",
	}

	require.Equal(t, "Test result\nTest warning", o.Plain())
	require.Equal(t, "Test result", o.ValueOnly())
	require.JSONEq(t, `{"result": "Test result", "warning": "Test warning"}`, toJsonString(t, o.Json()))
}

func TestResultOutputNoWarning(t *testing.T) {
	var o CommandOutput = &resultOutput{
		Result: "Test result",
	}

	require.Equal(t, "Test result", o.Plain())
	require.Equal(t, "Test result", o.ValueOnly())
	require.JSONEq(t, `{"result": "Test result"}`, toJsonString(t, o.Json()))
}
