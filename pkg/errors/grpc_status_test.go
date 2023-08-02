/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package errors

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestImmuError_GRPCStatus(t *testing.T) {
	ie := &immuError{
		code:       "",
		msg:        "",
		stack:      "",
		retryDelay: 0,
	}
	st := ie.GRPCStatus()

	require.NotNil(t, st)
	require.Len(t, st.Details(), 2)
}

func TestWrappedError_GRPCStatus(t *testing.T) {
	ie := &wrappedError{
		cause: New("cause"),
		msg:   "",
	}
	st := ie.GRPCStatus()

	require.NotNil(t, st)
	require.Len(t, st.Details(), 2)
}

func TestWrappedError_GRPCStatusEmptyCause(t *testing.T) {
	ie := &wrappedError{
		cause: nil,
		msg:   "",
	}
	st := ie.GRPCStatus()

	require.Nil(t, st)
}

func TestImmuError_GRPCStatusWithStack(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	ie := &immuError{
		code:       "",
		msg:        "",
		stack:      "",
		retryDelay: 0,
	}
	st := ie.GRPCStatus()

	require.NotNil(t, st)

	var errorInfo *schema.ErrorInfo = nil
	var debugInfo *schema.DebugInfo = nil
	var retryInfo *schema.RetryInfo = nil

	for _, det := range st.Details() {
		switch ele := det.(type) {
		case *schema.ErrorInfo:
			errorInfo = ele
		case *schema.DebugInfo:
			debugInfo = ele
		case *schema.RetryInfo:
			retryInfo = ele
		}
	}
	require.NotNil(t, errorInfo)
	require.NotNil(t, debugInfo)
	require.NotNil(t, retryInfo)
}
