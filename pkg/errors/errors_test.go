/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_Immuerror(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "cause error"
	err := New(cause)

	require.Error(t, err)
	require.Equal(t, err.Error(), cause)
	require.Equal(t, err.Message(), cause)
	require.Equal(t, err.Code(), InternalError)
	require.Equal(t, err.Cause(), err)
	require.Equal(t, err.RetryDelay(), int32(0))
	require.NotNil(t, err.Stack())

}

func Test_WrappingError(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "std error"
	err := errors.New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedError := Wrap(err, wrappedMessage)
	wrappedNilError := Wrap(nil, "msg")

	require.Nil(t, wrappedNilError)

	require.Error(t, wrappedError)
	require.Equal(t, wrappedError.Error(), fmt.Sprintf("%s: %s", wrappedMessage, cause))
	require.Equal(t, wrappedError.Message(), wrappedMessage)
	require.Equal(t, wrappedError.Code(), InternalError)
	require.Equal(t, wrappedError.RetryDelay(), int32(0))
	require.NotNil(t, wrappedError.Stack())
}

func Test_WrappingImmuerror(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "cause error"
	err := New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedError := Wrap(err, wrappedMessage)

	require.Error(t, wrappedError)
	require.Equal(t, wrappedError.Error(), fmt.Sprintf("%s: %s", wrappedMessage, cause))
	require.Equal(t, wrappedError.Message(), wrappedMessage)
	require.Equal(t, wrappedError.Code(), InternalError)
	require.Equal(t, wrappedError.Cause(), err)
	require.Equal(t, wrappedError.RetryDelay(), int32(0))
	require.NotNil(t, wrappedError.Stack())
}

func Test_ImmuerrorIs(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "cause error"
	err := New(cause).WithCode(InternalError)
	wrappedMessage := "this is the message I want to show"
	wrappedError := Wrap(err, wrappedMessage)

	err2 := New(cause).WithCode(InternalError)
	wrappedError2 := Wrap(err2, wrappedMessage)

	errStd := errors.New("stdError")

	require.True(t, errors.Is(wrappedError, wrappedError2))
	require.False(t, errors.Is(wrappedError, err2))
	require.False(t, errors.Is(wrappedError, err))
	require.False(t, errors.Is(wrappedError, errStd))
	require.False(t, errors.Is(errStd, wrappedError))
}

func Test_CauseComparisonWrappedError(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "std error"
	err := errors.New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedWrappedMessage := "change idea"
	wrappedError := Wrap(err, wrappedMessage).WithCode(SqlclientUnableToEstablishSqlConnection).WithRetryDelay(123)
	wrappedWrappedError := Wrap(wrappedError, wrappedWrappedMessage).WithCode(SqlclientUnableToEstablishSqlConnection)
	immuError := New("immu error")
	immuErrorWithCode := New("immu error").WithCode(SqlclientUnableToEstablishSqlConnection)

	require.True(t, errors.Is(wrappedError.Cause(), err))
	require.True(t, errors.Is(wrappedError, wrappedWrappedError))
	require.False(t, errors.Is(wrappedError, immuError))
	require.True(t, errors.Is(wrappedWrappedError, immuErrorWithCode))
}

func Test_CauseComparisonError(t *testing.T) {
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("LOG_LEVEL")

	cause := "std error"
	err := errors.New(cause)

	err2 := New(cause).WithCode(SqlclientUnableToEstablishSqlConnection).WithRetryDelay(123)
	wrappedError := Wrap(err2, "msg").WithCode(SqlclientUnableToEstablishSqlConnection)
	err3 := New(cause).WithCode(SqlclientUnableToEstablishSqlConnection).WithRetryDelay(321)

	require.True(t, errors.Is(err2.Cause(), err))
	require.True(t, errors.Is(err2, err))
	require.True(t, errors.Is(err2, wrappedError))
	require.True(t, errors.Is(err2, err))
	require.True(t, errors.Is(err2, err3))
}
