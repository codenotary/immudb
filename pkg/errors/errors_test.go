/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package errors_test

import (
	stdErrors "errors"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func Test_Immuerror(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "cause error"
	err := errors.New(cause)

	require.Error(t, err)
	require.Equal(t, err.Error(), cause)
	require.Equal(t, err.Message(), cause)
	require.Equal(t, err.Code(), errors.CodInternalError)
	require.ErrorIs(t, err, err.Cause())
	require.Equal(t, err.RetryDelay(), int32(0))
	require.NotNil(t, err.Stack())
}

func Test_WrappingError(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "std error"
	err := errors.New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedError := errors.Wrap(err, wrappedMessage)
	wrappedNilError := errors.Wrap(nil, "msg")

	require.Nil(t, wrappedNilError)

	require.Error(t, wrappedError)
	require.Equal(t, wrappedError.Error(), fmt.Sprintf("%s: %s", wrappedMessage, cause))
	require.Equal(t, wrappedError.Message(), wrappedMessage)
	require.Equal(t, wrappedError.Code(), errors.CodInternalError)
	require.Equal(t, wrappedError.RetryDelay(), int32(0))
	require.NotNil(t, wrappedError.Stack())
}

func Test_WrappingImmuerror(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "cause error"
	err := errors.New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedError := errors.Wrap(err, wrappedMessage)

	require.Error(t, wrappedError)
	require.Equal(t, wrappedError.Error(), fmt.Sprintf("%s: %s", wrappedMessage, cause))
	require.Equal(t, wrappedError.Message(), wrappedMessage)
	require.Equal(t, wrappedError.Code(), errors.CodInternalError)
	require.Equal(t, wrappedError.Cause(), err)
	require.Equal(t, wrappedError.RetryDelay(), int32(0))
	require.NotNil(t, wrappedError.Stack())
}

func Test_ImmuerrorIs(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "cause error"
	err := errors.New(cause).WithCode(errors.CodInternalError)
	wrappedMessage := "this is the message I want to show"
	wrappedError := errors.Wrap(err, wrappedMessage)

	err2 := errors.New(cause).WithCode(errors.CodInternalError)
	wrappedError2 := errors.Wrap(err2, wrappedMessage)

	errStd := errors.New("stdError")

	require.True(t, stdErrors.Is(wrappedError, wrappedError2))
	require.False(t, stdErrors.Is(wrappedError, err2))
	require.False(t, stdErrors.Is(wrappedError, err))
	require.False(t, stdErrors.Is(wrappedError, errStd))
	require.False(t, stdErrors.Is(errStd, wrappedError))
}

func Test_CauseComparisonWrappedError(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "std error"
	err := errors.New(cause)
	wrappedMessage := "this is the message I want to show"
	wrappedWrappedMessage := "change idea"
	wrappedError := errors.Wrap(err, wrappedMessage).WithCode(errors.CodSqlclientUnableToEstablishSqlConnection).WithRetryDelay(123)
	wrappedWrappedError := errors.Wrap(wrappedError, wrappedWrappedMessage).WithCode(errors.CodSqlclientUnableToEstablishSqlConnection)
	immuError := errors.New("immu error")
	immuErrorWithCode := errors.New("immu error").WithCode(errors.CodSqlclientUnableToEstablishSqlConnection)

	require.True(t, stdErrors.Is(wrappedError.Cause(), err))
	require.True(t, stdErrors.Is(wrappedError, wrappedWrappedError))
	require.False(t, stdErrors.Is(wrappedError, immuError))
	require.True(t, stdErrors.Is(wrappedWrappedError, immuErrorWithCode))
}

func Test_CauseComparisonError(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "std error"
	err := errors.New(cause).WithCode(errors.CodSqlclientUnableToEstablishSqlConnection)

	err2 := errors.New(cause).WithCode(errors.CodSqlclientUnableToEstablishSqlConnection).WithRetryDelay(123)
	wrappedError := errors.Wrap(err2, "msg").WithCode(errors.CodSqlclientUnableToEstablishSqlConnection)
	err3 := errors.New(cause).WithCode(errors.CodSqlclientUnableToEstablishSqlConnection).WithRetryDelay(321)

	require.True(t, stdErrors.Is(err2.Cause(), err))
	require.True(t, stdErrors.Is(err2, err))
	require.True(t, stdErrors.Is(err2, wrappedError))
	require.True(t, stdErrors.Is(err2, err))
	require.True(t, stdErrors.Is(err2, err3))
}

func Test_WrappingImmuerrorWithKnowCode(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")

	cause := "cause error"
	err := errors.New(cause)
	wrappedError := errors.Wrap(err, server.ErrUserNotActive)

	require.Error(t, wrappedError)
	require.Equal(t, wrappedError.Error(), fmt.Sprintf("%s: %s", server.ErrUserNotActive, cause))
	require.Equal(t, wrappedError.Message(), server.ErrUserNotActive)
	require.Equal(t, wrappedError.Code(), errors.CodSqlserverRejectedEstablishmentOfSqlconnection)
	require.Equal(t, wrappedError.Cause(), err)
	require.Equal(t, wrappedError.RetryDelay(), int32(0))
	require.NotNil(t, wrappedError.Stack())
}
