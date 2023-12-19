/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package errors

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/status"
)

// ImmuError SDK immudb error interface.
// _, err = client.StreamSet(ctx, kvs)
// code := err.(errors.ImmuError).Code()) //errors.CodDataException
type ImmuError interface {
	//Error return the message.
	Error() string
	//Cause is the inner error cause.
	Cause() string
	//Stack is present if immudb is running with LEVEL_INFO=debug
	Stack() string
	//Code is the immudb error code
	Code() Code
	//RetryDelay if present the error is retryable after N milliseconds
	RetryDelay() int32
}

func New(message string) *immuError {
	return &immuError{
		msg:  message,
		code: CodInternalError,
	}
}

type immuError struct {
	cause      string
	code       Code
	msg        string
	retryDelay int32
	stack      string
}

func FromError(err error) ImmuError {
	if err == nil {
		return nil
	}

	if immuErr, ok := err.(ImmuError); ok {
		// Already an ImmuError instance
		return immuErr
	}

	st, ok := status.FromError(err)
	if ok {
		ie := New(st.Message())
		for _, det := range st.Details() {
			switch ele := det.(type) {
			case *schema.ErrorInfo:
				ie.WithCode(Code(ele.Code)).WithCause(ele.Cause)
			case *schema.DebugInfo:
				ie.WithStack(ele.Stack)
			case *schema.RetryInfo:
				ie.WithRetryDelay(ele.RetryDelay)
			}
		}
		return ie
	}
	return New(err.Error())
}

func (f *immuError) Error() string {
	return f.msg
}

func (f *immuError) Cause() string {
	return f.cause
}

func (f *immuError) Stack() string {
	return f.stack
}

func (f *immuError) Code() Code {
	return f.code
}

func (f *immuError) RetryDelay() int32 {
	return f.retryDelay
}

func (e *immuError) WithMessage(message string) *immuError {
	e.msg = message
	return e
}

func (e *immuError) WithCause(cause string) *immuError {
	e.cause = cause
	return e
}

func (e *immuError) WithCode(code Code) *immuError {
	e.code = code
	return e
}

func (e *immuError) WithStack(stack string) *immuError {
	e.stack = stack
	return e
}

func (e *immuError) WithRetryDelay(retry int32) *immuError {
	e.retryDelay = retry
	return e
}

func (e *immuError) Is(target error) bool {
	if target == nil {
		return false
	}
	t, ok := target.(ImmuError)
	if !ok {
		return e.Error() == target.Error()
	}
	return compare(e, t)
}

func compare(e ImmuError, t ImmuError) bool {
	if e.Code() != CodInternalError || t.Code() != CodInternalError {
		return e.Code() == t.Code()
	}
	return e.Cause() == t.Cause() && e.Error() == t.Error()
}
