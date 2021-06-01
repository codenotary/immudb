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
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/status"
)

func DefaultImmuError() *ImmuError {
	return &ImmuError{
		msg: "generic error",
	}
}

func New(message string) *ImmuError {
	return &ImmuError{
		msg: message,
	}
}

type ImmuError struct {
	cause      string
	code       string
	msg        string
	retryDelay int32
	stack      string
}

func FromError(err error) *ImmuError {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if ok {
		ie := DefaultImmuError().WithMessage(st.Message())
		for _, det := range st.Details() {
			switch ele := det.(type) {
			case *schema.ErrorInfo:
				ie.WithCode(ele.Code).WithCause(ele.Cause)
			case *schema.DebugInfo:
				ie.WithStack(ele.Stack)
			case *schema.RetryInfo:
				ie.WithRetryDelay(ele.RetryDelay)
			}
		}
		return ie
	}
	return DefaultImmuError()
}

func (f *ImmuError) Error() string {
	return f.msg
}

func (f *ImmuError) Cause() string {
	return f.cause
}

func (f *ImmuError) Stack() string {
	return f.stack
}

func (f *ImmuError) Code() string {
	return f.code
}

func (f *ImmuError) RetryDelay() int32 {
	return f.retryDelay
}

func (e *ImmuError) WithMessage(message string) *ImmuError {
	e.msg = message
	return e
}

func (e *ImmuError) WithCause(cause string) *ImmuError {
	e.cause = cause
	return e
}

func (e *ImmuError) WithCode(code string) *ImmuError {
	e.code = code
	return e
}

func (e *ImmuError) WithStack(stack string) *ImmuError {
	e.stack = stack
	return e
}

func (e *ImmuError) WithRetryDelay(retry int32) *ImmuError {
	e.retryDelay = retry
	return e
}
