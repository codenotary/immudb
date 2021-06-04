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

type ImmuError interface {
	Error() string
	Cause() string
	Stack() string
	Code() string
	RetryDelay() int32
}

func New(message string) *immuError {
	return &immuError{
		msg: message,
	}
}

type immuError struct {
	cause      string
	code       string
	msg        string
	retryDelay int32
	stack      string
}

func FromError(err error) ImmuError {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if ok {
		ie := New(st.Message())
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

func (f *immuError) Code() string {
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

func (e *immuError) WithCode(code string) *immuError {
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
