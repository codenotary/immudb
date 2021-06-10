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
	"runtime/debug"
)

type Error interface {
	Error() string
	Message() string
	Cause() error
	Code() Code
	RetryDelay() int32
	Stack() string
}

func New(message string) *immuError {

	e := &immuError{
		code:  CodInternalError,
		msg:   message,
		stack: string(debug.Stack()),
	}

	return e
}

type immuError struct {
	code       Code
	msg        string
	stack      string
	retryDelay int32
}

func (f *immuError) Error() string {
	return f.msg
}

func (f *immuError) Message() string {
	return f.msg
}

func (f *immuError) Cause() error {
	return f
}

func (f *immuError) Code() Code {
	return f.code
}

func (f *immuError) RetryDelay() int32 {
	return f.retryDelay
}

func (f *immuError) Stack() string {
	return f.stack
}

func (e *immuError) WithCode(code Code) *immuError {
	e.code = code
	return e
}

func (e *immuError) WithRetryDelay(delay int32) *immuError {
	e.retryDelay = delay
	return e
}

func (e *immuError) Is(target error) bool {
	switch t := target.(type) {
	case *immuError:
		return compare(e, t)
	case *wrappedError:
		return compare(e, t)
	default:
		return e.Cause().Error() == target.Error()
	}
}

func compare(e Error, t Error) bool {
	if e.Code() != CodInternalError || t.Code() != CodInternalError {
		return e.Code() == t.Code()
	}
	return e.Message() == t.Message() && e.Cause().Error() == t.Cause().Error()
}
