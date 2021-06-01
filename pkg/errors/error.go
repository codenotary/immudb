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

func New(message string) *immuError {

	e := &immuError{
		code:  Unknown,
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

func (e *immuError) WithCode(code Code) *immuError {
	e.code = code
	return e
}

func (e *immuError) WithRetryDelay(delay int32) *immuError {
	e.retryDelay = delay
	return e
}
