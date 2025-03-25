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

package errors

import (
	"runtime/debug"
)

// On server side errors need to be returned using following interface.
//
// An error can be created with:
//
// errors.New
// errors.Wrap
//
// if read == 0 && err == io.EOF {
//    return errors.New(ErrReaderIsEmpty).WithCode(errors.CodInvalidParameterValue)
// }
//
// or
//
// u, err := s.getValidatedUser(r.user, r.Password)
// if err != nil {
//    return nil, errors.Wrap(err, "invalid user name or password").WithCode(errors.CodSqlserverRejectedEstablishmentOfSqlconnection)
// }
//
// If the error is registered inside an init function like it's possible to skip the inline code definition.
//
// func init() {
//    ...
//    errors.CodeMap[ErrMaxValueLenExceeded] = errors.CodDataException
//    ...
// }
//
// Errors are converted in the transportation in gRPC status. When adding a new error code it's required to add an entry in gRPC error map. See map.go
//
// func mapGRPcErrorCode(code Code) codes.Code
// In order to avoid dependency between client and server package SDK contains proper error codes definitions, like server.
//
// Both SDK ImmuError and server Error can be compared with errors.Is utility.
//
type Error interface {
	Error() string
	Message() string
	Cause() error
	Code() Code
	RetryDelay() int32
	Stack() string
}

func New(message string) *immuError {
	c, ok := CodeMap[message]
	if !ok {
		c = CodInternalError
	}
	e := &immuError{
		code:  c,
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
