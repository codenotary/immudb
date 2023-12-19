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

type wrappedError struct {
	cause error
	msg   string
}

func Wrap(err error, message string) *wrappedError {
	if err == nil {
		return nil
	}

	e, ok := err.(*immuError)
	if !ok {
		e = New(err.Error())
	}
	c, ok := CodeMap[message]
	if ok {
		e.code = c
	}
	return &wrappedError{
		cause: e,
		msg:   message,
	}
}

func (w *wrappedError) Error() string {
	return w.msg + ": " + w.cause.Error()
}

func (w *wrappedError) Message() string {
	return w.msg
}

func (w *wrappedError) Cause() error {
	return w.cause
}

func (w *wrappedError) Code() Code {
	return w.cause.(*immuError).code
}

func (w *wrappedError) Stack() string {
	return w.cause.(*immuError).stack
}

func (w *wrappedError) RetryDelay() int32 {
	return w.cause.(*immuError).retryDelay
}

func (w *wrappedError) WithCode(code Code) *wrappedError {
	w.cause.(*immuError).code = code
	return w
}

// WithRetryDelay specifies milliseconds needed to retry. If 0 is returned error is not retryable.
func (w *wrappedError) WithRetryDelay(delay int32) *wrappedError {
	w.cause.(*immuError).retryDelay = delay
	return w
}

func (e *wrappedError) Is(target error) bool {
	switch t := target.(type) {
	case *immuError:
		return compare(e, t)
	case *wrappedError:
		return compare(e, t)
	default:
		return e.Cause().Error() == target.Error()
	}
}
