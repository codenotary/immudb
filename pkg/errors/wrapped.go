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

type wrappedError struct {
	cause error
	msg   string
}

func Wrap(err error, message string) *wrappedError {
	if err == nil {
		return nil
	}

	_, ok := err.(*immuError)
	if !ok {
		err = New(err.Error())
	}
	return &wrappedError{
		cause: err,
		msg:   message,
	}
}

func (w *wrappedError) Error() string {
	return w.msg + ": " + w.cause.Error()
}

func (w *wrappedError) Cause() error {
	return w.cause
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
