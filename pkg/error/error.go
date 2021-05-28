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

package error

import (
	"runtime"
)

func New(setters ...Option) error {

	// Default Options
	e := &immuError{
		code:  0,
		msg:   "",
		stack: nil,
	}

	for _, setter := range setters {
		setter(e)
	}

	return e
}

type immuError struct {
	code  int
	msg   string
	stack []uintptr
}

func (f *immuError) Error() string { return f.msg }

type wrappedError struct {
	cause error
	msg   string
}

func (w *wrappedError) Error() string { return w.msg + ": " + w.cause.Error() }
func (w *wrappedError) Cause() error  { return w.cause }

func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	_, ok := err.(*immuError)
	if !ok {
		err = New(Msg(err.Error()))
	}
	return &wrappedError{
		cause: err,
		msg:   message,
	}
}

func callers() []uintptr {
	var cl [32]uintptr
	n := runtime.Callers(3, cl[:])
	return cl[0:n]
}

type Option func(s *immuError)

func Code(code int) Option {
	return func(args *immuError) {
		args.code = code
	}
}
func Msg(msg string) Option {
	return func(args *immuError) {
		args.msg = msg
	}
}
