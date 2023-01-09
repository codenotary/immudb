/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package util

import (
	"runtime"

	"github.com/codenotary/immudb/pkg/logger"
)

// PanicHandler is a function which is invoked when a panic happens
type PanicHandler func(logger.Logger, interface{})

// DefaultPanicHandlers is a list of PanicHandler(s) which will be invoked when a panic happens
var DefaultPanicHandlers = []PanicHandler{panicLogger}

// HandlePanic catches a crash and logs an error. Should to be called via defer.
func HandlePanic(l logger.Logger, handlers ...PanicHandler) {
	if r := recover(); r != nil {
		for _, fn := range DefaultPanicHandlers {
			fn(l, r)
		}
		for _, fn := range handlers {
			fn(l, r)
		}
	}
}

// panicLogger logs the stack trace when a panic occurs
func panicLogger(l logger.Logger, r interface{}) {
	const size = 64 << 10
	stacktrace := make([]byte, size)
	stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]
	if _, ok := r.(string); ok {
		l.Errorf("Observed a panic: %s\n%s", r, stacktrace)
	} else {
		l.Errorf("Observed a panic: %#v (%v)\n%s", r, r, stacktrace)
	}
}
