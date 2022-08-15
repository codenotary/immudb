/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package logger

import (
	"io"
	"os"
	"strings"
	"time"
)

// LogLevel ...
type LogLevel int8

// Log levels
const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarn
	LogError
)

// Logger ...
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
}

func logLevelFromEnvironment() LogLevel {
	logLevel, _ := os.LookupEnv("LOG_LEVEL")
	switch strings.ToLower(logLevel) {
	case "error":
		return LogError
	case "warn":
		return LogWarn
	case "info":
		return LogInfo
	case "debug":
		return LogDebug
	}
	return LogInfo
}

type (
	TimeFunc = func() time.Time

	// Options can be used to configure a new logger.
	Options struct {
		// Name of the subsystem to prefix logs with
		Name string

		// The threshold for the logger. Anything less severe is supressed
		Level LogLevel

		// Where to write the logs to. Defaults to os.Stderr if nil
		Output io.Writer

		// The time format to use instead of the default
		TimeFormat string

		// A function which is called to get the time object that is formatted using `TimeFormat`
		TimeFnc TimeFunc

		// The format in which logs will be formatted. (eg: text/json)
		LogFormat string

		// The file to write to.
		LogFile string
	}
)

// NewLogger is a factory for selecting a logger based on options
func NewLogger(opts *Options) (logger Logger, out *os.File, err error) {
	switch opts.LogFormat {
	case "json":
		if opts.LogFile != "" {
			return NewJSONFileLogger("immudb", opts.LogFile)
		}
		return NewJSONLogger(opts), nil, nil
	default:
		if opts.LogFile != "" {
			return NewFileLogger("immudb ", opts.LogFile)
		}
		return NewSimpleLogger("immudb ", defaultOutput), nil, nil
	}
}
