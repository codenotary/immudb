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

package logger

import (
	"errors"
	"io"
	"os"
	"strings"
	"time"
)

var (
	ErrInvalidLoggerType = errors.New("invalid logger type")

	levelToString = map[LogLevel]string{
		LogDebug: "debug",
		LogInfo:  "info",
		LogWarn:  "warn",
		LogError: "error",
	}
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
	Close() error
}

func LogLevelFromEnvironment() LogLevel {
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
func NewLogger(opts *Options) (logger Logger, err error) {
	switch opts.LogFormat {
	case LogFormatJSON:
		return NewJSONLogger(opts)
	case LogFormatText:
		if opts.LogFile != "" {
			logger, _, err = NewFileLogger(opts.Name, opts.LogFile)
			return logger, err
		}
		return NewSimpleLogger(opts.Name, defaultOutput), nil
	default:
		return nil, ErrInvalidLoggerType
	}
}
