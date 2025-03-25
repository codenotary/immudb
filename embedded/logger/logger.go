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

const (
	LogFileFormat     = time.RFC3339
	logRotationAgeMin = time.Minute
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

		// The directory logs will be stored to.
		LogDir string

		// The file to write to.
		LogFile string

		// The time format of different log segments.
		LogFileTimeFormat string

		// The maximum size a log segment can reach before being rotated.
		LogRotationSize int

		// The maximum duration (age) of a log segment before it is rotated.
		LogRotationAge time.Duration
	}
)

// NewLogger is a factory for selecting a logger based on options
func NewLogger(opts *Options) (logger Logger, err error) {
	out := opts.Output
	if out == nil {
		out = os.Stderr
	}

	if opts.LogFile != "" {
		w, err := createLogFileWriter(opts)
		if err != nil {
			return nil, err
		}
		out = w
	}

	switch opts.LogFormat {
	case LogFormatJSON:
		optsCopy := *opts
		optsCopy.Output = out

		return NewJSONLogger(&optsCopy)
	case LogFormatText:
		return NewSimpleLogger(opts.Name, out), nil
	default:
		return nil, ErrInvalidLoggerType
	}
}
