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
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultTimeFormat is the time format to use for JSON output
	DefaultTimeFormat = "2006-01-02T15:04:05.000000Z07:00"

	// errInvalidTypeMsg message implies an arg cannot be serialized to json
	errInvalidTypeMsg = "cannot serialize arg(s) to json"

	// callerOffset is the stack frame offset in the call stack for the caller
	callerOffset = 4

	// LogFormatText is the log format to use for TEXT output
	LogFormatText = "text"

	// LogFormatJSON is the log format to use for JSON output
	LogFormatJSON = "json"
)

var (
	// defaultOutput is used as the default log output.
	defaultOutput io.Writer = os.Stderr
)

var _ Logger = (*JsonLogger)(nil)

// JsonLogger is a logger implementation for json logging.
type JsonLogger struct {
	name       string
	timeFormat string

	timeFnc TimeFunc

	mutex  sync.Mutex
	writer *writer

	level int32
}

// NewJSONLogger returns a json logger.
func NewJSONLogger(opts *Options) (*JsonLogger, error) {
	if opts == nil {
		opts = &Options{}
	}

	output := opts.Output
	if output == nil {
		output = defaultOutput
	}

	if opts.LogFile != "" {
		out, err := setup(opts.LogFile)
		if err != nil {
			return nil, err
		}
		output = out
	}

	l := &JsonLogger{
		name:       opts.Name,
		timeFormat: DefaultTimeFormat,
		timeFnc:    time.Now,
		writer:     newWriter(output),
	}

	if opts.TimeFnc != nil {
		l.timeFnc = opts.TimeFnc
	}
	if opts.TimeFormat != "" {
		l.timeFormat = opts.TimeFormat
	}

	l.level = int32(opts.Level)
	return l, nil
}

func (l *JsonLogger) logWithFmt(name string, level LogLevel, msg string, args ...interface{}) {
	msgStr := fmt.Sprintf(msg, args...)
	l.log(name, level, msgStr)
}

// Log a message and a set of key/value pairs for a given level.
func (l *JsonLogger) log(name string, level LogLevel, msg string, args ...interface{}) {
	minLevel := LogLevel(atomic.LoadInt32(&l.level))
	if level < minLevel {
		return
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	t := l.timeFnc()

	l.logJSON(t, name, level, msg, args...)

	l.writer.Flush()
}

func (l *JsonLogger) logJSON(t time.Time, name string, level LogLevel, msg string, args ...interface{}) {
	vals := l.getVals(t, name, level, msg)

	if args != nil && len(args) > 0 {
		for i := 0; i < len(args); i = i + 2 {
			val := args[i+1]
			switch sv := val.(type) {
			case error:
				switch sv.(type) {
				case json.Marshaler, encoding.TextMarshaler:
				default:
					val = sv.Error()
				}
			}

			var key string

			switch st := args[i].(type) {
			case string:
				key = st
			default:
				key = fmt.Sprintf("%s", st)
			}
			vals[key] = val
		}
	}

	err := json.NewEncoder(l.writer).Encode(vals)
	if err != nil {
		if _, ok := err.(*json.UnsupportedTypeError); ok {
			vals := l.getVals(t, name, level, msg)
			vals["warn"] = errInvalidTypeMsg

			json.NewEncoder(l.writer).Encode(vals)
		}
	}
}

func (l *JsonLogger) getVals(t time.Time, name string, level LogLevel, msg string) map[string]interface{} {
	vals := map[string]interface{}{
		"message":   msg,
		"timestamp": t.Format(l.timeFormat),
	}

	if name != "" {
		vals["module"] = name
	}

	levelStr := levelToString[level]
	if levelStr == "" {
		levelStr = "all"
	}

	vals["level"] = levelStr

	if pc, file, line, ok := runtime.Caller(callerOffset + 1); ok {
		vals["caller"] = fmt.Sprintf("%s:%d", file, line)
		if ok {
			f := runtime.FuncForPC(pc)
			vals["component"] = f.Name()
		}
	}

	return vals
}

// Debugf prints the message and args at DEBUG level
func (l *JsonLogger) Debug(msg string, args ...interface{}) {
	l.log(l.Name(), LogDebug, msg, args...)
}

// Infof prints the message and args at INFO level
func (l *JsonLogger) Info(msg string, args ...interface{}) {
	l.log(l.Name(), LogInfo, msg, args...)
}

// Warningf prints the message and args at WARN level
func (l *JsonLogger) Warning(msg string, args ...interface{}) {
	l.log(l.Name(), LogWarn, msg, args...)
}

// Errorf prints the message and args at ERROR level
func (l *JsonLogger) Error(msg string, args ...interface{}) {
	l.log(l.Name(), LogError, msg, args...)
}

// Debugf prints the message and args at DEBUG level
func (l *JsonLogger) Debugf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogDebug, msg, args...)
}

// Infof prints the message and args at INFO level
func (l *JsonLogger) Infof(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogInfo, msg, args...)
}

// Warningf prints the message and args at WARN level
func (l *JsonLogger) Warningf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogWarn, msg, args...)
}

// Errorf prints the message and args at ERROR level
func (l *JsonLogger) Errorf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogError, msg, args...)
}

// Update the logging level
func (l *JsonLogger) SetLogLevel(level LogLevel) {
	atomic.StoreInt32(&l.level, int32(level))
}

// Name returns the loggers name
func (i *JsonLogger) Name() string {
	return i.name
}

// Close the logger
func (i *JsonLogger) Close() error {
	return i.writer.Close()
}

type writer struct {
	b   bytes.Buffer
	out io.Writer
}

func newWriter(w io.Writer) *writer {
	return &writer{out: w}
}

func (w *writer) Flush() (err error) {
	var unwritten = w.b.Bytes()
	_, err = w.out.Write(unwritten)
	w.b.Reset()
	return err
}

func (w *writer) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

func (w *writer) Close() error {
	switch t := w.out.(type) {
	case *os.File:
		err := w.Flush()
		if err != nil {
			return err
		}
		return t.Close()
	default:
		return w.Flush()
	}
}
