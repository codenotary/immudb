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
	"bytes"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultTimeFormat is the time format to use for JSON output
	DefaultTimeFormat = "2006-01-02T15:04:05.000000Z07:00"

	// errInvalidTypeMsg message implies an arg cannot be serialized to json
	errInvalidTypeMsg = "cannot serialize arg(s) to json"

	// defaultCallerOffset is the stack frame offset in the call stack for the caller
	defaultCallerOffset = 4
)

var (
	// defaultOutput is used as the default log output.
	defaultOutput io.Writer = os.Stderr

	levelToString = map[LogLevel]string{
		LogDebug: "DEBUG",
		LogInfo:  "INFO",
		LogWarn:  "WARN",
		LogError: "ERROR",
	}
)

var _ Logger = &intLogger{}

// intLogger is a logger implementation for json logging.
type (
	writer interface {
		Flush() error
		Writer() io.Writer
		Close() error
		Write([]byte) (int, error)
	}

	intLogger struct {
		name      string
		logFormat string

		timeFormat string
		timeFnc    TimeFunc

		mutex  sync.Mutex
		writer writer

		level int32
	}
)

// newLogger returns a json logger.
func newLogger(opts *Options) (*intLogger, error) {
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

	var w writer
	switch opts.LogFormat {
	case "json":
		w = newWriter(output)
	case "text":
		w = newWriter(newLogWriter(opts.Name, output))
	}

	l := &intLogger{
		name:       opts.Name,
		timeFormat: DefaultTimeFormat,
		timeFnc:    time.Now,
		writer:     w,
		logFormat:  opts.LogFormat,
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

func (l *intLogger) logWithFmt(name string, level LogLevel, msg string, args ...interface{}) {
	msgStr := fmt.Sprintf(msg, args...)
	l.log(name, level, msgStr)
}

// Log a message and a set of key/value pairs for a given level.
func (l *intLogger) log(name string, level LogLevel, msg string, args ...interface{}) {
	minLevel := LogLevel(atomic.LoadInt32(&l.level))
	if level < minLevel {
		return
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	t := l.timeFnc()

	// l.logJSON(t, name, level, msg, args...)
	switch l.logFormat {
	case "json":
		l.logJSON(t, name, level, msg, args...)
	case "text":
		l.logText(t, name, level, msg, args...)
	}

	l.writer.Flush()
}

func (l *intLogger) logText(t time.Time, name string, level LogLevel, msg string, args ...interface{}) {
	lvl, ok := levelToString[level]
	if !ok {
		lvl = "[?????]"
	}
	text := fmt.Sprintf("%s: %s", lvl, msg)
	l.writer.Write([]byte(text))
}

func (l *intLogger) logJSON(t time.Time, name string, level LogLevel, msg string, args ...interface{}) {
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

	err := json.NewEncoder(l.writer.Writer()).Encode(vals)
	if err != nil {
		if _, ok := err.(*json.UnsupportedTypeError); ok {
			vals := l.getVals(t, name, level, msg)
			vals["warn"] = errInvalidTypeMsg

			json.NewEncoder(l.writer.Writer()).Encode(vals)
		}
	}
}

func (l *intLogger) getVals(t time.Time, name string, level LogLevel, msg string) map[string]interface{} {
	vals := map[string]interface{}{
		"message":   msg,
		"timestamp": t.Format(l.timeFormat),
	}

	if name != "" {
		vals["module"] = name
	}

	var levelStr string
	switch level {
	case LogError:
		levelStr = "error"
	case LogWarn:
		levelStr = "warn"
	case LogInfo:
		levelStr = "info"
	case LogDebug:
		levelStr = "debug"
	default:
		levelStr = "all"
	}

	vals["level"] = levelStr

	if pc, file, line, ok := runtime.Caller(defaultCallerOffset + 1); ok {
		vals["caller"] = fmt.Sprintf("%s:%d", file, line)
		if ok {
			f := runtime.FuncForPC(pc)
			vals["component"] = f.Name()
		}
	}

	return vals
}

// Debugf prints the message and args at DEBUG level
func (l *intLogger) Debug(msg string, args ...interface{}) {
	l.log(l.Name(), LogDebug, msg, args...)
}

// Infof prints the message and args at INFO level
func (l *intLogger) Info(msg string, args ...interface{}) {
	l.log(l.Name(), LogInfo, msg, args...)
}

// Warningf prints the message and args at WARN level
func (l *intLogger) Warning(msg string, args ...interface{}) {
	l.log(l.Name(), LogWarn, msg, args...)
}

// Errorf prints the message and args at ERROR level
func (l *intLogger) Error(msg string, args ...interface{}) {
	l.log(l.Name(), LogError, msg, args...)
}

// Debugf prints the message and args at DEBUG level
func (l *intLogger) Debugf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogDebug, msg, args...)
}

// Infof prints the message and args at INFO level
func (l *intLogger) Infof(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogInfo, msg, args...)
}

// Warningf prints the message and args at WARN level
func (l *intLogger) Warningf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogWarn, msg, args...)
}

// Errorf prints the message and args at ERROR level
func (l *intLogger) Errorf(msg string, args ...interface{}) {
	l.logWithFmt(l.Name(), LogError, msg, args...)
}

// Update the logging level
func (l *intLogger) SetLogLevel(level LogLevel) {
	atomic.StoreInt32(&l.level, int32(level))
}

// Name returns the loggers name
func (i *intLogger) Name() string {
	return i.name
}

// Close the logger
func (i *intLogger) Close() error {
	return i.writer.Close()
}

type bufferedWriter struct {
	b bytes.Buffer
	w io.Writer
}

func newWriter(w io.Writer) writer {
	return &bufferedWriter{w: w}
}

func (w *bufferedWriter) Write(p []byte) (int, error) {
	return w.b.Write(p)
}

func (w *bufferedWriter) Flush() (err error) {
	var unwritten = w.b.Bytes()
	_, err = w.w.Write(unwritten)
	w.b.Reset()
	return err
}

func (w *bufferedWriter) Writer() io.Writer {
	return w.w
}

func (w *bufferedWriter) Close() error {
	switch t := w.w.(type) {
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

func newLogWriter(name string, out io.Writer) io.Writer {
	return &logWriter{
		Logger: log.New(out, name+" ", log.LstdFlags),
	}
}

type logWriter struct {
	*log.Logger
}

func (l *logWriter) Write(p []byte) (n int, err error) {
	l.Println(string(p))
	return 0, nil
}

func newMemoryWriter() io.Writer {
	lines := make([]string, 0, 1)
	return &memoryWriter{
		lines: &lines,
	}
}

type memoryWriter struct {
	lines *[]string
}

func (l *memoryWriter) Write(p []byte) (n int, err error) {
	sb := &strings.Builder{}

	sb.WriteRune('[')
	sb.WriteString(time.Now().Format(time.RFC3339Nano))
	sb.WriteString("] ")

	fmt.Fprintf(sb, string(p))
	*l.lines = append(*l.lines, sb.String())
	return 0, nil
}

func setup(file string) (out *os.File, err error) {
	if _, err = os.Stat(filepath.Dir(file)); os.IsNotExist(err) {
		if err = os.Mkdir(filepath.Dir(file), os.FileMode(0755)); err != nil {
			return nil, errors.New("Unable to create log folder")
		}
	}
	out, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return out, errors.New("Unable to create log file")
	}
	return out, err
}
