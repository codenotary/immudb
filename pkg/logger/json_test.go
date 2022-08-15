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
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJSONLogger(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Info("test call", "user", "foo")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "test call", raw["message"])
		assert.Equal(t, "foo", raw["user"])
	})

	t.Run("use UTC time zone", func(t *testing.T) {
		var buf bytes.Buffer

		logger := NewJSONLogger(&Options{
			Name:       "test",
			Output:     &buf,
			TimeFormat: time.Kitchen,
			TimeFnc:    func() time.Time { return time.Now().UTC() },
		})

		logger.Info("foobar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		val, ok := raw["timestamp"]
		if !ok {
			t.Fatal("missing 'timestamp' key")
		}

		assert.Equal(t, val, time.Now().UTC().Format(time.Kitchen))
	})

	t.Run("log error type", func(t *testing.T) {
		var buf bytes.Buffer

		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		errMsg := errors.New("this is an error")
		logger.Info("test call", "user", "foo", "err", errMsg)

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "test call", raw["message"])
		assert.Equal(t, "foo", raw["user"])
		assert.Equal(t, errMsg.Error(), raw["err"])
	})

	t.Run("handles non-serializable args", func(t *testing.T) {
		var buf bytes.Buffer

		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		myfunc := func() int { return 42 }
		logger.Info("test call", "production", myfunc)

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "test call", raw["message"])
		assert.Equal(t, errInvalidTypeMsg, raw["warn"])
	})

	t.Run("use file output for logging", func(t *testing.T) {
		file, err := ioutil.TempFile("", "logger")
		assert.NoError(t, err)
		defer os.Remove(file.Name())

		logger := NewJSONLogger(&Options{
			Name:       "test",
			Output:     file,
			TimeFormat: time.Kitchen,
			TimeFnc:    func() time.Time { return time.Now().UTC() },
		})

		logger.Info("some info", "foo", "bar")

		logBytes, err := ioutil.ReadFile(file.Name())
		assert.NoError(t, err)

		var raw map[string]interface{}
		if err := json.Unmarshal(logBytes, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info", raw["message"])
		assert.Equal(t, "bar", raw["foo"])
	})

	t.Run("log with debug", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Debug("some info", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info", raw["message"])
		assert.Equal(t, "bar", raw["foo"])
		assert.Equal(t, "debug", raw["level"])
	})

	t.Run("log with warning", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Warning("some info", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info", raw["message"])
		assert.Equal(t, "bar", raw["foo"])
		assert.Equal(t, "warn", raw["level"])
	})

	t.Run("log with error", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Error("some info", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info", raw["message"])
		assert.Equal(t, "bar", raw["foo"])
		assert.Equal(t, "error", raw["level"])
	})

	t.Run("log with infof", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Infof("some info %s %s", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info foo bar", raw["message"])
		assert.Equal(t, "info", raw["level"])
	})

	t.Run("log with debugf", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Debugf("some info %s %s", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info foo bar", raw["message"])
		assert.Equal(t, "debug", raw["level"])
	})

	t.Run("log with warningf", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Warningf("some info %s %s", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info foo bar", raw["message"])
		assert.Equal(t, "warn", raw["level"])
	})

	t.Run("log with errorf", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logger.Errorf("some info %s %s", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "some info foo bar", raw["message"])
		assert.Equal(t, "error", raw["level"])
	})

	t.Run("log with component", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})

		logWithFunc(logger, "some info foo bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, "github.com/codenotary/immudb/pkg/logger.logWithFunc", raw["component"])
	})

}

func logWithFunc(l *JsonLogger, msg string) {
	l.Infof(msg)
}
