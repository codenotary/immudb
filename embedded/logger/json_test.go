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
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJSONLogger(t *testing.T) {
	t.Run("log", func(t *testing.T) {
		logger, err := NewJSONLogger(nil)
		require.NoError(t, err)

		logger.SetLogLevel(LogDebug)
		require.EqualValues(t, LogDebug, logger.level)

		var buf bytes.Buffer
		logger, err = NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Info("test call", "user", "foo")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "test call", raw["message"])
		require.Equal(t, "foo", raw["user"])

		require.NoError(t, logger.Close())
	})

	t.Run("use UTC time zone", func(t *testing.T) {
		var buf bytes.Buffer

		logger, err := NewJSONLogger(&Options{
			Name:       "test",
			Output:     &buf,
			TimeFormat: time.Kitchen,
			TimeFnc:    func() time.Time { return time.Now().UTC() },
		})
		require.NoError(t, err)

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

		require.Equal(t, val, time.Now().UTC().Format(time.Kitchen))
	})

	t.Run("log error type", func(t *testing.T) {
		var buf bytes.Buffer

		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		errMsg := errors.New("this is an error")
		logger.Info("test call", "user", "foo", "err", errMsg)

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "test call", raw["message"])
		require.Equal(t, "foo", raw["user"])
		require.Equal(t, errMsg.Error(), raw["err"])
	})

	t.Run("handles non-serializable args", func(t *testing.T) {
		var buf bytes.Buffer

		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		myfunc := func() int { return 42 }
		logger.Info("test call", "production", myfunc)

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "test call", raw["message"])
		require.Equal(t, errInvalidTypeMsg, raw["warn"])
	})

	t.Run("use file output for logging", func(t *testing.T) {
		file, err := ioutil.TempFile("", "logger")
		require.NoError(t, err)
		defer os.Remove(file.Name())

		logger, err := NewJSONLogger(&Options{
			Name:       "test",
			Output:     file,
			TimeFormat: time.Kitchen,
			TimeFnc:    func() time.Time { return time.Now().UTC() },
		})
		require.NoError(t, err)

		logger.Info("some info", "foo", "bar")

		logBytes, err := ioutil.ReadFile(file.Name())
		require.NoError(t, err)

		var raw map[string]interface{}
		if err := json.Unmarshal(logBytes, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info", raw["message"])
		require.Equal(t, "bar", raw["foo"])

		require.NoError(t, logger.Close())
	})

	t.Run("log with debug", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Debug("some info", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info", raw["message"])
		require.Equal(t, "bar", raw["foo"])
		require.Equal(t, "debug", raw["level"])
	})

	t.Run("log with warning", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Warning("some info", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info", raw["message"])
		require.Equal(t, "bar", raw["foo"])
		require.Equal(t, "warn", raw["level"])
	})

	t.Run("log with error", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Error("some info", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info", raw["message"])
		require.Equal(t, "bar", raw["foo"])
		require.Equal(t, "error", raw["level"])
	})

	t.Run("log with infof", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Infof("some info %s %s", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info foo bar", raw["message"])
		require.Equal(t, "info", raw["level"])
	})

	t.Run("log with debugf", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Debugf("some info %s %s", "foo", "bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info foo bar", raw["message"])
		require.Equal(t, "debug", raw["level"])
	})

	t.Run("log with warningf", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Warningf("some info %s %s", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info foo bar", raw["message"])
		require.Equal(t, "warn", raw["level"])
	})

	t.Run("log with errorf", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logger.Errorf("some info %s %s", "foo", "bar")
		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "some info foo bar", raw["message"])
		require.Equal(t, "error", raw["level"])
	})

	t.Run("log with component", func(t *testing.T) {
		var buf bytes.Buffer
		logger, err := NewJSONLogger(&Options{
			Name:   "test",
			Output: &buf,
		})
		require.NoError(t, err)

		logWithFunc(logger, "some info foo bar")

		b := buf.Bytes()

		var raw map[string]interface{}
		if err := json.Unmarshal(b, &raw); err != nil {
			t.Fatal(err)
		}

		require.Equal(t, "github.com/codenotary/immudb/embedded/logger.logWithFunc", raw["component"])
	})

}

func logWithFunc(l *JsonLogger, msg string) {
	l.Infof(msg)
}
