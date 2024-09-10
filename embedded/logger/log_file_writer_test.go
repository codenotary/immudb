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
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogFileIsRotatedOnInit(t *testing.T) {
	tempDir := t.TempDir()

	today := time.Now().Truncate(time.Hour * 24)

	opts := &Options{
		Name:       "immudb",
		LogFormat:  LogFormatJSON,
		Level:      LogDebug,
		LogDir:     tempDir,
		LogFile:    "immudb.log",
		TimeFormat: LogFileFormat,
		TimeFnc: func() time.Time {
			return today
		},
	}

	logger, err := NewLogger(opts)
	require.NoError(t, err)

	files, err := readFiles(tempDir, "immudb")
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "immudb.log", files[0])

	err = logger.Close()
	require.NoError(t, err)

	logger, err = NewLogger(opts)
	require.NoError(t, err)
	defer logger.Close()

	files, err = readFiles(tempDir, "immudb")
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Equal(t, []string{"immudb.log", "immudb.log.0001"}, files)
}

type mockWriter struct {
	nBytes int
	nCalls int
}

func (w *mockWriter) Write(buf []byte) (int, error) {
	w.nBytes += len(buf)
	w.nCalls++
	return len(buf), nil
}

func TestLogAreSentToOutput(t *testing.T) {
	mw := &mockWriter{}

	logger, err := NewLogger(&Options{
		Name:      "immudb",
		LogFormat: LogFormatJSON,
		Output:    mw,
		Level:     LogDebug,
	})
	require.NoError(t, err)
	defer logger.Close()

	nLogs := 100
	for i := 0; i < nLogs; i++ {
		logger.Errorf("test log %d", i)
	}
	require.Equal(t, mw.nCalls, nLogs)
}

func TestLoggerFileWithRotationDisabled(t *testing.T) {
	tempDir := t.TempDir()

	logger, err := NewLogger(&Options{
		Name:      "immudb",
		LogFormat: LogFormatJSON,
		Level:     LogDebug,
		LogDir:    tempDir,
		LogFile:   "immudb.log",
		TimeFnc: func() time.Time {
			return time.Now()
		},
	})
	require.NoError(t, err)
	defer logger.Close()

	nLogs := 100
	for i := 0; i < nLogs; i++ {
		logger.Errorf("test log %d", i)
	}

	err = logger.Close()
	require.NoError(t, err)

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "immudb.log", entries[0].Name())

	f, err := os.Open(filepath.Join(tempDir, "immudb.log"))
	require.NoError(t, err)

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		require.Contains(t, sc.Text(), "test log")
	}
	require.NoError(t, sc.Err())
}

func TestLoggerFileAgeRotation(t *testing.T) {
	tempDir := t.TempDir()

	age := time.Hour * 24
	i := 0
	now := time.Now().Truncate(age)

	logger, err := NewLogger(&Options{
		Name:            "immudb",
		LogFormat:       LogFormatText,
		Level:           LogDebug,
		LogDir:          tempDir,
		LogFile:         "immudb.log",
		LogRotationSize: 1024 * 1024,
		LogRotationAge:  time.Hour * 24,
		TimeFnc: func() time.Time {
			t := now.Add(age * time.Duration(i))
			i++
			return t
		},
	})
	require.NoError(t, err)
	defer logger.Close()

	nLogs := 100
	for i := 0; i < nLogs; i++ {
		logger.Errorf("this is a test")
	}
	require.Equal(t, nLogs+1, i)

	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	require.Len(t, entries, nLogs+1)

	files, err := readFiles(tempDir, "immudb.log")
	require.NoError(t, err)
	require.Len(t, files, nLogs+1)

	require.Equal(t, files[0], "immudb.log")
	for i := 1; i < len(files); i++ {
		require.Equal(t, fmt.Sprintf("immudb.log.%04d", i), files[i])
	}
}

func TestLoggerFileAgeRotationWithTimeFormat(t *testing.T) {
	tempDir := t.TempDir()

	age := time.Hour * 24
	i := 0
	now := time.Now().Truncate(age)

	logger, err := NewLogger(&Options{
		Name:              "immudb",
		LogFormat:         LogFormatText,
		LogFileTimeFormat: LogFileFormat,
		Level:             LogDebug,
		LogDir:            tempDir,
		LogFile:           "immudb.log",
		LogRotationSize:   1024 * 1024,
		LogRotationAge:    time.Hour * 24,
		TimeFnc: func() time.Time {
			t := now.Add(age * time.Duration(i))
			i++
			return t
		},
	})
	require.NoError(t, err)
	defer logger.Close()

	nLogs := 100
	for i := 0; i < nLogs; i++ {
		logger.Errorf("this is a test")
	}
	require.Equal(t, nLogs+1, i)

	files, err := readFiles(tempDir, "immudb")
	require.NoError(t, err)
	require.Len(t, files, nLogs+1)

	for n, f := range files {
		name := strings.TrimSuffix(f, path.Ext(f))
		idx := strings.LastIndex(name, "_")
		require.GreaterOrEqual(t, idx, 0)

		segmentAge := name[idx+1:]
		ageTime, err := time.Parse(LogFileFormat, segmentAge)
		require.NoError(t, err)
		require.Equal(t, ageTime.UTC(), now.Add(age*time.Duration(n)).UTC())
	}
}

func TestLoggerFileSizeRotation(t *testing.T) {
	tempDir := t.TempDir()

	logger, err := NewLogger(&Options{
		Name:            "immudb",
		LogFormat:       LogFormatText,
		Level:           LogDebug,
		LogDir:          tempDir,
		LogFile:         "immudb.log",
		LogRotationSize: 100,
		LogRotationAge:  time.Hour * 24,
	})
	require.NoError(t, err)

	nLogs := 100
	for i := 0; i < nLogs; i++ {
		logger.Errorf("this is a test")
	}

	err = logger.Close()
	require.NoError(t, err)

	files, err := readFiles(tempDir, "immudb.log")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for _, fname := range files {
		finfo, err := os.Stat(filepath.Join(tempDir, fname))
		require.NoError(t, err)
		require.LessOrEqual(t, finfo.Size(), int64(100))
	}
}

func readFiles(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		if strings.HasPrefix(e.Name(), prefix) {
			files = append(files, e.Name())
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i] < files[j]
	})
	return files, nil
}
