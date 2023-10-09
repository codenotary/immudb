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

package logger

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileLogger(t *testing.T) {
	t.Setenv("LOG_LEVEL", "error")

	name := t.TempDir()

	outputFile := filepath.Join(name, "test-file-logger.log")
	fl, _, err := NewFileLogger(name, outputFile)

	require.NoError(t, err)
	fl.Debugf("some debug %d", 1)
	fl.Infof("some info %d", 1)
	fl.Warningf("some warning %d", 1)
	fl.Errorf("some error %d", 1)
	logBytes, err := ioutil.ReadFile(outputFile)
	logOutput := string(logBytes)
	require.NoError(t, err)
	require.Contains(t, logOutput, name)
	require.Contains(t, logOutput, " ERROR: some error 1")
	require.NotContains(t, logOutput, "some debug 1")
	require.NotContains(t, logOutput, "some info 1")
	require.NotContains(t, logOutput, "some warning 1")
	require.NoError(t, fl.Close())

	outputFile3 := filepath.Join(name, "test-file-logger-with-level.log")
	fl3, err := NewFileLoggerWithLevel(name, outputFile3, LogWarn)
	require.NoError(t, err)
	fl3.Debugf("some debug %d", 3)
	fl3.Infof("some info %d", 3)
	fl3.Warningf("some warning %d", 3)
	fl3.Errorf("some error %d", 3)
	logBytes, err = ioutil.ReadFile(outputFile3)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.NotContains(t, logOutput, "some debug 3")
	require.NotContains(t, logOutput, "some info 2")
	require.Contains(t, logOutput, " WARNING: some warning 3")
	require.Contains(t, logOutput, " ERROR: some error 3")
	require.NoError(t, fl3.Close())

	outputFile4 := filepath.Join(name, "test-file-logger-with-debug-level.log")
	fl4, err := NewFileLoggerWithLevel(name, outputFile4, LogDebug)
	require.NoError(t, err)
	fl4.Debugf("some debug %d", 4)
	fl4.Infof("some info %d", 4)
	fl4.Warningf("some warning %d", 4)
	fl4.Errorf("some error %d", 4)
	logBytes, err = ioutil.ReadFile(outputFile4)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.Contains(t, logOutput, "some debug 4")
	require.Contains(t, logOutput, "some info 4")
	require.Contains(t, logOutput, " WARNING: some warning 4")
	require.Contains(t, logOutput, " ERROR: some error 4")
	require.NoError(t, fl3.Close())
}
