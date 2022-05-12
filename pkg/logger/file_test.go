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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileLogger(t *testing.T) {
	os.Setenv("LOG_LEVEL", "error")
	defer os.Unsetenv("LOG_LEVEL")
	name := "test-file-logger"
	outputFile := filepath.Join(name, "test-file-logger.log")
	fl, _, err := NewFileLogger(fmt.Sprintf("%s ", name), outputFile)
	defer os.RemoveAll(name)
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

	fl2 := fl.CloneWithLevel(LogDebug)
	fl2.Debugf("some debug %d", 2)
	fl2.Infof("some info %d", 2)
	fl2.Warningf("some warning %d", 2)
	fl2.Errorf("some error %d", 2)
	logBytes, err = ioutil.ReadFile(outputFile)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.Contains(t, logOutput, " DEBUG: some debug 2")
	require.Contains(t, logOutput, " INFO: some info 2")
	require.Contains(t, logOutput, " WARNING: some warning 2")
	require.Contains(t, logOutput, " ERROR: some error 2")

	outputFile3 := filepath.Join(name, "test-file-logger-with-level.log")
	fl3, _, err := NewFileLoggerWithLevel(fmt.Sprintf("%s ", name), outputFile3, LogWarn)
	require.NoError(t, err)
	fl3.Debugf("some debug %d", 3)
	fl3.Infof("some info %d", 3)
	fl3.Warningf("some warning %d", 3)
	fl3.Errorf("some error %d", 3)
	logBytes, err = ioutil.ReadFile(outputFile3)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.NotContains(t, logOutput, "some debug 3")
	require.NotContains(t, logOutput, "ome info 2")
	require.Contains(t, logOutput, " WARNING: some warning 3")
	require.Contains(t, logOutput, " ERROR: some error 3")
}
