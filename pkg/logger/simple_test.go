/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleLogger(t *testing.T) {
	os.Setenv("LOG_LEVEL", "error")
	defer os.Unsetenv("LOG_LEVEL")
	name := "test-simple-logger"
	outputWriter := bytes.NewBufferString("")
	sl := NewSimpleLogger(name, outputWriter)
	sl.Debugf("some debug %d", 1)
	sl.Infof("some info %d", 1)
	sl.Warningf("some warning %d", 1)
	sl.Errorf("some error %d", 1)
	logBytes, err := ioutil.ReadAll(outputWriter)
	logOutput := string(logBytes)
	require.NoError(t, err)
	require.Contains(t, logOutput, name)
	require.Contains(t, logOutput, " ERROR: some error 1")
	require.NotContains(t, logOutput, "some debug 1")
	require.NotContains(t, logOutput, "some info 1")
	require.NotContains(t, logOutput, "some warning 1")

	sl2 := sl.CloneWithLevel(LogDebug)
	sl2.Debugf("some debug %d", 2)
	sl2.Infof("some info %d", 2)
	sl2.Warningf("some warning %d", 2)
	sl2.Errorf("some error %d", 2)
	logBytes, err = ioutil.ReadAll(outputWriter)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.Contains(t, logOutput, " DEBUG: some debug 2")
	require.Contains(t, logOutput, " INFO: some info 2")
	require.Contains(t, logOutput, " WARNING: some warning 2")
	require.Contains(t, logOutput, " ERROR: some error 2")

	outputWriter.Reset()
	sl3 := NewSimpleLoggerWithLevel(fmt.Sprintf("%s ", name), outputWriter, LogWarn)
	sl3.Debugf("some debug %d", 3)
	sl3.Infof("some info %d", 3)
	sl3.Warningf("some warning %d", 3)
	sl3.Errorf("some error %d", 3)
	logBytes, err = ioutil.ReadAll(outputWriter)
	require.NoError(t, err)
	logOutput = string(logBytes)
	require.NotContains(t, logOutput, "some debug 3")
	require.NotContains(t, logOutput, "ome info 2")
	require.Contains(t, logOutput, " WARNING: some warning 3")
	require.Contains(t, logOutput, " ERROR: some error 3")
}

func TestLogLevelFromEnvironment(t *testing.T) {
	defaultLevel := logLevelFromEnvironment()
	require.Equal(t, LogInfo, defaultLevel)
	defer os.Unsetenv("LOG_LEVEL")
	os.Setenv("LOG_LEVEL", "error")
	require.Equal(t, LogError, logLevelFromEnvironment())
	os.Setenv("LOG_LEVEL", "warn")
	require.Equal(t, LogWarn, logLevelFromEnvironment())
	os.Setenv("LOG_LEVEL", "info")
	require.Equal(t, LogInfo, logLevelFromEnvironment())
	os.Setenv("LOG_LEVEL", "debug")
	require.Equal(t, LogDebug, logLevelFromEnvironment())
}
