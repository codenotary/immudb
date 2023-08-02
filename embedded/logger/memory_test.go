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

package logger_test

import (
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/stretchr/testify/require"
)

func TestMemoryLogger(t *testing.T) {
	t.Setenv("LOG_LEVEL", "error")

	ml := logger.NewMemoryLogger()
	defer ml.Close()

	ml.Infof("hello %s!", "world")
	ml.Errorf("Hello %s!", "World")

	require.Len(t, ml.GetLogs(), 1)
	require.Regexp(t, `^\[.*\] ERR: Hello World!`, ml.GetLogs()[0])

	for _, d := range []struct {
		level           logger.LogLevel
		expectedNewLogs int
	}{
		{logger.LogDebug, 4},
		{logger.LogInfo, 3},
		{logger.LogWarn, 2},
		{logger.LogError, 1},
	} {
		t.Run(fmt.Sprintf("filtering test (%+v)", d), func(t *testing.T) {
			ml2 := logger.NewMemoryLoggerWithLevel(d.level)
			ml2.Debugf("DEBUG")
			ml2.Infof("INFO")
			ml2.Warningf("WARNING")
			ml2.Errorf("ERROR")

			require.Equal(t, d.expectedNewLogs, len(ml2.GetLogs()))
		})
	}
}
