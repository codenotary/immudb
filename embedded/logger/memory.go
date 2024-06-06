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
	"fmt"
	"strings"
	"sync"
	"time"
)

type MemoryLogger struct {
	m     sync.Mutex
	lines *[]string
	level LogLevel
}

func NewMemoryLogger() *MemoryLogger {
	return NewMemoryLoggerWithLevel(LogLevelFromEnvironment())
}

func NewMemoryLoggerWithLevel(level LogLevel) *MemoryLogger {
	return &MemoryLogger{
		lines: &[]string{},
		level: level,
	}
}

func (l *MemoryLogger) Errorf(fmt string, args ...interface{}) {
	l.addLog(LogError, "ERR", fmt, args)
}

func (l *MemoryLogger) Warningf(fmt string, args ...interface{}) {
	l.addLog(LogWarn, "WRN", fmt, args)
}

func (l *MemoryLogger) Infof(fmt string, args ...interface{}) {
	l.addLog(LogInfo, "INF", fmt, args)
}

func (l *MemoryLogger) Debugf(fmt string, args ...interface{}) {
	l.addLog(LogDebug, "DBG", fmt, args)
}

func (l *MemoryLogger) GetLogs() []string {
	l.m.Lock()
	defer l.m.Unlock()

	return *l.lines
}

func (l *MemoryLogger) addLog(level LogLevel, prefix string, f string, args []interface{}) {
	if level < l.level {
		return
	}

	sb := &strings.Builder{}

	sb.WriteRune('[')
	sb.WriteString(time.Now().Format(time.RFC3339Nano))
	sb.WriteString("] ")
	sb.WriteString(prefix)
	sb.WriteString(": ")

	fmt.Fprintf(sb, f, args...)

	l.m.Lock()
	defer l.m.Unlock()

	*l.lines = append(*l.lines, sb.String())
}

// Close the logger ...
func (l *MemoryLogger) Close() error {
	return nil
}
