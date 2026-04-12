/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package audit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldLog_All(t *testing.T) {
	l := &Logger{eventFilter: "all"}
	assert.True(t, l.shouldLog(EventAuth))
	assert.True(t, l.shouldLog(EventAdmin))
	assert.True(t, l.shouldLog(EventWrite))
	assert.True(t, l.shouldLog(EventRead))
	assert.True(t, l.shouldLog(EventSystem))
}

func TestShouldLog_Write(t *testing.T) {
	l := &Logger{eventFilter: "write"}
	assert.True(t, l.shouldLog(EventAuth))
	assert.True(t, l.shouldLog(EventAdmin))
	assert.True(t, l.shouldLog(EventWrite))
	assert.False(t, l.shouldLog(EventRead))
	assert.True(t, l.shouldLog(EventSystem))
}

func TestShouldLog_Admin(t *testing.T) {
	l := &Logger{eventFilter: "admin"}
	assert.True(t, l.shouldLog(EventAuth))
	assert.True(t, l.shouldLog(EventAdmin))
	assert.False(t, l.shouldLog(EventWrite))
	assert.False(t, l.shouldLog(EventRead))
	assert.True(t, l.shouldLog(EventSystem))
}

func TestLog_NilEvent(t *testing.T) {
	l := &Logger{
		eventFilter: "all",
		ch:          make(chan *AuditEvent, 1),
	}
	l.Log(nil)
	assert.Equal(t, 0, len(l.ch))
}

func TestLog_FilteredEvent(t *testing.T) {
	l := &Logger{
		eventFilter: "admin",
		ch:          make(chan *AuditEvent, 1),
	}
	l.Log(&AuditEvent{EventType: EventRead})
	assert.Equal(t, 0, len(l.ch))
}

func TestLog_Enqueue(t *testing.T) {
	l := &Logger{
		eventFilter: "all",
		ch:          make(chan *AuditEvent, 1),
		stopCh:      make(chan struct{}),
	}
	l.Log(&AuditEvent{EventType: EventWrite, Method: "Set"})
	assert.Equal(t, 1, len(l.ch))
}

func TestLog_DropOnFull(t *testing.T) {
	l := &Logger{
		eventFilter: "all",
		ch:          make(chan *AuditEvent, 1),
		stopCh:      make(chan struct{}),
		log:         &noopLogger{},
	}
	// Fill the channel
	l.Log(&AuditEvent{EventType: EventWrite, Method: "Set"})

	// Second event should block then drop after timeout.
	// To test quickly, close stopCh so it takes the shutdown path.
	close(l.stopCh)
	l.Log(&AuditEvent{EventType: EventWrite, Method: "Set"})

	assert.Equal(t, int64(1), l.Dropped())
	assert.Equal(t, 1, len(l.ch))
}

type noopLogger struct{}

func (n *noopLogger) Errorf(string, ...interface{})   {}
func (n *noopLogger) Warningf(string, ...interface{}) {}
func (n *noopLogger) Infof(string, ...interface{})    {}
func (n *noopLogger) Debugf(string, ...interface{})   {}
func (n *noopLogger) Close() error                    { return nil }

func TestLog_DefaultFilter(t *testing.T) {
	l := &Logger{
		eventFilter: "",
		ch:          make(chan *AuditEvent, 10),
	}
	// Empty filter defaults to logging everything in shouldLog
	l.Log(&AuditEvent{EventType: EventRead})
	assert.Equal(t, 1, len(l.ch))
}
