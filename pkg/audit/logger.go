/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

const (
	logEnqueueTimeout = 5 * time.Second
	writeRetryCount   = 3
	writeRetryDelay   = 100 * time.Millisecond
)

const (
	defaultChannelSize = 4096
)

// Logger writes structured audit events to an immudb database as KV entries.
type Logger struct {
	db          database.DB
	eventFilter string
	ch          chan *AuditEvent
	log         logger.Logger
	wg          sync.WaitGroup
	stopCh      chan struct{}
	dropped     atomic.Int64
}

// NewLogger creates a new audit logger that writes events to the given database.
// eventFilter controls which events are logged: "all", "write", "admin".
func NewLogger(db database.DB, eventFilter string, log logger.Logger) *Logger {
	if eventFilter == "" {
		eventFilter = "all"
	}
	return &Logger{
		db:          db,
		eventFilter: eventFilter,
		ch:          make(chan *AuditEvent, defaultChannelSize),
		log:         log,
		stopCh:      make(chan struct{}),
	}
}

// Start begins the background goroutine that drains the event channel.
func (l *Logger) Start() {
	l.wg.Add(1)
	go l.drain()
}

// Stop signals the background goroutine to stop and waits for it to drain.
func (l *Logger) Stop() {
	close(l.stopCh)
	l.wg.Wait()

	if dropped := l.dropped.Load(); dropped > 0 {
		l.log.Warningf("audit logger dropped %d events due to full buffer", dropped)
	}
}

// Log enqueues an audit event for asynchronous writing.
// Blocks for up to logEnqueueTimeout if the channel is full to avoid
// silently dropping compliance-critical events. Only drops after timeout.
func (l *Logger) Log(event *AuditEvent) {
	if event == nil || !l.shouldLog(event.EventType) {
		return
	}

	select {
	case l.ch <- event:
		return
	default:
	}

	// Channel full — wait with timeout before dropping
	timer := time.NewTimer(logEnqueueTimeout)
	defer timer.Stop()

	select {
	case l.ch <- event:
	case <-timer.C:
		dropped := l.dropped.Add(1)
		l.log.Errorf("audit: DROPPED event (buffer full for %v, total dropped: %d) method=%s user=%s",
			logEnqueueTimeout, dropped, event.Method, event.Username)
	case <-l.stopCh:
		// Logger is shutting down — try one more time
		select {
		case l.ch <- event:
		default:
			l.dropped.Add(1)
		}
	}
}

// Dropped returns the number of events dropped due to a full buffer.
func (l *Logger) Dropped() int64 {
	return l.dropped.Load()
}

func (l *Logger) shouldLog(eventType EventType) bool {
	switch l.eventFilter {
	case "write":
		return eventType == EventWrite || eventType == EventAdmin || eventType == EventAuth || eventType == EventSystem
	case "admin":
		return eventType == EventAdmin || eventType == EventAuth || eventType == EventSystem
	default:
		return true
	}
}

func (l *Logger) drain() {
	defer l.wg.Done()

	for {
		select {
		case event := <-l.ch:
			l.writeEvent(event)
		case <-l.stopCh:
			// Drain remaining events
			for {
				select {
				case event := <-l.ch:
					l.writeEvent(event)
				default:
					return
				}
			}
		}
	}
}

func (l *Logger) writeEvent(event *AuditEvent) {
	value, err := json.Marshal(event)
	if err != nil {
		l.log.Errorf("audit: failed to marshal event: %v", err)
		return
	}

	req := &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   event.Key(),
				Value: value,
			},
		},
		NoWait: true,
	}

	// Retry on transient write failures (disk pressure, replication lag)
	for attempt := 0; attempt < writeRetryCount; attempt++ {
		_, err = l.db.Set(context.Background(), req)
		if err == nil {
			return
		}

		l.log.Warningf("audit: write attempt %d/%d failed: %v (method=%s user=%s)",
			attempt+1, writeRetryCount, err, event.Method, event.Username)

		if attempt < writeRetryCount-1 {
			time.Sleep(writeRetryDelay * time.Duration(attempt+1))
		}
	}

	l.log.Errorf("audit: PERMANENTLY FAILED to write event after %d attempts: %v (method=%s user=%s)",
		writeRetryCount, err, event.Method, event.Username)
}
