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
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
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

// Log enqueues an audit event for asynchronous writing. Non-blocking.
func (l *Logger) Log(event *AuditEvent) {
	if event == nil || !l.shouldLog(event.EventType) {
		return
	}

	select {
	case l.ch <- event:
	default:
		l.dropped.Add(1)
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
		l.log.Warningf("audit: failed to marshal event: %v", err)
		return
	}

	_, err = l.db.Set(context.Background(), &schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   event.Key(),
				Value: value,
			},
		},
		NoWait: true,
	})
	if err != nil {
		l.log.Warningf("audit: failed to write event: %v", err)
	}
}
