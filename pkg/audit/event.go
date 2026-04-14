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
	"fmt"
	"strings"
)

// EventType classifies audit events by operation category.
type EventType string

const (
	EventAuth   EventType = "AUTH"
	EventAdmin  EventType = "ADMIN"
	EventWrite  EventType = "WRITE"
	EventRead   EventType = "READ"
	EventSystem EventType = "SYSTEM"
)

// AuditEvent represents a single structured audit log entry.
type AuditEvent struct {
	Timestamp  int64     `json:"ts"`
	Username   string    `json:"user"`
	ClientIP   string    `json:"ip"`
	Database   string    `json:"db,omitempty"`
	Method     string    `json:"method"`
	EventType  EventType `json:"type"`
	Success    bool      `json:"ok"`
	ErrorMsg   string    `json:"err,omitempty"`
	DurationMs int64     `json:"dur_ms"`
	SessionID  string    `json:"sid,omitempty"`
}

// KeyPrefix is the prefix for all audit event keys in the KV store.
const KeyPrefix = "audit:"

// Key returns the KV key for this event. Zero-padded nanosecond timestamp
// ensures lexicographic ordering matches time ordering for prefix scans.
func (e *AuditEvent) Key() []byte {
	return []byte(fmt.Sprintf("%s%020d", KeyPrefix, e.Timestamp))
}

var authMethods = map[string]struct{}{
	"Login":        {},
	"Logout":       {},
	"OpenSession":  {},
	"CloseSession": {},
}

var adminMethods = map[string]struct{}{
	"ListUsers":           {},
	"CreateUser":          {},
	"ChangePassword":      {},
	"SetPermission":       {},
	"ChangePermission":    {},
	"DeactivateUser":      {},
	"SetActiveUser":       {},
	"UpdateAuthConfig":    {},
	"UpdateMTLSConfig":    {},
	"CreateDatabase":      {},
	"CreateDatabaseV2":    {},
	"UpdateDatabase":      {},
	"UpdateDatabaseV2":    {},
	"DeleteDatabase":      {},
	"ChangeSQLPrivileges": {},
}

var writeMethods = map[string]struct{}{
	"Set":                    {},
	"VerifiableSet":          {},
	"StreamSet":              {},
	"StreamVerifiableSet":    {},
	"Delete":                 {},
	"ExecAll":                {},
	"StreamExecAll":          {},
	"SetReference":           {},
	"VerifiableSetReference": {},
	"ZAdd":                   {},
	"VerifiableZAdd":         {},
	"SQLExec":                {},
	"TxSQLExec":              {},
	"InsertDocuments":        {},
	"ReplaceDocuments":       {},
	"DeleteDocuments":        {},
	"CreateCollection":       {},
	"UpdateCollection":       {},
	"DeleteCollection":       {},
	"AddField":               {},
	"RemoveField":            {},
	"CreateIndex":            {},
	"DeleteIndex":            {},
}

var systemMethods = map[string]struct{}{
	"TruncateDatabase": {},
	"FlushIndex":       {},
	"CompactIndex":     {},
	"Dump":             {},
	"ExportTx":         {},
	"ReplicateTx":      {},
}

// ClassifyMethod maps a gRPC method name to an EventType.
// The method parameter can be a full gRPC path (e.g., "/immudb.schema.ImmuService/Set")
// or just the method name (e.g., "Set").
func ClassifyMethod(method string) EventType {
	// Extract short method name from full gRPC path
	short := method
	if idx := strings.LastIndex(method, "/"); idx >= 0 {
		short = method[idx+1:]
	}

	if _, ok := authMethods[short]; ok {
		return EventAuth
	}
	if _, ok := adminMethods[short]; ok {
		return EventAdmin
	}
	if _, ok := writeMethods[short]; ok {
		return EventWrite
	}
	if _, ok := systemMethods[short]; ok {
		return EventSystem
	}
	return EventRead
}
