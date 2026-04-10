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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyMethod_Auth(t *testing.T) {
	for _, method := range []string{"Login", "Logout", "OpenSession", "CloseSession"} {
		assert.Equal(t, EventAuth, ClassifyMethod(method), "method %s", method)
	}
}

func TestClassifyMethod_Admin(t *testing.T) {
	for _, method := range []string{"CreateUser", "ChangePassword", "SetPermission",
		"CreateDatabase", "CreateDatabaseV2", "UpdateDatabase", "DeleteDatabase",
		"ChangeSQLPrivileges", "DeactivateUser", "SetActiveUser"} {
		assert.Equal(t, EventAdmin, ClassifyMethod(method), "method %s", method)
	}
}

func TestClassifyMethod_Write(t *testing.T) {
	for _, method := range []string{"Set", "VerifiableSet", "Delete", "ExecAll",
		"SQLExec", "ZAdd", "SetReference", "InsertDocuments", "DeleteDocuments"} {
		assert.Equal(t, EventWrite, ClassifyMethod(method), "method %s", method)
	}
}

func TestClassifyMethod_System(t *testing.T) {
	for _, method := range []string{"TruncateDatabase", "FlushIndex", "CompactIndex",
		"Dump", "ExportTx", "ReplicateTx"} {
		assert.Equal(t, EventSystem, ClassifyMethod(method), "method %s", method)
	}
}

func TestClassifyMethod_Read(t *testing.T) {
	for _, method := range []string{"Get", "VerifiableGet", "Scan", "History",
		"SQLQuery", "TxByID", "Count", "ListTables"} {
		assert.Equal(t, EventRead, ClassifyMethod(method), "method %s", method)
	}
}

func TestClassifyMethod_FullGRPCPath(t *testing.T) {
	assert.Equal(t, EventWrite, ClassifyMethod("/immudb.schema.ImmuService/Set"))
	assert.Equal(t, EventAuth, ClassifyMethod("/immudb.schema.ImmuService/Login"))
	assert.Equal(t, EventRead, ClassifyMethod("/immudb.schema.ImmuService/Get"))
}

func TestClassifyMethod_Unknown(t *testing.T) {
	assert.Equal(t, EventRead, ClassifyMethod("UnknownMethod"))
}

func TestAuditEvent_Key(t *testing.T) {
	event := &AuditEvent{Timestamp: 1234567890123456789}
	key := event.Key()

	require.True(t, strings.HasPrefix(string(key), KeyPrefix))
	assert.Equal(t, "audit:01234567890123456789", string(key))
}

func TestAuditEvent_Key_Ordering(t *testing.T) {
	e1 := &AuditEvent{Timestamp: 100}
	e2 := &AuditEvent{Timestamp: 200}
	e3 := &AuditEvent{Timestamp: 1000}

	k1, k2, k3 := string(e1.Key()), string(e2.Key()), string(e3.Key())

	assert.True(t, k1 < k2, "key ordering: %s < %s", k1, k2)
	assert.True(t, k2 < k3, "key ordering: %s < %s", k2, k3)
}
