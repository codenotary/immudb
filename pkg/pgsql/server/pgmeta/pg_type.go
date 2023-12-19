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

package pgmeta

import (
	"fmt"
)

const PgTypeMapOid = 0
const PgTypeMapLength = 1

const PgsqlProtocolVersion = "9.6"

var PgsqlProtocolVersionMessage = fmt.Sprintf("pgsql wire protocol %s or greater version implemented by immudb", PgsqlProtocolVersion)

// PgTypeMap maps the immudb type descriptor with pgsql pgtype map.
// First int is the oid value (retrieved with select * from pg_type;)
// Second int is the length of the value. -1 for dynamic.
var PgTypeMap = map[string][]int{
	"BOOLEAN":   {16, 1},    //bool
	"BLOB":      {17, -1},   //bytea
	"TIMESTAMP": {20, 8},    //int8
	"INTEGER":   {20, 8},    //int8
	"VARCHAR":   {25, -1},   //text
	"UUID":      {2950, 16}, //uuid
	"FLOAT":     {701, 8},   //double-precision floating point number
}

const PgSeverityError = "ERROR"
const PgSeverityFaral = "FATAL"
const PgSeverityPanic = "PANIC"
const PgSeverityWarning = "WARNING"
const PgSeverityNotice = "NOTICE"
const PgSeverityDebug = "DEBUG"
const PgSeverityInfo = "INFO"
const PgSeverityLog = "LOG"

const PgServerErrRejectedEstablishmentOfSqlconnection = "08004"
const PgServerErrSyntaxError = "42601"
const PgServerErrProtocolViolation = "08P01"
const PgServerErrConnectionFailure = "08006"
const ProgramLimitExceeded = "54000"
const DataException = "22000"

var MTypes = map[byte]string{
	'Q': "query",
	'T': "rowDescription",
	'D': "dataRow",
	'C': "commandComplete",
	'Z': "readyForQuery",
	'R': "authentication",
	'p': "passwordMessage",
	'U': "unknown",
	'X': "terminate",
	'S': "parameterStatus",
	'E': "execute",
	'P': "parse",
	't': "parameterDesctiption",
	'B': "bind",
	'H': "flush",
}

var MaxMsgSize = 32 << 20 // 32MB
