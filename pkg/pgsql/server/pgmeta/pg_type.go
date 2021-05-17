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

package pgmeta

const PgTypeMapOid = 0
const PgTypeMapLength = 1

// PgTypeMap maps the immudb type descriptor with pgsql pgtype map.
// First int is the oid value (retrieved with select * from pg_type;)
// Second int is the length of the value. -1 for dynamic.
var PgTypeMap = map[string][]int{
	"BOOLEAN":   {16, 1},  //bool
	"BLOB":      {17, -1}, //bytea
	"TIMESTAMP": {20, 8},  //int8
	"INTEGER":   {20, 8},  //int8
	"VARCHAR":   {25, -1}, //text
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
