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

package bmessages

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// reservedRenameColumns lists the lower-case keywords that the pgsql
// translator (query_machine.go) prefixes with `_` when they appear as
// quoted identifiers in incoming DDL/DML. The list MUST stay in sync
// with the regex in query_machine.go pgTypeReplacements at the
// `"reserved-keyword" -> _word` rule. The reverse mapping is needed
// here so result columns presented to ORMs (XORM, GORM, …) carry
// the original Postgres-side name. Without it, Gitea's
// `Issue.Index int64 \`xorm:"INDEX"\`` field never gets populated
// (column comes back as `_index`, struct mapper looks for `index`),
// and the issue-list template renders every issue as "#0".
var reservedRenameColumns = map[string]bool{
	"add": true, "admin": true, "after": true, "all": true, "alter": true, "and": true,
	"as": true, "asc": true, "auto_increment": true, "avg": true, "before": true,
	"begin": true, "between": true, "bigint": true, "bigserial": true, "blob": true,
	"boolean": true, "by": true, "bytea": true, "cascade": true, "case": true,
	"cast": true, "check": true, "column": true, "commit": true, "conflict": true,
	"constraint": true, "count": true, "create": true, "cross": true, "database": true,
	"databases": true, "date": true, "day": true, "decimal": true, "default": true,
	"delete": true, "desc": true, "diff": true, "distinct": true, "do": true,
	"double": true, "drop": true, "else": true, "end": true, "except": true,
	"exists": true, "explain": true, "extract": true, "false": true, "fetch": true,
	"first": true, "float": true, "for": true, "foreign": true, "from": true,
	"full": true, "grant": true, "grants": true, "group": true, "having": true,
	"history": true, "hour": true, "if": true, "ilike": true, "in": true,
	"index": true, "inner": true, "insert": true, "int": true, "integer": true,
	"intersect": true, "into": true, "is": true, "join": true, "json": true,
	"jsonb": true, "key": true, "last": true, "lateral": true, "left": true,
	"like": true, "limit": true, "max": true, "min": true, "minute": true,
	"month": true, "natural": true, "not": true, "nothing": true, "null": true,
	"nulls": true, "numeric": true, "of": true, "offset": true, "on": true,
	"only": true, "or": true, "order": true, "over": true, "partition": true,
	"password": true, "primary": true, "privileges": true, "read": true,
	"readwrite": true, "real": true, "recursive": true, "references": true,
	"release": true, "rename": true, "returning": true, "revoke": true,
	"right": true, "rollback": true, "rows": true, "savepoint": true, "second": true,
	"select": true, "sequence": true, "serial": true, "set": true, "show": true,
	"since": true, "smallint": true, "snapshot": true, "sum": true, "table": true,
	"tables": true, "then": true, "timestamp": true, "timestamptz": true, "to": true,
	"transaction": true, "true": true, "truncate": true, "tx": true, "union": true,
	"unique": true, "until": true, "update": true, "upsert": true, "use": true,
	"user": true, "users": true, "using": true, "uuid": true, "values": true,
	"varchar": true, "view": true, "when": true, "where": true, "with": true,
	"year": true,
}

// stripReservedColumnPrefix reverses the `_<keyword>` rename applied
// by the pgsql translator on the way IN. If the column name is
// `_<word>` AND `<word>` is one of the immudb-reserved identifiers
// the translator handles, return the bare `<word>` so client-side
// ORMs see the same name they wrote. Columns that legitimately start
// with `_` and whose suffix is NOT a reserved keyword pass through
// untouched.
func stripReservedColumnPrefix(name string) string {
	if len(name) < 2 || name[0] != '_' {
		return name
	}
	if reservedRenameColumns[strings.ToLower(name[1:])] {
		return name[1:]
	}
	return name
}

func RowDescription(cols []sql.ColDescriptor, formatCodes []int16) []byte {
	////##-> dataRowDescription
	//Byte1('T')
	messageType := []byte(`T`)

	// Specifies the number of fields in a row (can be zero).
	// Int16
	fieldNumb := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldNumb, uint16(len(cols)))

	rowDescMessageB := make([]byte, 0)
	for n, col := range cols {
		// The field name.
		// String
		// Postgres returns plain column names (e.g. "id"), not the
		// "(table.col)" wrapper that immudb's Selector() produces.
		// ORMs (Rails ActiveRecord, SQLAlchemy, …) match result rows
		// to model attributes by this name, so the wrapper makes
		// every column look unrecognised. Fall back to Selector
		// only when Column itself is empty.
		name := col.Column
		if name == "" {
			name = col.Selector()
		}
		name = stripReservedColumnPrefix(name)
		fieldName := []byte(name)
		fieldName = bytes.Join([][]byte{fieldName, {0}}, nil)
		// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
		// Int32
		id := make([]byte, 4)
		binary.BigEndian.PutUint32(id, uint32(0))
		// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
		// Int16
		attributeNumber := make([]byte, 2)
		binary.BigEndian.PutUint16(attributeNumber, uint16(n+1))
		// The object ID of the field's data type.
		// Int32
		objectId := make([]byte, 4)

		oid := pgmeta.PgTypeMap[col.Type][pgmeta.PgTypeMapOid]
		binary.BigEndian.PutUint32(objectId, uint32(oid))
		// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
		// For a fixed-size type, typlen is the number of bytes in the internal representation of the type. But for a variable-length type, typlen is negative. -1 indicates a “varlena” type (one that has a length word), -2 indicates a null-terminated C string.
		// Int16
		dataTypeSize := make([]byte, 2)
		l := pgmeta.PgTypeMap[col.Type][pgmeta.PgTypeMapLength]
		binary.BigEndian.PutUint16(dataTypeSize, uint16(l))
		// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
		// atttypmod records type-specific data supplied at table creation time (for example, the maximum length of a varchar column). It is passed to type-specific input functions and length coercion functions. The value will generally be -1 for types that do not need atttypmod.
		// Int32
		typeModifier := make([]byte, 4)
		tm := int32(-1)
		binary.BigEndian.PutUint32(typeModifier, uint32(tm))
		// The format code being used for the field. Currently will be zero (text) or one (binary). In a RowDescription returned from the statement variant of Describe, the format code is not yet known and will always be zero.
		// Int16
		// In simple Query mode, the format of retrieved values is always text, except when the given command is a FETCH from a cursor declared with the BINARY option. In that case, the retrieved values are in binary format. The format codes given in the RowDescription message tell which format is being used.
		fc := int16(0)
		if len(formatCodes) >= n+1 {
			fc = formatCodes[n]
		}
		formatCode := make([]byte, 2)
		binary.BigEndian.PutUint16(formatCode, uint16(fc))

		rowDescMessageB = append(rowDescMessageB, bytes.Join([][]byte{fieldName, id, attributeNumber, objectId, dataTypeSize, typeModifier, formatCode}, nil)...)
	}

	// Length of message contents in bytes, including self.
	// Int32
	rowDescMessageLengthB := make([]byte, 4)
	rowDescMessageLength := 4 + 2 + len(rowDescMessageB)
	binary.BigEndian.PutUint32(rowDescMessageLengthB, uint32(rowDescMessageLength))

	return bytes.Join([][]byte{messageType, rowDescMessageLengthB, fieldNumb, rowDescMessageB}, nil)
}
