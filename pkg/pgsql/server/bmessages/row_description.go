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

package bmessages

import (
	"bytes"
	"encoding/binary"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

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
		fieldName := []byte(col.Selector())
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
