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
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
)

// DataRow if ResultColumnFormatCodes is nil default text format is used
func DataRow(rows []*sql.Row, colNumb int, ResultColumnFormatCodes []int16) []byte {
	rowsB := make([]byte, 0)
	for _, row := range rows {
		rowB := make([]byte, 0)
		// Identifies the message as a data row.
		// Byte1('D')
		messageType := []byte(`D`)

		// The number of column values that follow (possibly zero).
		// Int16
		columnNumb := make([]byte, 2)
		binary.BigEndian.PutUint16(columnNumb, uint16(colNumb))

		for i, val := range row.ValuesByPosition {
			if val == nil {
				return nil
			}

			valueLength := make([]byte, 4)
			value := make([]byte, 0)

			BINformat := false
			if len(ResultColumnFormatCodes) == 1 {
				BINformat = ResultColumnFormatCodes[0] == 1
			}
			if ResultColumnFormatCodes != nil && len(ResultColumnFormatCodes) > i && ResultColumnFormatCodes[i] == 1 {
				BINformat = true
			}
			if BINformat {
				if val.IsNull() {
					n := -1
					binary.BigEndian.PutUint32(valueLength, uint32(n))
				} else {
					rv := val.RawValue()
					switch val.Type() {
					case sql.IntegerType:
						{
							binary.BigEndian.PutUint32(valueLength, uint32(8))
							value = make([]byte, 8)
							binary.BigEndian.PutUint64(value, uint64(rv.(int64)))
						}
					case sql.JSONType:
						{
							jsonStr := trimQuotes(val.String())
							binary.BigEndian.PutUint32(valueLength, uint32(len(jsonStr)))
							value = []byte(jsonStr)
						}
					case sql.VarcharType:
						{
							s := rv.(string)
							binary.BigEndian.PutUint32(valueLength, uint32(len(s)))
							value = []byte(s)
						}
					case sql.BooleanType:
						{
							binary.BigEndian.PutUint32(valueLength, uint32(1))
							value = []byte{0}
							if rv.(bool) {
								value = []byte{1}
							}
						}
					case sql.BLOBType:
						{
							blob := rv.([]byte)
							binary.BigEndian.PutUint32(valueLength, uint32(len(blob)))
							value = blob
						}
					}
				}
			} else {
				// only text format is allowed in simple query
				value = renderValueAsByte(val)
			}
			binary.BigEndian.PutUint32(valueLength, uint32(len(value)))
			//  As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
			if value == nil {
				tm := int32(-1)
				value = nil
				binary.BigEndian.PutUint32(valueLength, uint32(tm))
			}
			rowB = append(rowB, bytes.Join([][]byte{valueLength, value}, nil)...)
		}

		// Length of message contents in bytes, including self.
		// Int32
		selfMessageLength := make([]byte, 4)
		binary.BigEndian.PutUint32(selfMessageLength, uint32(4+2+len(rowB)))
		rowsB = append(rowsB, bytes.Join([][]byte{messageType, selfMessageLength, columnNumb, rowB}, nil)...)
	}
	return rowsB
}

func renderValueAsByte(v sql.TypedValue) []byte {
	if v.IsNull() {
		return nil
	}

	var s string
	switch v.Type() {
	case sql.VarcharType:
		s, _ = v.RawValue().(string)
	case sql.JSONType:
		s = trimQuotes(v.String())
	default:
		s = v.String()
	}
	return []byte(s)
}

func trimQuotes(s string) string {
	return strings.TrimSuffix(strings.TrimPrefix(s, "'"), "'")
}
