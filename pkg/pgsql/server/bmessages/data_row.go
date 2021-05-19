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

package bmessages

import (
	"bytes"
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func DataRow(rows []*schema.Row, colNumb int, binaryFormat bool) []byte {
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

		for _, val := range row.Values {
			if val == nil {
				return nil
			}

			valueLength := make([]byte, 4)
			value := make([]byte, 0)

			// only text format is allowed in simple query
			value = schema.RenderValueAsByte(val.Value)

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
