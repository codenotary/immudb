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
)

func DataRow() []byte {
	////##-> dataRow
	// Identifies the message as a data row.
	// Byte1('D')
	messageType := []byte(`D`)
	// Length of message contents in bytes, including self.
	// Int32
	selfMessageLength := make([]byte, 4)
	binary.BigEndian.PutUint32(selfMessageLength, uint32(14))
	// The number of column values that follow (possibly zero).
	// Int16
	columnNumb := make([]byte, 2)
	binary.BigEndian.PutUint16(columnNumb, uint16(1))

	// Next, the following pair of fields appear for each column:
	// The length of the column value, in bytes (this count does not include itself). Can be zero. As a special case, -1 indicates a NULL column value. No value bytes follow in the NULL case.
	// Int32
	valueLength := make([]byte, 4)
	binary.BigEndian.PutUint32(valueLength, uint32(4))
	// The value of the column, in the format indicated by the associated format code. n is the above length.
	// Byten
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, uint32(1))

	return bytes.Join([][]byte{messageType, selfMessageLength, columnNumb, valueLength, value}, nil)
}
