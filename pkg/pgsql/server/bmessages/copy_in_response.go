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

package bmessages

import (
	"bytes"
	"encoding/binary"
)

// CopyInResponse tells the client to start sending COPY data.
// Format: 'G' + int32(len) + int8(format) + int16(numCols) + int16[](col formats)
// format=0 means text format for the overall COPY operation.
func CopyInResponse(numCols int) []byte {
	msgType := []byte{'G'}

	// Overall format: 0 = text
	format := []byte{0}

	// Number of columns
	colCount := make([]byte, 2)
	binary.BigEndian.PutUint16(colCount, uint16(numCols))

	// Per-column format codes (all 0 = text)
	colFormats := make([]byte, 2*numCols)
	for i := 0; i < numCols; i++ {
		binary.BigEndian.PutUint16(colFormats[i*2:], 0)
	}

	body := bytes.Join([][]byte{format, colCount, colFormats}, nil)

	// Message length includes self (4 bytes) + body
	msgLen := make([]byte, 4)
	binary.BigEndian.PutUint32(msgLen, uint32(4+len(body)))

	return bytes.Join([][]byte{msgType, msgLen, body}, nil)
}
