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

// Byte1('t')
// Identifies the message as a parameter description.
//
// Int32
// Length of message contents in bytes, including self.
//
// Int16
// The number of parameters used by the statement (can be zero).
//
// Then, for each parameter, there is the following:
//
// Int32
// Specifies the object ID of the parameter data type.
// ParameterDescription send a parameter description message. Cols need to be lexicographically ordered by selector
func ParameterDescription(cols []sql.ColDescriptor) []byte {
	// Identifies the message as a run-time parameter status report.
	messageType := []byte(`t`)
	selfMessageLength := make([]byte, 4)

	paramsNumberB := make([]byte, 2)
	binary.BigEndian.PutUint16(paramsNumberB, uint16(len(cols)))

	params := make([][]byte, 0)
	for _, c := range cols {
		p := pgmeta.PgTypeMap[c.Type][pgmeta.PgTypeMapOid]
		paramB := make([]byte, 4)
		binary.BigEndian.PutUint32(paramB, uint32(p))
		params = append(params, paramB)
	}

	binary.BigEndian.PutUint32(selfMessageLength, uint32(len(paramsNumberB)+len(params)*4+4))

	return bytes.Join([][]byte{messageType, selfMessageLength, paramsNumberB, bytes.Join(params, nil)}, nil)
}
