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
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

// Byte1('t')
//Identifies the message as a parameter description.
//
//Int32
//Length of message contents in bytes, including self.
//
//Int16
//The number of parameters used by the statement (can be zero).
//
//Then, for each parameter, there is the following:
//
//Int32
//Specifies the object ID of the parameter data type.
func ParameterDescriptiom(paramsNumber int) []byte {
	// Identifies the message as a run-time parameter status report.
	messageType := []byte(`t`)
	selfMessageLength := make([]byte, 4)

	paramsNumberB := make([]byte, 2)
	binary.BigEndian.PutUint16(paramsNumberB, uint16(paramsNumber))

	p1 := pgmeta.PgTypeMap["INTEGER"][pgmeta.PgTypeMapOid]
	p2 := pgmeta.PgTypeMap["INTEGER"][pgmeta.PgTypeMapOid]
	p3 := pgmeta.PgTypeMap["VARCHAR"][pgmeta.PgTypeMapOid]

	param1B := make([]byte, 4)
	binary.BigEndian.PutUint32(param1B, uint32(p1))

	param2B := make([]byte, 4)
	binary.BigEndian.PutUint32(param2B, uint32(p2))

	param3B := make([]byte, 4)
	binary.BigEndian.PutUint32(param3B, uint32(p3))

	binary.BigEndian.PutUint32(selfMessageLength, uint32(len(paramsNumberB)+len(param1B)+len(param2B)+len(param3B)+4))

	return bytes.Join([][]byte{messageType, selfMessageLength, paramsNumberB, param1B, param2B, param3B}, nil)
}
