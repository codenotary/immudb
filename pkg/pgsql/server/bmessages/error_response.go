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
	"fmt"
)

type errorResp struct {
	fields map[byte]string
}

type ErrorResp interface {
	Encode() []byte
	ToString() string
}

func ErrorResponse(setters ...Option) *errorResp {
	er := &errorResp{
		fields: make(map[byte]string),
	}
	for _, setter := range setters {
		setter(er)
	}

	return er
}

//Encode encode in binary
func (er *errorResp) Encode() []byte {
	messageType := []byte(`E`)
	messageLength := make([]byte, 4)
	body := make([]byte, 0)
	for code, value := range er.fields {
		body = append(body, bytes.Join([][]byte{{code}, []byte(value), {0}}, nil)...)
	}

	binary.BigEndian.PutUint32(messageLength, uint32(len(body)+4+1))

	return bytes.Join([][]byte{messageType, messageLength, body, {0}}, nil)
}

func (er *errorResp) ToString() string {
	return fmt.Sprintf("Map: %v", er.fields)
}
