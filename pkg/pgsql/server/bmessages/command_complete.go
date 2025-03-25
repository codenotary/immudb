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

func CommandComplete(msg []byte) []byte {
	messageType := []byte(`C`)
	msg = bytes.Join([][]byte{msg, {0}}, nil)
	selfMessageLength := make([]byte, 4)
	binary.BigEndian.PutUint32(selfMessageLength, uint32(len(msg)+4))

	return bytes.Join([][]byte{messageType, selfMessageLength, msg}, nil)
}
