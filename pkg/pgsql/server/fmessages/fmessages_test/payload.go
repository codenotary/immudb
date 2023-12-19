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

package fmessages_test

import (
	"bytes"
	"encoding/binary"
)

func Join(chunk [][]byte) []byte {
	return bytes.Join(chunk, nil)
}

func S(str string) []byte {
	return bytes.Join([][]byte{[]byte(str), {0}}, nil)
}
func B(b []byte) []byte {
	return b
}

func I16(i int) []byte {
	ib := make([]byte, 2)
	binary.BigEndian.PutUint16(ib, uint16(i))
	return ib
}
func I32(i int) []byte {
	ib := make([]byte, 4)
	binary.BigEndian.PutUint32(ib, uint32(i))
	return ib
}

func Msg(t byte, payload []byte) []byte {
	ml := make([]byte, 4)
	binary.BigEndian.PutUint32(ml, uint32(len(payload)+4))
	return bytes.Join([][]byte{{t}, ml, payload}, nil)
}
