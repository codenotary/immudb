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

package fmessages

import (
	"bufio"
	"encoding/binary"
)

func getNextString(r *bufio.Reader) (string, error) {
	s, err := r.ReadBytes(0)
	if err != nil {
		return "", err
	}
	return string(s[:len(s)-1]), nil
}

func getNextInt16(r *bufio.Reader) (int16, error) {
	pcb := make([]byte, 2)
	_, err := r.Read(pcb)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(pcb)), nil
}

func getNextInt32(r *bufio.Reader) (int32, error) {
	pcb := make([]byte, 4)
	_, err := r.Read(pcb)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(pcb)), nil
}
