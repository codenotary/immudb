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

package stream

import (
	"bytes"
	"io"
)

// ReadValue returns the complete value from a message
// If no more data is present on the reader nil and io.EOF are returned
func ReadValue(vr io.Reader, bufferSize int) (value []byte, err error) {
	b := bytes.NewBuffer([]byte{})
	vl := 0
	eof := false
	chunk := make([]byte, bufferSize)
	for {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		vl += l
		b.Write(chunk)
		// we return an EOF also if there is another message present on stream (l == 0)
		if err == io.EOF || l == 0 {
			eof = true
			break
		}
	}
	if eof && vl == 0 {
		return nil, io.EOF
	}
	value = make([]byte, vl)
	_, err = b.Read(value)
	if err != nil {
		return nil, err
	}
	return value, err
}
