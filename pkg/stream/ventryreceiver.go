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
	"io"
)

type vEntryStreamReceiver struct {
	s          io.Reader
	BufferSize int
}

// NewVEntryStreamReceiver ...
func NewVEntryStreamReceiver(s io.Reader, bs int) VEntryStreamReceiver {
	return &vEntryStreamReceiver{
		s:          s,
		BufferSize: bs,
	}
}

func (vesr *vEntryStreamReceiver) Next() ([]byte, []byte, []byte, io.Reader, error) {
	ris := make([][]byte, 3)
	for i := range ris {
		r, err := ReadValue(vesr.s, vesr.BufferSize)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		ris[i] = r
	}
	// for the value, (which can be large), return a Reader and let the caller read it
	return ris[0], ris[1], ris[2], vesr.s, nil
}
