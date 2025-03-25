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

type zStreamReceiver struct {
	s          io.Reader
	BufferSize int
}

// NewZStreamReceiver ...
func NewZStreamReceiver(s io.Reader, bs int) *zStreamReceiver {
	return &zStreamReceiver{
		s:          s,
		BufferSize: bs,
	}
}

func (zr *zStreamReceiver) Next() ([]byte, []byte, float64, uint64, io.Reader, error) {
	ris := make([][]byte, 4)
	for i, _ := range ris {
		r, err := ReadValue(zr.s, zr.BufferSize)
		if err != nil {
			return nil, nil, 0, 0, nil, err
		}
		ris[i] = r
	}

	var score float64
	if err := NumberFromBytes(ris[2], &score); err != nil {
		return nil, nil, 0, 0, nil, err
	}
	var atTx uint64
	if err := NumberFromBytes(ris[3], &atTx); err != nil {
		return nil, nil, 0, 0, nil, err
	}

	// for the value, (which can be large), return a Reader and let the caller read it
	return ris[0], ris[1], score, atTx, zr.s, nil
}
