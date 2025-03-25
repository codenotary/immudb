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
package document

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDocumentID_WithTimestamp(t *testing.T) {
	tests := []struct {
		time            string
		expectedTimeHex string
	}{
		{
			"1970-01-01T00:00:00.000Z",
			"00000000",
		},
		{
			"2038-01-19T03:14:07.000Z",
			"7fffffff",
		},
		{
			"2038-01-19T03:14:08.000Z",
			"80000000",
		},
		{
			"2106-02-07T06:28:15.000Z",
			"ffffffff",
		},
	}

	layout := "2006-01-02T15:04:05.000Z"
	for _, test := range tests {
		time, err := time.Parse(layout, test.time)
		require.NoError(t, err)

		id := NewDocumentIDFromTimestamp(time, 0)
		fmt.Println(test.time, id.EncodeToHexString())
		timeStr := hex.EncodeToString(id[0:4])

		require.Equal(t, test.expectedTimeHex, timeStr)
	}
}

func TestDocumentID_FromDocumentHex(t *testing.T) {
	tests := []struct {
		hex      string
		Expected string
	}{
		{
			"0000000075b4f29e0000000000000000",
			"1970-01-01 00:00:00 +0000 UTC",
		},
		{
			"7fffffffa7ec50600000000000000000",
			"2038-01-19 03:14:07 +0000 UTC",
		},
		{
			"80000000441e18f90000000000000000",
			"2038-01-19 03:14:08 +0000 UTC",
		},
		{
			"ffffffffb840d6030000000000000000",
			"2106-02-07 06:28:15 +0000 UTC",
		},
	}

	for _, test := range tests {
		id, err := NewDocumentIDFromHexEncodedString(test.hex)
		require.NoError(t, err)

		genTime := id.Timestamp()
		require.Equal(t, test.Expected, genTime.String())
	}
}

func TestDocumentID_IncrementalCounter(t *testing.T) {
	id := NewDocumentIDFromTx(0)
	counter := binary.BigEndian.Uint32(id[12:16])
	for i := 0; i < 10; i++ {
		id = NewDocumentIDFromTx(0)
		newCounter := binary.BigEndian.Uint32(id[12:16])
		require.Equal(t, atomic.AddUint32(&counter, 1), newCounter)
	}
}

func TestDocumentID_FromRawBytes(t *testing.T) {
	id := NewDocumentIDFromTx(1)
	b := []byte(id.EncodeToHexString())

	_, err := NewDocumentIDFromRawBytes([]byte{})
	require.ErrorIs(t, ErrIllegalArguments, err)

	_, err = NewDocumentIDFromRawBytes(b)
	require.NoError(t, err)

	b = append(b, byte(0))
	_, err = NewDocumentIDFromRawBytes(b)
	require.ErrorIs(t, ErrMaxLengthExceeded, err)
}

func BenchmarkHex(b *testing.B) {
	id := NewDocumentIDFromTx(0)
	for i := 0; i < b.N; i++ {
		id.EncodeToHexString()
	}
}
