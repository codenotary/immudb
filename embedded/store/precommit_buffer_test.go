/*
Copyright 2026 Codenotary Inc. All rights reserved.

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
package store

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrecommitBuffer(t *testing.T) {
	size := 256
	b := newPrecommitBuffer(size)

	_, _, _, _, err := b.readAhead(0)
	require.ErrorIs(t, err, ErrNotEnoughData)

	for i := 0; i < size; i++ {
		err := b.put(uint64(i), sha256.Sum256([]byte{byte(i)}), int64(i*100), i*10)
		require.NoError(t, err)
	}

	_, _, _, _, err = b.readAhead(-1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = b.put(0, sha256.Sum256(nil), 0, 0)
	require.ErrorIs(t, err, ErrBufferIsFull)

	_, _, _, _, err = b.readAhead(size + 1)
	require.ErrorIs(t, err, ErrNotEnoughData)

	// reading ahead should not consume entries
	for it := 0; it < 2; it++ {
		for i := 0; i < size; i++ {
			txID, alh, txOff, txSize, err := b.readAhead(i)
			require.NoError(t, err)
			require.Equal(t, uint64(i), txID)
			require.Equal(t, sha256.Sum256([]byte{byte(i)}), alh)
			require.Equal(t, int64(i*100), txOff)
			require.Equal(t, i*10, txSize)
		}
	}

	err = b.advanceReader(-1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = b.advanceReader(0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = b.advanceReader(size + 1)
	require.ErrorIs(t, err, ErrNotEnoughData)

	// advance reader should consume entries
	for i := 0; i < size; i++ {
		txID, alh, txOff, txSize, err := b.readAhead(0)
		require.NoError(t, err)
		require.Equal(t, uint64(i), txID)
		require.Equal(t, sha256.Sum256([]byte{byte(i)}), alh)
		require.Equal(t, int64(i*100), txOff)
		require.Equal(t, i*10, txSize)

		err = b.advanceReader(1)
		require.NoError(t, err)
	}

	_, _, _, _, err = b.readAhead(0)
	require.ErrorIs(t, err, ErrNotEnoughData)

	for i := 0; i < size; i++ {
		err := b.put(uint64(i), sha256.Sum256([]byte{byte(i)}), int64(i*100), i*10)
		require.NoError(t, err)
	}

	err = b.put(0, sha256.Sum256([]byte{byte(0)}), 0, 0)
	require.ErrorIs(t, err, ErrBufferIsFull)
}

// TestPrecommitBufferGrow exercises the recovery-path path that lets
// OpenStore replay a backlog larger than MaxActiveTransactions when async
// cLog flushing fell behind before the previous shutdown. See issue #2086.
func TestPrecommitBufferGrow(t *testing.T) {
	b := newPrecommitBuffer(4)

	for i := 0; i < 4; i++ {
		require.NoError(t, b.put(uint64(i+1), sha256.Sum256([]byte{byte(i + 1)}), int64((i+1)*100), (i+1)*10))
	}
	require.ErrorIs(t,
		b.put(99, sha256.Sum256([]byte{99}), 9900, 990),
		ErrBufferIsFull,
		"baseline: 5th put into a 4-slot buffer must reject")

	// grow to 16 slots and continue inserting; the head 4 entries must
	// remain intact and readable in their original order.
	b.grow(16)

	for i := 4; i < 14; i++ {
		require.NoError(t, b.put(uint64(i+1), sha256.Sum256([]byte{byte(i + 1)}), int64((i+1)*100), (i+1)*10))
	}

	for i := 0; i < 14; i++ {
		txID, alh, txOff, txSize, err := b.readAhead(i)
		require.NoErrorf(t, err, "readAhead(%d)", i)
		require.Equalf(t, uint64(i+1), txID, "readAhead(%d).txID", i)
		require.Equalf(t, sha256.Sum256([]byte{byte(i + 1)}), alh, "readAhead(%d).alh", i)
		require.Equalf(t, int64((i+1)*100), txOff, "readAhead(%d).txOff", i)
		require.Equalf(t, (i+1)*10, txSize, "readAhead(%d).txSize", i)
	}

	// grow to a smaller / equal size is a no-op.
	b.grow(2)
	require.Equal(t, 16, len(b.buf))

	// advance and grow on a partially-consumed buffer must keep the
	// remaining head entries intact and in order.
	require.NoError(t, b.advanceReader(5))
	b.grow(32)

	txID, _, _, _, err := b.readAhead(0)
	require.NoError(t, err)
	require.Equal(t, uint64(6), txID, "after advanceReader(5)+grow(32), readAhead(0) must be tx 6")

	// And further puts after grow continue from the last logical slot.
	require.NoError(t, b.put(99, sha256.Sum256([]byte{99}), 9900, 990))
	txID, _, _, _, err = b.readAhead(9)
	require.NoError(t, err)
	require.Equal(t, uint64(99), txID)
}

func TestPrecommitBufferRecedeWriter(t *testing.T) {
	size := 256
	b := newPrecommitBuffer(size)

	err := b.recedeWriter(-1)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = b.recedeWriter(0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	err = b.recedeWriter(1)
	require.ErrorIs(t, err, ErrNotEnoughData)

	for i := 0; i < size; i++ {
		err := b.put(uint64(i), sha256.Sum256([]byte{byte(i)}), int64(i*100), i*10)
		require.NoError(t, err)
	}

	err = b.recedeWriter(size + 1)
	require.ErrorIs(t, err, ErrNotEnoughData)

	err = b.recedeWriter(size / 2)
	require.NoError(t, err)

	require.Equal(t, size/2, b.freeSlots())

	for i := size / 2; i < size; i++ {
		err := b.put(uint64(i), sha256.Sum256([]byte{byte(i)}), int64(i*100), i*10)
		require.NoError(t, err)
	}

	err = b.recedeWriter(size - 1)
	require.NoError(t, err)

	txID, alh, txOff, txSize, err := b.readAhead(0)
	require.NoError(t, err)
	require.Zero(t, uint64(0), txID)
	require.Equal(t, sha256.Sum256([]byte{byte(0)}), alh)
	require.Equal(t, int64(0), txOff)
	require.Equal(t, 0, txSize)

	require.Equal(t, size-1, b.freeSlots())

	err = b.recedeWriter(1)
	require.NoError(t, err)

	require.Equal(t, size, b.freeSlots())
}
