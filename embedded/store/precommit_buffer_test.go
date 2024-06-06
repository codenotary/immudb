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
