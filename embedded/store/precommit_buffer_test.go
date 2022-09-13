/*
Copyright 2022 Codenotary Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

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
	require.ErrorIs(t, err, ErrBufferFullyConsumed)

	for i := 0; i < size; i++ {
		err := b.put(uint64(i), sha256.Sum256([]byte{byte(i)}), int64(i*100), i*10)
		require.NoError(t, err)
	}

	err = b.put(0, sha256.Sum256(nil), 0, 0)
	require.Equal(t, ErrBufferIsFull, err)

	for i := 0; i < size; i++ {
		txID, alh, txOff, txSize, err := b.readAhead(i)
		require.NoError(t, err)
		require.Equal(t, uint64(i), txID)
		require.Equal(t, sha256.Sum256([]byte{byte(i)}), alh)
		require.Equal(t, int64(i*100), txOff)
		require.Equal(t, i*10, txSize)
	}

	_, _, _, _, err = b.readAhead(size + 1)
	require.Equal(t, ErrBufferFullyConsumed, err)
}
