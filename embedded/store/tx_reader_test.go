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
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/stretchr/testify/require"
)

func TestTxReader(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	_, err = immuStore.NewTxReader(0, false, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	txHolder := tempTxHolder(t, immuStore)

	currTxID := uint64(1)
	txReader, err := immuStore.NewTxReader(currTxID, false, txHolder)
	require.NoError(t, err)

	for {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		require.NoError(t, err)
		require.Equal(t, currTxID, tx.header.ID)
		currTxID++
	}

	require.Equal(t, uint64(txCount), currTxID-1)

	currTxID = uint64(txCount)
	txReader, err = immuStore.NewTxReader(currTxID, true, txHolder)
	require.NoError(t, err)

	for {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		require.NoError(t, err)
		require.Equal(t, currTxID, tx.header.ID)
		currTxID--
	}

	require.Equal(t, uint64(0), currTxID)
}

func TestWrapAppendableErr(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	err = immuStore.wrapAppendableErr(nil, "anAction")
	require.NoError(t, err)

	unwrappedErr := errors.New("some error")
	err = immuStore.wrapAppendableErr(unwrappedErr, "anAction")
	require.ErrorIs(t, err, unwrappedErr)

	err = immuStore.wrapAppendableErr(singleapp.ErrAlreadyClosed, "anAction")
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.wrapAppendableErr(multiapp.ErrAlreadyClosed, "anAction")
	require.ErrorIs(t, err, ErrAlreadyClosed)
}
