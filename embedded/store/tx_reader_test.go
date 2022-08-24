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
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/stretchr/testify/require"
)

func TestTxReader(t *testing.T) {
	dir, err := ioutil.TempDir("", "data_txreader")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
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
	dir, err := ioutil.TempDir("", "data_txreader_wrap_error")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	err = immuStore.wrapAppendableErr(nil, "anAction")
	require.Nil(t, err)

	unwrappedErr := errors.New("some error")
	err = immuStore.wrapAppendableErr(unwrappedErr, "anAction")
	require.ErrorIs(t, err, unwrappedErr)

	err = immuStore.wrapAppendableErr(singleapp.ErrAlreadyClosed, "anAction")
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.wrapAppendableErr(multiapp.ErrAlreadyClosed, "anAction")
	require.ErrorIs(t, err, ErrAlreadyClosed)
}
