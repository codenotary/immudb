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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImmudbStoreReader(t *testing.T) {
	defer os.RemoveAll("data_store_reader")

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_store_reader", opts)
	require.NoError(t, err)

	defer immuStore.Close()

	txCount := 100
	eCount := 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			err = tx.Set(k[:], nil, v[:])
			require.NoError(t, err)
		}

		_, err = tx.Commit()
		require.NoError(t, err)
	}

	snap, err := immuStore.Snapshot()
	require.NoError(t, err)

	defer snap.Close()

	_, err = snap.NewKeyReader(nil)
	require.Equal(t, ErrIllegalArguments, err)

	reader, err := snap.NewKeyReader(&KeyReaderSpec{})
	require.NoError(t, err)

	defer reader.Close()

	for j := 0; j < eCount; j++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], uint64(j))

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], uint64(txCount-1))

		rk, vref, err := reader.Read()
		require.NoError(t, err)
		require.Equal(t, k[:], rk)

		rv, err := vref.Resolve()
		require.NoError(t, err)
		require.Equal(t, v[:], rv)
	}

	_, _, err = reader.Read()
	require.Equal(t, ErrNoMoreEntries, err)
}

func TestImmudbStoreReaderAsBefore(t *testing.T) {
	defer os.RemoveAll("data_store_reader_as_before")

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_store_reader_as_before", opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	txCount := 100
	eCount := 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			err = tx.Set(k[:], nil, v[:])
			require.NoError(t, err)
		}

		_, err = tx.Commit()
		require.NoError(t, err)
	}

	snap, err := immuStore.Snapshot()
	require.NoError(t, err)

	defer snap.Close()

	reader, err := snap.NewKeyReader(&KeyReaderSpec{})
	require.NoError(t, err)

	defer reader.Close()

	for i := 0; i < txCount; i++ {
		for j := 0; j < eCount; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			rk, vref, _, err := reader.ReadBetween(0, uint64(i+1))
			require.NoError(t, err)
			require.Equal(t, k[:], rk)

			rv, err := vref.Resolve()
			require.NoError(t, err)
			require.Equal(t, v[:], rv)
		}

		_, _, err = reader.Read()
		require.Equal(t, ErrNoMoreEntries, err)

		err = reader.Reset()
		require.NoError(t, err)
	}
}

func TestImmudbStoreReaderWithOffset(t *testing.T) {
	defer os.RemoveAll("data_store_reader_offset")

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_store_reader_offset", opts)
	require.NoError(t, err)

	defer immuStore.Close()

	txCount := 10
	eCount := 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			err = tx.Set(k[:], nil, v[:])
			require.NoError(t, err)
		}

		_, err = tx.Commit()
		require.NoError(t, err)
	}

	snap, err := immuStore.Snapshot()
	require.NoError(t, err)

	defer snap.Close()

	offset := eCount - 10

	reader, err := snap.NewKeyReader(&KeyReaderSpec{
		Offset: uint64(offset),
	})
	require.NoError(t, err)

	defer reader.Close()

	for j := 0; j < 10; j++ {
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], uint64(j+offset))

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], uint64(txCount-1))

		rk, vref, err := reader.Read()
		require.NoError(t, err)
		require.Equal(t, k[:], rk)

		rv, err := vref.Resolve()
		require.NoError(t, err)
		require.Equal(t, v[:], rv)
	}

	_, _, err = reader.Read()
	require.Equal(t, ErrNoMoreEntries, err)
}

func TestImmudbStoreReaderAsBeforeWithOffset(t *testing.T) {
	defer os.RemoveAll("data_store_reader_as_before_offset")

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_store_reader_as_before_offset", opts)
	require.NoError(t, err)

	defer immuStore.Close()

	txCount := 10
	eCount := 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			err = tx.Set(k[:], nil, v[:])
			require.NoError(t, err)
		}

		_, err = tx.Commit()
		require.NoError(t, err)
	}

	snap, err := immuStore.Snapshot()
	require.NoError(t, err)

	defer snap.Close()

	offset := eCount - 10

	reader, err := snap.NewKeyReader(&KeyReaderSpec{
		Offset: uint64(offset),
	})
	require.NoError(t, err)

	defer reader.Close()

	for i := 0; i < txCount; i++ {
		for j := 0; j < eCount-offset; j++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(j+offset))

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], uint64(i))

			rk, vref, _, err := reader.ReadBetween(0, uint64(i+1))
			require.NoError(t, err)
			require.Equal(t, k[:], rk)

			rv, err := vref.Resolve()
			require.NoError(t, err)
			require.Equal(t, v[:], rv)
		}

		_, _, err = reader.Read()
		require.Equal(t, ErrNoMoreEntries, err)

		err = reader.Reset()
		require.NoError(t, err)
	}
}
