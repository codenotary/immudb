/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/stretchr/testify/require"
)

func TestImmudbStoreReader(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_store_reader", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_store_reader")

	txCount := 100
	eCount := 100

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			kvs[j] = &KV{Key: k, Value: v}
		}

		_, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
	}

	time.Sleep(1 * time.Second)

	snap, err := immuStore.Snapshot()
	require.NoError(t, err)

	_, err = snap.NewKeyReader(nil)
	require.Equal(t, ErrIllegalArguments, err)

	spec := &tbtree.ReaderSpec{}
	reader, err := snap.NewKeyReader(spec)
	require.NoError(t, err)

	defer reader.Close()

	for {
		_, v, _, _, err := reader.Read()
		if err != nil {
			require.Equal(t, ErrNoMoreEntries, err)
			break
		}

		_, err = v.Resolve()
		require.NoError(t, err)
	}
}
