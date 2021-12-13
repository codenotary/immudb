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
package store_test

import (
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestReadExpiredData(t *testing.T) {
	opts := store.DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := store.Open("data_concurrency", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_concurrency")

	now := time.Now()

	tx, err := immuStore.NewTx()
	require.NoError(t, err)

	md := store.NewKVMetadata()
	err = md.ExpiresAt(now)
	require.NoError(t, err)

	err = tx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
	require.NoError(t, err)

	txMeta, err := tx.Commit()
	require.NoError(t, err)

	// _ = txMeta
	// scan transaction entries
	txHolder := immuStore.NewTxHolder()
	err = immuStore.ReadTx(txMeta.ID, txHolder)
	require.NoError(t, err)

	_, err = immuStore.ReadValue(txHolder.Entries()[0])
	require.ErrorIs(t, err, store.ErrExpiredEntry)

	val, err := immuStore.ReadValue(store.NewTxEntry(
		txHolder.Entries()[0].Key(),
		nil,
		txHolder.Entries()[0].VLen(),
		txHolder.Entries()[0].HVal(),
		txHolder.Entries()[0].VOff(),
	))
	require.ErrorIs(t, err, store.ErrExpiredEntry)
	require.Nil(t, val)
}
