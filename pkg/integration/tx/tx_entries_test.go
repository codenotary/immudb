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

package integration

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func Test_GetTransactionEntries(t *testing.T) {
	_, client := setupTest(t, -1)

	hdr, err := client.ExecAll(context.Background(), &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key1"),
						Value: []byte("value1"),
					},
				},
			},
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte("ref1"),
						ReferencedKey: []byte("key1"),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte("set1"),
						Score: 10,
						Key:   []byte("key1"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Run("all entries should be resolved", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx: hdr.Id,
				EntriesSpec: &schema.EntriesSpec{
					KvEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_RESOLVE,
					},
					ZEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_RESOLVE,
					},
				},
			})
		require.NoError(t, err)
		require.Empty(t, tx.Entries)
		require.Len(t, tx.KvEntries, 2)
		require.Len(t, tx.ZEntries, 1)

		require.Equal(t, []byte("key1"), tx.KvEntries[0].Key)
		require.Equal(t, []byte("value1"), tx.KvEntries[0].Value)

		require.Equal(t, []byte("key1"), tx.KvEntries[1].Key)
		require.Equal(t, []byte("value1"), tx.KvEntries[1].Value)
		require.NotNil(t, tx.KvEntries[1].ReferencedBy)
		require.Equal(t, []byte("ref1"), tx.KvEntries[1].ReferencedBy.Key)

		require.Equal(t, []byte("set1"), tx.ZEntries[0].Set)
		require.Equal(t, []byte("key1"), tx.ZEntries[0].Key)
		require.Equal(t, float64(10), tx.ZEntries[0].Score)
	})

	t.Run("only resolved kv entries should be returned", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx: hdr.Id,
				EntriesSpec: &schema.EntriesSpec{
					KvEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_RESOLVE,
					},
					ZEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_EXCLUDE,
					},
				},
			})
		require.NoError(t, err)
		require.Empty(t, tx.Entries)
		require.Len(t, tx.KvEntries, 2)
		require.Empty(t, tx.ZEntries)

		require.Equal(t, []byte("key1"), tx.KvEntries[0].Key)
		require.Equal(t, []byte("value1"), tx.KvEntries[0].Value)

		require.Equal(t, []byte("key1"), tx.KvEntries[1].Key)
		require.Equal(t, []byte("value1"), tx.KvEntries[1].Value)
		require.NotNil(t, tx.KvEntries[1].ReferencedBy)
		require.Equal(t, []byte("ref1"), tx.KvEntries[1].ReferencedBy.Key)
	})

	t.Run("only resolved zentries should be returned", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx: hdr.Id,
				EntriesSpec: &schema.EntriesSpec{
					KvEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_EXCLUDE,
					},
					ZEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_RESOLVE,
					},
				},
			})
		require.NoError(t, err)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Len(t, tx.ZEntries, 1)

		require.Equal(t, []byte("set1"), tx.ZEntries[0].Set)
		require.Equal(t, []byte("key1"), tx.ZEntries[0].Key)
		require.Equal(t, float64(10), tx.ZEntries[0].Score)
	})

	t.Run("all entries should be excluded", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx: hdr.Id,
				EntriesSpec: &schema.EntriesSpec{
					KvEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_EXCLUDE,
					},
					ZEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_EXCLUDE,
					},
				},
			})
		require.NoError(t, err)
		require.Empty(t, tx.Entries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("all entries should be unresolved if no spec is provided", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(), &schema.TxRequest{Tx: hdr.Id})
		require.NoError(t, err)
		require.Len(t, tx.Entries, 3)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("no entries should be returned if an empty spec is provided", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx:          hdr.Id,
				EntriesSpec: &schema.EntriesSpec{},
			})
		require.NoError(t, err)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)
	})

	t.Run("all kv entries should be unresolved but including the raw value", func(t *testing.T) {
		tx, err := client.TxByIDWithSpec(context.Background(),
			&schema.TxRequest{
				Tx: hdr.Id,
				EntriesSpec: &schema.EntriesSpec{
					KvEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_RAW_VALUE,
					},
					ZEntriesSpec: &schema.EntryTypeSpec{
						Action: schema.EntryTypeAction_EXCLUDE,
					},
				},
			})
		require.NoError(t, err)
		require.Len(t, tx.Entries, 2)
		require.Empty(t, tx.KvEntries)
		require.Empty(t, tx.ZEntries)

		for _, e := range tx.Entries {
			require.Equal(t, int(e.VLen), len(e.Value))

			hval := sha256.Sum256(e.Value)
			require.Equal(t, e.HValue, hval[:])
		}
	})

	err = client.CloseSession(context.Background())
	require.NoError(t, err)
}
