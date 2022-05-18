/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package database

import (
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.maxResultSize = 3

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`bbb`), Value: []byte(`item2`)}}})

	scanOptions := schema.ScanRequest{
		Prefix: []byte(`z`),
	}
	list, err := db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Empty(t, list.Entries)

	meta, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`abc`), Value: []byte(`item3`)}}})
	require.NoError(t, err)

	item, err := db.Get(&schema.KeyRequest{Key: []byte(`abc`), SinceTx: meta.Id})
	require.Equal(t, []byte(`abc`), item.Key)
	require.NoError(t, err)

	_, err = db.Scan(nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	scanOptions = schema.ScanRequest{
		SeekKey: []byte(`b`),
		Prefix:  []byte(`a`),
		Limit:   uint64(db.MaxResultSize() + 1),
		Desc:    true,
	}

	_, err = db.Scan(&scanOptions)
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	scanOptions = schema.ScanRequest{
		SeekKey: []byte(`b`),
		Prefix:  []byte(`a`),
		Limit:   0,
		Desc:    true,
	}

	list, err = db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 2, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`abc`))
	require.Equal(t, list.Entries[0].Value, []byte(`item3`))
	require.Equal(t, list.Entries[1].Key, []byte(`aaa`))
	require.Equal(t, list.Entries[1].Value, []byte(`item1`))

	scanOptions1 := schema.ScanRequest{
		SeekKey: []byte(`a`),
		Prefix:  nil,
		Limit:   0,
		Desc:    false,
	}

	list1, err := db.Scan(&scanOptions1)
	require.ErrorIs(t, err, ErrResultSizeLimitReached)
	require.Exactly(t, 3, len(list1.Entries))
	require.Equal(t, list1.Entries[0].Key, []byte(`aaa`))
	require.Equal(t, list1.Entries[0].Value, []byte(`item1`))
	require.Equal(t, list1.Entries[1].Key, []byte(`abc`))
	require.Equal(t, list1.Entries[1].Value, []byte(`item3`))
	require.Equal(t, list1.Entries[2].Key, []byte(`bbb`))
	require.Equal(t, list1.Entries[2].Value, []byte(`item2`))
}

func TestStoreScanPrefix(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`prefix:suffix1`), Value: []byte(`item1`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`prefix:suffix2`), Value: []byte(`item2`)}}})

	meta, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`prefix:suffix3`), Value: []byte(`item3`)}}})
	require.NoError(t, err)

	scanOptions := schema.ScanRequest{
		SeekKey: nil,
		Prefix:  []byte(`prefix:`),
		Limit:   0,
		Desc:    false,
		SinceTx: meta.Id,
	}

	list, err := db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 3, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`prefix:suffix1`))
	require.Equal(t, list.Entries[1].Key, []byte(`prefix:suffix2`))
	require.Equal(t, list.Entries[2].Key, []byte(`prefix:suffix3`))

	scanOptions = schema.ScanRequest{
		SeekKey: []byte(`prefix?`),
		Prefix:  []byte(`prefix:`),
		Limit:   0,
		Desc:    true,
		SinceTx: meta.Id,
	}

	list, err = db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 3, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`prefix:suffix3`))
	require.Equal(t, list.Entries[1].Key, []byte(`prefix:suffix2`))
	require.Equal(t, list.Entries[2].Key, []byte(`prefix:suffix1`))
}

func TestStoreScanDesc(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`item1`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`item2`)}}})

	meta, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`item3`)}}})
	require.NoError(t, err)

	scanOptions := schema.ScanRequest{
		SeekKey: []byte(`k`),
		Prefix:  []byte(`key`),
		Limit:   0,
		Desc:    false,
		SinceTx: meta.Id,
	}

	list, err := db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 3, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`key1`))
	require.Equal(t, list.Entries[1].Key, []byte(`key2`))
	require.Equal(t, list.Entries[2].Key, []byte(`key3`))

	scanOptions = schema.ScanRequest{
		SeekKey: []byte(`key22`),
		Prefix:  []byte(`key`),
		Limit:   0,
		Desc:    true,
		SinceTx: meta.Id,
	}

	list, err = db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 2, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`key2`))
	require.Equal(t, list.Entries[1].Key, []byte(`key1`))

	scanOptions = schema.ScanRequest{
		SeekKey: []byte(`key2`),
		Prefix:  []byte(`key`),
		Limit:   0,
		Desc:    true,
		SinceTx: meta.Id,
	}

	list, err = db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Exactly(t, 1, len(list.Entries))
	require.Equal(t, list.Entries[0].Key, []byte(`key1`))

	scanOptions = schema.ScanRequest{
		SeekKey: nil,
		Prefix:  []byte(`key`),
		Limit:   0,
		Desc:    true,
		SinceTx: meta.Id,
	}

	list, err = db.Scan(&scanOptions)
	require.NoError(t, err)
	require.Len(t, list.Entries, 3)
}

func TestStoreScanEndKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	for i := 1; i < 100; i++ {
		_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{
			Key:   []byte(fmt.Sprintf("key_%02d", i)),
			Value: []byte(fmt.Sprintf("val_%02d", i)),
		}}})
		require.NoError(t, err)
	}

	t.Run("not inclusive", func(t *testing.T) {
		res, err := db.Scan(&schema.ScanRequest{
			SeekKey: []byte("key_11"),
			EndKey:  []byte("key_44"),
		})
		require.NoError(t, err)
		require.Len(t, res.Entries, 44-12)
		for i := 12; i < 44; i++ {
			require.Equal(t, res.Entries[i-12].Key, []byte(fmt.Sprintf("key_%02d", i)))
			require.Equal(t, res.Entries[i-12].Value, []byte(fmt.Sprintf("val_%02d", i)))
		}
	})

	t.Run("inclusive seek", func(t *testing.T) {
		res, err := db.Scan(&schema.ScanRequest{
			SeekKey:       []byte("key_11"),
			EndKey:        []byte("key_44"),
			InclusiveSeek: true,
		})
		require.NoError(t, err)
		require.Len(t, res.Entries, 44-11)
		for i := 11; i < 44; i++ {
			require.Equal(t, res.Entries[i-11].Key, []byte(fmt.Sprintf("key_%02d", i)))
			require.Equal(t, res.Entries[i-11].Value, []byte(fmt.Sprintf("val_%02d", i)))
		}
	})

	t.Run("inclusive end", func(t *testing.T) {
		res, err := db.Scan(&schema.ScanRequest{
			SeekKey:      []byte("key_11"),
			EndKey:       []byte("key_44"),
			InclusiveEnd: true,
		})
		require.NoError(t, err)
		require.Len(t, res.Entries, 44-11)
		for i := 12; i <= 44; i++ {
			require.Equal(t, res.Entries[i-12].Key, []byte(fmt.Sprintf("key_%02d", i)))
			require.Equal(t, res.Entries[i-12].Value, []byte(fmt.Sprintf("val_%02d", i)))
		}
	})

	t.Run("inclusive seek and end", func(t *testing.T) {
		res, err := db.Scan(&schema.ScanRequest{
			SeekKey:       []byte("key_11"),
			EndKey:        []byte("key_44"),
			InclusiveSeek: true,
			InclusiveEnd:  true,
		})
		require.NoError(t, err)
		require.Len(t, res.Entries, 44-10)
		for i := 11; i <= 44; i++ {
			require.Equal(t, res.Entries[i-11].Key, []byte(fmt.Sprintf("key_%02d", i)))
			require.Equal(t, res.Entries[i-11].Value, []byte(fmt.Sprintf("val_%02d", i)))
		}
	})
}
