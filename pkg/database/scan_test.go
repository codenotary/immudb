package database

import (
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

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
		Limit:   MaxKeyScanLimit + 1,
		Desc:    true,
	}

	_, err = db.Scan(&scanOptions)
	require.Equal(t, ErrMaxKeyScanLimitExceeded, err)

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

	list1, err1 := db.Scan(&scanOptions1)
	require.NoError(t, err1)
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
