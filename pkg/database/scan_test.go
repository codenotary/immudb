package database

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreScan(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`item1`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`bbb`), Value: []byte(`item2`)}}})

	meta, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`abc`), Value: []byte(`item3`)}}})
	require.NoError(t, err)

	item, err := db.Get(&schema.KeyRequest{Key: []byte(`abc`), SinceTx: meta.Id})
	require.Equal(t, []byte(`abc`), item.Key)
	require.NoError(t, err)

	scanOptions := schema.ScanRequest{
		SeekKey: []byte(`b`),
		Prefix:  []byte(`a`),
		Limit:   0,
		Desc:    true,
		SinceTx: meta.Id,
	}

	list, err := db.Scan(&scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 2, len(list.Entries))
	assert.Equal(t, list.Entries[0].Key, []byte(`abc`))
	assert.Equal(t, list.Entries[0].Value, []byte(`item3`))
	assert.Equal(t, list.Entries[1].Key, []byte(`aaa`))
	assert.Equal(t, list.Entries[1].Value, []byte(`item1`))

	scanOptions1 := schema.ScanRequest{
		SeekKey: []byte(`a`),
		Prefix:  nil,
		Limit:   0,
		Desc:    false,
		SinceTx: meta.Id,
	}

	list1, err1 := db.Scan(&scanOptions1)
	assert.NoError(t, err1)
	assert.Exactly(t, 3, len(list1.Entries))
	assert.Equal(t, list1.Entries[0].Key, []byte(`aaa`))
	assert.Equal(t, list1.Entries[0].Value, []byte(`item1`))
	assert.Equal(t, list1.Entries[1].Key, []byte(`abc`))
	assert.Equal(t, list1.Entries[1].Value, []byte(`item3`))
	assert.Equal(t, list1.Entries[2].Key, []byte(`bbb`))
	assert.Equal(t, list1.Entries[2].Value, []byte(`item2`))

}
