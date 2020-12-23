/*
Copyright 2019-2020 vChain, Inc.

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
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreIndexExists(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)}}})

	_, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)}}})
	require.NoError(t, err)

	zaddOpts1 := &schema.ZAddRequest{
		Key:   []byte(`myFirstElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(14.6),
	}

	reference1, err1 := db.ZAdd(zaddOpts1)
	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(4))
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`mySecondElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(6),
	}

	reference2, err2 := db.ZAdd(zaddOpts2)
	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(5))
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`myThirdElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(14.5),
	}

	reference3, err3 := db.ZAdd(zaddOpts3)
	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(6))
	require.NotEmptyf(t, reference3, "Should not be empty")

	//try to retrieve directly the value or full scan to debug

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`firstIndex`),
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`mySecondElementKey`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`myThirdElementKey`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`myFirstElementKey`), itemList1.Entries[2].Entry.Key)

	zscanOpts2 := &schema.ZScanRequest{
		Set:      []byte(`firstIndex`),
		MaxScore: &schema.Score{Score: 100.0},
		Desc:     true,
	}

	itemList2, err := db.ZScan(zscanOpts2)

	require.NoError(t, err)
	require.Len(t, itemList2.Entries, 3)
	require.Equal(t, []byte(`myFirstElementKey`), itemList2.Entries[0].Entry.Key)
	require.Equal(t, []byte(`myThirdElementKey`), itemList2.Entries[1].Entry.Key)
	require.Equal(t, []byte(`mySecondElementKey`), itemList2.Entries[2].Entry.Key)
}

func TestStoreIndexEqualKeys(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: float64(1),
		Key:   []byte(`SignerId1`),
		AtTx:  i1.Id,
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(4))
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`SignerId1`),
		Set:   []byte(`hashA`),
		Score: float64(2),
		AtTx:  i2.Id,
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(5))
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`SignerId2`),
		Set:   []byte(`hashA`),
		Score: float64(3),
		AtTx:  i3.Id,
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(6))
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Entries[2].Entry.Key)

}

func TestStoreIndexEqualKeysEqualScores(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	score := float64(1.1)

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: score,
		Key:   []byte(`SignerId1`),
		AtTx:  i1.Id,
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`SignerId1`),
		Set:   []byte(`hashA`),
		Score: score,
		AtTx:  i2.Id,
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`SignerId2`),
		Set:   []byte(`hashA`),
		Score: score,
		AtTx:  i3.Id,
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`firstValue`), itemList1.Entries[0].Entry.Value)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`secondValue`), itemList1.Entries[1].Entry.Value)
	require.Equal(t, []byte(`SignerId2`), itemList1.Entries[2].Entry.Key)
	require.Equal(t, []byte(`thirdValue`), itemList1.Entries[2].Entry.Value)

}

func TestStoreIndexEqualKeysMismatchError(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: float64(1),
		Key:   []byte(`WrongKey`),
		AtTx:  i1.Id,
	}

	_, err := db.ZAdd(zaddOpts1)

	require.Error(t, store.ErrKeyNotFound, err)
}

// TestStore_ZScanMinMax
// set1
// key: key1, score: 1
// key: key2, score: 1
// key: key3, score: 2
// key: key4, score: 2
// key: key5, score: 2
// key: key6, score: 3/*
func TestStore_ZScanPagination(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	setName := []byte(`set1`)
	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3`)}}})
	i4, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key4`), Value: []byte(`val4`)}}})
	i5, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key5`), Value: []byte(`val5`)}}})
	i6, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key6`), Value: []byte(`val6`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(1),
		Key:   []byte(`key1`),
		AtTx:  i1.Id,
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(1),
		Key:   []byte(`key2`),
		AtTx:  i2.Id,
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key3`),
		AtTx:  i3.Id,
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key4`),
		AtTx:  i4.Id,
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key5`),
		AtTx:  i5.Id,
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(3),
		Key:   []byte(`key6`),
		AtTx:  i6.Id,
	}

	db.ZAdd(zaddOpts1)
	db.ZAdd(zaddOpts2)
	db.ZAdd(zaddOpts3)
	db.ZAdd(zaddOpts4)
	db.ZAdd(zaddOpts5)

	meta, err := db.ZAdd(zaddOpts6)
	require.NoError(t, err)

	zScanOption1 := &schema.ZScanRequest{
		Set:      setName,
		SeekKey:  nil,
		Limit:    2,
		Desc:     false,
		MinScore: &schema.Score{Score: 2},
		MaxScore: &schema.Score{Score: 3},
		SinceTx:  meta.Id,
	}

	list1, err := db.ZScan(zScanOption1)
	require.NoError(t, err)
	require.Len(t, list1.Entries, 2)
	require.Equal(t, list1.Entries[0].Entry.Key, []byte(`key3`))
	require.Equal(t, list1.Entries[1].Entry.Key, []byte(`key4`))

	lastItem := list1.Entries[len(list1.Entries)-1]

	zScanOption2 := &schema.ZScanRequest{
		Set:       setName,
		SeekKey:   lastItem.Key,
		SeekScore: lastItem.Score,
		SeekAtTx:  lastItem.AtTx,
		Limit:     2,
		Desc:      false,
		MinScore:  &schema.Score{Score: 2},
		MaxScore:  &schema.Score{Score: 3},
		SinceTx:   meta.Id,
	}

	list, err := db.ZScan(zScanOption2)
	require.NoError(t, err)
	require.Len(t, list.Entries, 2)
	require.Equal(t, list.Entries[0].Entry.Key, []byte(`key5`))
	require.Equal(t, list.Entries[1].Entry.Key, []byte(`key6`))
}

// TestStore_ZScanMinMax
// set1
// key: key1, score: 1
// key: key2, score: 1
// key: key3, score: 2
// key: key4, score: 2
// key: key5, score: 2
// key: key6, score: 3
func TestStore_ZScanReversePagination(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	setName := []byte(`set1`)
	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3`)}}})
	i4, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key4`), Value: []byte(`val4`)}}})
	i5, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key5`), Value: []byte(`val5`)}}})
	i6, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key6`), Value: []byte(`val6`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(1),
		Key:   []byte(`key1`),
		AtTx:  i1.Id,
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(1),
		Key:   []byte(`key2`),
		AtTx:  i2.Id,
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key3`),
		AtTx:  i3.Id,
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key4`),
		AtTx:  i4.Id,
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(2),
		Key:   []byte(`key5`),
		AtTx:  i5.Id,
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:   setName,
		Score: float64(3),
		Key:   []byte(`key6`),
		AtTx:  i6.Id,
	}

	db.ZAdd(zaddOpts1)
	db.ZAdd(zaddOpts2)
	db.ZAdd(zaddOpts3)
	db.ZAdd(zaddOpts4)
	db.ZAdd(zaddOpts5)
	meta, err := db.ZAdd(zaddOpts6)
	require.NoError(t, err)

	zScanOption1 := &schema.ZScanRequest{
		Set:      setName,
		SeekKey:  nil,
		Limit:    2,
		Desc:     true,
		MaxScore: &schema.Score{Score: 3},
		SinceTx:  meta.Id,
	}

	list1, err := db.ZScan(zScanOption1)
	require.NoError(t, err)
	require.Len(t, list1.Entries, 2)
	require.Equal(t, list1.Entries[0].Entry.Key, []byte(`key6`))
	require.Equal(t, list1.Entries[1].Entry.Key, []byte(`key5`))

	lastItem := list1.Entries[len(list1.Entries)-1]

	zScanOption2 := &schema.ZScanRequest{
		Set:           setName,
		SeekScore:     lastItem.Score,
		SeekAtTx:      lastItem.AtTx,
		SeekKey:       lastItem.Key,
		Limit:         2,
		InclusiveSeek: true,
		Desc:          true,
		SinceTx:       meta.Id,
	}

	list2, err := db.ZScan(zScanOption2)
	require.NoError(t, err)
	require.Len(t, list2.Entries, 2)
	require.Equal(t, list2.Entries[0].Entry.Key, []byte(`key5`))
	require.Equal(t, list2.Entries[1].Entry.Key, []byte(`key4`))

	zScanOption3 := &schema.ZScanRequest{
		Set:           setName,
		SeekScore:     lastItem.Score,
		SeekAtTx:      lastItem.AtTx,
		SeekKey:       lastItem.Key,
		Limit:         2,
		InclusiveSeek: false,
		Desc:          true,
		SinceTx:       meta.Id,
	}

	list3, err := db.ZScan(zScanOption3)
	require.NoError(t, err)
	require.Len(t, list3.Entries, 2)
	require.Equal(t, list3.Entries[0].Entry.Key, []byte(`key4`))
	require.Equal(t, list3.Entries[1].Entry.Key, []byte(`key3`))
}

func TestStore_ZScanInvalidSet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	opt := &schema.ZScanRequest{
		Set: nil,
	}
	_, err := db.ZScan(opt)
	require.Error(t, store.ErrIllegalArguments, err)
}

func TestStore_ZScanOnEqualKeysWithSameScoreAreReturnedOrderedByTS(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx0, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-A`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2-A`)}}})
	idx2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-B`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3-A`)}}})
	idx4, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-C`)}}})

	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key1`),
		AtTx:  idx2.Id,
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key1`),
		AtTx:  idx0.Id,
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key2`),
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key3`),
	})
	meta, _ := db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key1`),
		AtTx:  idx4.Id,
	})

	ZScanRequest := &schema.ZScanRequest{
		Set:     []byte(`mySet`),
		SinceTx: meta.Id,
	}

	list, err := db.ZScan(ZScanRequest)

	require.NoError(t, err)
	// same key, sorted by internal timestamp
	require.Exactly(t, []byte(`val1-A`), list.Entries[0].Entry.Value)
	require.Exactly(t, []byte(`val1-B`), list.Entries[1].Entry.Value)
	require.Exactly(t, []byte(`val1-C`), list.Entries[2].Entry.Value)
	require.Exactly(t, []byte(`val2-A`), list.Entries[3].Entry.Value)
	require.Exactly(t, []byte(`val3-A`), list.Entries[4].Entry.Value)
}

func TestStoreZScanOnZAddIndexReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: float64(1),
		Key:   []byte(`SignerId1`),
		AtTx:  i1.Id,
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: float64(2),
		Key:   []byte(`SignerId1`),
		AtTx:  i2.Id,
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: float64(3),
		Key:   []byte(`SignerId2`),
		AtTx:  i3.Id,
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Entries[2].Entry.Key)

}
