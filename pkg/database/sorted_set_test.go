/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package database

import (
	"context"
	"math"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreIndexExists(t *testing.T) {
	db := makeDb(t)

	db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)}}})
	db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)}}})

	_, err := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)}}})
	require.NoError(t, err)

	zaddOpts1 := &schema.ZAddRequest{
		Key:   []byte(`myFirstElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(14.6),
	}

	reference1, err1 := db.ZAdd(context.Background(), zaddOpts1)
	require.NoError(t, err1)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`mySecondElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(6),
	}

	reference2, err2 := db.ZAdd(context.Background(), zaddOpts2)
	require.NoError(t, err2)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts2 = &schema.ZAddRequest{
		Key:      []byte(`mySecondElementKey`),
		Set:      []byte(`firstIndex`),
		Score:    float64(6),
		AtTx:     0,
		BoundRef: true,
	}
	_, err2 = db.ZAdd(context.Background(), zaddOpts2)
	require.ErrorIs(t, err2, ErrIllegalArguments)

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`myThirdElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(14.5),
	}

	reference3, err3 := db.ZAdd(context.Background(), zaddOpts3)
	require.NoError(t, err3)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts := &schema.ZScanRequest{
		Set:   []byte(`firstIndex`),
		Limit: uint64(db.MaxResultSize() + 1),
	}

	_, err = db.ZScan(context.Background(), zscanOpts)
	require.ErrorIs(t, err, ErrResultSizeLimitExceeded)

	//try to retrieve directly the value or full scan to debug

	zscanOpts1 := &schema.ZScanRequest{
		Set: []byte(`firstIndex`),
	}

	itemList1, err := db.ZScan(context.Background(), zscanOpts1)
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

	itemList2, err := db.ZScan(context.Background(), zscanOpts2)
	require.NoError(t, err)
	require.Len(t, itemList2.Entries, 3)
	require.Equal(t, []byte(`myFirstElementKey`), itemList2.Entries[0].Entry.Key)
	require.Equal(t, []byte(`myThirdElementKey`), itemList2.Entries[1].Entry.Key)
	require.Equal(t, []byte(`mySecondElementKey`), itemList2.Entries[2].Entry.Key)
}

func TestStoreIndexEqualKeys(t *testing.T) {
	db := makeDb(t)

	_, err := db.ZAdd(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	i, err := db.SetReference(context.Background(), &schema.ReferenceRequest{Key: []byte(`myTag1`), ReferencedKey: []byte(`SignerId1`), AtTx: i1.Id, BoundRef: true})
	require.NoError(t, err)

	zaddOpts := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(1),
		Key:      []byte(`myTag1`),
		AtTx:     i.Id,
		BoundRef: true,
	}

	_, err = db.ZAdd(context.Background(), zaddOpts)
	require.ErrorIs(t, err, ErrReferencedKeyCannotBeAReference)

	zaddOpts1 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(1),
		Key:      []byte(`SignerId1`),
		AtTx:     i1.Id,
		BoundRef: true,
	}

	reference1, err1 := db.ZAdd(context.Background(), zaddOpts1)
	require.NoError(t, err1)
	require.Exactly(t, uint64(5), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:      []byte(`SignerId1`),
		Set:      []byte(`hashA`),
		Score:    float64(2),
		AtTx:     i2.Id,
		BoundRef: true,
	}

	reference2, err2 := db.ZAdd(context.Background(), zaddOpts2)
	require.NoError(t, err2)
	require.Exactly(t, uint64(6), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:      []byte(`SignerId2`),
		Set:      []byte(`hashA`),
		Score:    float64(3),
		AtTx:     i3.Id,
		BoundRef: true,
	}

	reference3, err3 := db.ZAdd(context.Background(), zaddOpts3)
	require.NoError(t, err3)
	require.Exactly(t, uint64(7), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(context.Background(), zscanOpts1)
	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Entries[2].Entry.Key)
}

func TestStoreIndexEqualKeysEqualScores(t *testing.T) {
	db := makeDb(t)

	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	score := float64(1.1)

	zaddOpts1 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    score,
		Key:      []byte(`SignerId1`),
		AtTx:     i1.Id,
		BoundRef: true,
	}

	reference1, err1 := db.ZAdd(context.Background(), zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:      []byte(`SignerId1`),
		Set:      []byte(`hashA`),
		Score:    score,
		AtTx:     i2.Id,
		BoundRef: true,
	}

	reference2, err2 := db.ZAdd(context.Background(), zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:      []byte(`SignerId2`),
		Set:      []byte(`hashA`),
		Score:    score,
		AtTx:     i3.Id,
		BoundRef: true,
	}

	reference3, err3 := db.ZAdd(context.Background(), zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(context.Background(), zscanOpts1)

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
	db := makeDb(t)

	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(1),
		Key:      []byte(`WrongKey`),
		AtTx:     i1.Id,
		BoundRef: true,
	}

	_, err := db.ZAdd(context.Background(), zaddOpts1)

	require.ErrorIs(t, err, store.ErrKeyNotFound)
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
	db := makeDb(t)

	setName := []byte(`set1`)
	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1`)}}})
	i2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2`)}}})
	i3, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3`)}}})
	i4, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key4`), Value: []byte(`val4`)}}})
	i5, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key5`), Value: []byte(`val5`)}}})
	i6, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key6`), Value: []byte(`val6`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(1),
		Key:      []byte(`key1`),
		AtTx:     i1.Id,
		BoundRef: true,
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(1),
		Key:      []byte(`key2`),
		AtTx:     i2.Id,
		BoundRef: true,
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key3`),
		AtTx:     i3.Id,
		BoundRef: true,
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key4`),
		AtTx:     i4.Id,
		BoundRef: true,
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key5`),
		AtTx:     i5.Id,
		BoundRef: true,
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(3),
		Key:      []byte(`key6`),
		AtTx:     i6.Id,
		BoundRef: true,
	}

	db.ZAdd(context.Background(), zaddOpts1)
	db.ZAdd(context.Background(), zaddOpts2)
	db.ZAdd(context.Background(), zaddOpts3)
	db.ZAdd(context.Background(), zaddOpts4)
	db.ZAdd(context.Background(), zaddOpts5)

	meta, err := db.ZAdd(context.Background(), zaddOpts6)
	require.NoError(t, err)

	zScanOption0 := &schema.ZScanRequest{
		Set:      setName,
		MinScore: &schema.Score{Score: 20},
		SinceTx:  meta.Id,
	}

	list0, err := db.ZScan(context.Background(), zScanOption0)
	require.NoError(t, err)
	require.Empty(t, list0.Entries)

	zScanOption1 := &schema.ZScanRequest{
		Set:      setName,
		SeekKey:  nil,
		Limit:    2,
		Desc:     false,
		MinScore: &schema.Score{Score: 2},
		MaxScore: &schema.Score{Score: 3},
		SinceTx:  meta.Id,
	}

	list1, err := db.ZScan(context.Background(), zScanOption1)
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

	list, err := db.ZScan(context.Background(), zScanOption2)
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
	db := makeDb(t)

	setName := []byte(`set1`)
	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1`)}}})
	i2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2`)}}})
	i3, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3`)}}})
	i4, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key4`), Value: []byte(`val4`)}}})
	i5, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key5`), Value: []byte(`val5`)}}})
	i6, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key6`), Value: []byte(`val6`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(1),
		Key:      []byte(`key1`),
		AtTx:     i1.Id,
		BoundRef: true,
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(1),
		Key:      []byte(`key2`),
		AtTx:     i2.Id,
		BoundRef: true,
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key3`),
		AtTx:     i3.Id,
		BoundRef: true,
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key4`),
		AtTx:     i4.Id,
		BoundRef: true,
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(2),
		Key:      []byte(`key5`),
		AtTx:     i5.Id,
		BoundRef: true,
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:      setName,
		Score:    float64(3),
		Key:      []byte(`key6`),
		AtTx:     i6.Id,
		BoundRef: true,
	}

	db.ZAdd(context.Background(), zaddOpts1)
	db.ZAdd(context.Background(), zaddOpts2)
	db.ZAdd(context.Background(), zaddOpts3)
	db.ZAdd(context.Background(), zaddOpts4)
	db.ZAdd(context.Background(), zaddOpts5)
	meta, err := db.ZAdd(context.Background(), zaddOpts6)
	require.NoError(t, err)

	zScanOption1 := &schema.ZScanRequest{
		Set:           setName,
		SeekKey:       []byte(`key6`),
		SeekScore:     math.MaxFloat64,
		SeekAtTx:      math.MaxUint64,
		InclusiveSeek: true,
		Limit:         2,
		Desc:          true,
		MaxScore:      &schema.Score{Score: 3},
		SinceTx:       meta.Id,
	}

	list1, err := db.ZScan(context.Background(), zScanOption1)
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

	list2, err := db.ZScan(context.Background(), zScanOption2)
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

	list3, err := db.ZScan(context.Background(), zScanOption3)
	require.NoError(t, err)
	require.Len(t, list3.Entries, 2)
	require.Equal(t, list3.Entries[0].Entry.Key, []byte(`key4`))
	require.Equal(t, list3.Entries[1].Entry.Key, []byte(`key3`))
}

func TestStore_ZScanInvalidSet(t *testing.T) {
	db := makeDb(t)

	opt := &schema.ZScanRequest{
		Set: nil,
	}
	_, err := db.ZScan(context.Background(), opt)
	require.ErrorIs(t, err, store.ErrIllegalArguments)
}

func TestStore_ZScanOnEqualKeysWithSameScoreAreReturnedOrderedByTS(t *testing.T) {
	db := makeDb(t)

	idx0, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-A`)}}})
	db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2-A`)}}})
	idx2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-B`)}}})
	db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3-A`)}}})
	idx4, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-C`)}}})

	db.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:      []byte(`mySet`),
		Score:    0,
		Key:      []byte(`key1`),
		AtTx:     idx2.Id,
		BoundRef: true,
	})
	db.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:      []byte(`mySet`),
		Score:    0,
		Key:      []byte(`key1`),
		AtTx:     idx0.Id,
		BoundRef: true,
	})
	db.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key2`),
	})
	db.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: 0,
		Key:   []byte(`key3`),
	})
	meta, _ := db.ZAdd(context.Background(), &schema.ZAddRequest{
		Set:      []byte(`mySet`),
		Score:    0,
		Key:      []byte(`key1`),
		AtTx:     idx4.Id,
		BoundRef: true,
	})

	ZScanRequest := &schema.ZScanRequest{
		Set:     []byte(`mySet`),
		SinceTx: meta.Id,
	}

	list, err := db.ZScan(context.Background(), ZScanRequest)
	require.NoError(t, err)
	// same key, sorted by internal timestamp
	require.Exactly(t, []byte(`val1-A`), list.Entries[0].Entry.Value)
	require.Exactly(t, []byte(`val1-B`), list.Entries[1].Entry.Value)
	require.Exactly(t, []byte(`val1-C`), list.Entries[2].Entry.Value)
	require.Exactly(t, []byte(`val2-A`), list.Entries[3].Entry.Value)
	require.Exactly(t, []byte(`val3-A`), list.Entries[4].Entry.Value)
}

func TestStoreZScanOnZAddIndexReference(t *testing.T) {
	db := makeDb(t)

	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(1),
		Key:      []byte(`SignerId1`),
		AtTx:     i1.Id,
		BoundRef: true,
	}

	reference1, err1 := db.ZAdd(context.Background(), zaddOpts1)
	require.NoError(t, err1)
	require.Exactly(t, uint64(4), reference1.Id)
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(2),
		Key:      []byte(`SignerId1`),
		AtTx:     i2.Id,
		BoundRef: true,
	}

	reference2, err2 := db.ZAdd(context.Background(), zaddOpts2)
	require.NoError(t, err2)
	require.Exactly(t, uint64(5), reference2.Id)
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Set:      []byte(`hashA`),
		Score:    float64(3),
		Key:      []byte(`SignerId2`),
		AtTx:     i3.Id,
		BoundRef: true,
	}

	reference3, err3 := db.ZAdd(context.Background(), zaddOpts3)
	require.NoError(t, err3)
	require.Exactly(t, uint64(6), reference3.Id)
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Desc:    false,
		SinceTx: reference3.Id,
	}

	itemList1, err := db.ZScan(context.Background(), zscanOpts1)
	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[0].Entry.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Entries[1].Entry.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Entries[2].Entry.Key)

}

func TestStoreVerifiableZAdd(t *testing.T) {
	db := makeDb(t)

	_, err := db.VerifiableZAdd(context.Background(), nil)
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	i1, _ := db.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`value1`)}}})

	_, err = db.VerifiableZAdd(context.Background(), &schema.VerifiableZAddRequest{
		ZAddRequest:  nil,
		ProveSinceTx: i1.Id,
	})
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	_, err = db.VerifiableZAdd(context.Background(), &schema.VerifiableZAddRequest{
		ZAddRequest:  nil,
		ProveSinceTx: i1.Id,
	})
	require.ErrorIs(t, err, store.ErrIllegalArguments)

	req := &schema.ZAddRequest{
		Set:      []byte(`set1`),
		Key:      []byte(`key1`),
		Score:    float64(1.1),
		AtTx:     i1.Id,
		BoundRef: true,
	}

	vtx, err := db.VerifiableZAdd(context.Background(), &schema.VerifiableZAddRequest{
		ZAddRequest:  req,
		ProveSinceTx: i1.Id,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), vtx.Tx.Header.Id)

	ekv := EncodeZAdd(req.Set, req.Score, EncodeKey(req.Key), req.AtTx)
	require.Equal(t, ekv.Key, vtx.Tx.Entries[0].Key)
	require.Equal(t, int32(0), vtx.Tx.Entries[0].VLen)

	dualProof := schema.DualProofFromProto(vtx.DualProof)

	verifies := store.VerifyDualProof(
		dualProof,
		i1.Id,
		vtx.Tx.Header.Id,
		schema.TxHeaderFromProto(i1).Alh(),
		dualProof.TargetTxHeader.Alh(),
	)
	require.True(t, verifies)

	zscanReq := &schema.ZScanRequest{
		Set:     []byte(`set1`),
		SinceTx: vtx.Tx.Header.Id,
	}

	itemList1, err := db.ZScan(context.Background(), zscanReq)
	require.NoError(t, err)
	require.Len(t, itemList1.Entries, 1)
	require.Equal(t, req.Key, itemList1.Entries[0].Entry.Key)
	require.Equal(t, req.Score, itemList1.Entries[0].Score)
}
