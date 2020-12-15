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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/common"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestStoreIndexExists(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)}}})
	idx1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Key:   []byte(`myFirstElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(14.6)},
	}

	db.waitForIndexing(idx1.Id)

	reference1, err1 := db.ZAdd(zaddOpts1)
	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(4))
	require.NotEmptyf(t, reference1, "Should not be empty")

	//db.waitForIndexing(reference1.Id)

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`mySecondElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(6)},
	}

	reference2, err2 := db.ZAdd(zaddOpts2)
	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(5))
	require.NotEmptyf(t, reference2, "Should not be empty")

	//db.waitForIndexing(reference2.Id)

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`myThirdElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(14.5)},
	}

	reference3, err3 := db.ZAdd(zaddOpts3)
	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(6))
	require.NotEmptyf(t, reference3, "Should not be empty")

	db.waitForIndexing(reference3.Id)

	//try to retrieve directly the value or full scan to debug

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`firstIndex`),
		Reverse: true,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Items, 3)
	require.Equal(t, []byte(`mySecondElementKey`), itemList1.Items[0].Item.Key)
	require.Equal(t, []byte(`myThirdElementKey`), itemList1.Items[1].Item.Key)
	require.Equal(t, []byte(`myFirstElementKey`), itemList1.Items[2].Item.Key)

	/*zscanOpts2 := &schema.ZScanRequest{
		Set:     []byte(`firstIndex`),
		Reverse: true,
	}

	itemList2, err := db.ZScan(zscanOpts2)

	require.NoError(t, err)
	require.Len(t, itemList2.Items, 3)
	require.Equal(t, []byte(`myFirstElementKey`), itemList2.Items[0].Item.Key)
	require.Equal(t, []byte(`myThirdElementKey`), itemList2.Items[1].Item.Key)
	require.Equal(t, []byte(`mySecondElementKey`), itemList2.Items[2].Item.Key)*/
}

func TestStoreIndexEqualKeys(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`SignerId1`),
		AtTx:  int64(i1.Id),
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(4))
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`SignerId1`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(2)},
		AtTx:  int64(i2.Id),
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(5))
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`SignerId2`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(3)},
		AtTx:  int64(i3.Id),
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(6))
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Reverse: false,
	}
	db.waitForIndexing(reference3.Id)
	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Items, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[0].Item.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[1].Item.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Items[2].Item.Key)

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
		Score: &schema.Score{Score: score},
		Key:   []byte(`SignerId1`),
		AtTx:  int64(i1.Id),
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(3))
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Key:   []byte(`SignerId1`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: score},
		AtTx:  int64(i2.Id),
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(4))
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Key:   []byte(`SignerId2`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: score},
		AtTx:  int64(i3.Id),
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(5))
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Reverse: false,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Items, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[0].Item.Key)
	require.Equal(t, []byte(`firstValue`), itemList1.Items[0].Item.Value)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[1].Item.Key)
	require.Equal(t, []byte(`secondValue`), itemList1.Items[1].Item.Value)
	require.Equal(t, []byte(`SignerId2`), itemList1.Items[2].Item.Key)
	require.Equal(t, []byte(`thirdValue`), itemList1.Items[2].Item.Value)

}

func TestStoreIndexEqualKeysMismatchError(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`WrongKey`),
		AtTx:  int64(i1.Id),
	}

	_, err := db.ZAdd(zaddOpts1)

	require.Error(t, err)
	require.Equal(t, err, ErrIndexKeyMismatch)
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
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key1`),
		AtTx:  int64(i1.Id),
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key2`),
		AtTx:  int64(i2.Id),
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key3`),
		AtTx:  int64(i3.Id),
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key4`),
		AtTx:  int64(i4.Id),
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key5`),
		AtTx:  int64(i5.Id),
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(3)},
		Key:   []byte(`key6`),
		AtTx:  int64(i6.Id),
	}

	db.ZAdd(zaddOpts1)
	db.ZAdd(zaddOpts2)
	db.ZAdd(zaddOpts3)
	db.ZAdd(zaddOpts4)
	db.ZAdd(zaddOpts5)
	db.ZAdd(zaddOpts6)

	zScanOption1 := &schema.ZScanRequest{
		Set:     setName,
		Offset:  nil,
		Limit:   2,
		Reverse: false,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list1, err := db.ZScan(zScanOption1)
	require.NoError(t, err)
	require.Len(t, list1.Items, 2)
	require.Equal(t, list1.Items[0].Item.Key, []byte(`key3`))
	require.Equal(t, list1.Items[1].Item.Key, []byte(`key4`))

	zScanOption2 := &schema.ZScanRequest{
		Set:     setName,
		Offset:  list1.Items[len(list1.Items)-1].CurrentOffset,
		Limit:   2,
		Reverse: false,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list, err := db.ZScan(zScanOption2)
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	require.Equal(t, list.Items[0].Item.Key, []byte(`key5`))
	require.Equal(t, list.Items[1].Item.Key, []byte(`key6`))
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
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key1`),
		AtTx:  int64(i1.Id),
	}
	zaddOpts2 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key2`),
		AtTx:  int64(i2.Id),
	}
	zaddOpts3 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key3`),
		AtTx:  int64(i3.Id),
	}
	zaddOpts4 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key4`),
		AtTx:  int64(i4.Id),
	}
	zaddOpts5 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key5`),
		AtTx:  int64(i5.Id),
	}
	zaddOpts6 := &schema.ZAddRequest{
		Set:   setName,
		Score: &schema.Score{Score: float64(3)},
		Key:   []byte(`key6`),
		AtTx:  int64(i6.Id),
	}

	db.ZAdd(zaddOpts1)
	db.ZAdd(zaddOpts2)
	db.ZAdd(zaddOpts3)
	db.ZAdd(zaddOpts4)
	db.ZAdd(zaddOpts5)
	db.ZAdd(zaddOpts6)

	zScanOption1 := &schema.ZScanRequest{
		Set:     setName,
		Offset:  nil,
		Limit:   2,
		Reverse: true,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list1, err := db.ZScan(zScanOption1)
	require.NoError(t, err)
	require.Len(t, list1.Items, 2)
	require.Equal(t, list1.Items[0].Item.Key, []byte(`key6`))
	require.Equal(t, list1.Items[1].Item.Key, []byte(`key5`))

	zScanOption2 := &schema.ZScanRequest{
		Set:     setName,
		Offset:  list1.Items[len(list1.Items)-1].CurrentOffset,
		Limit:   2,
		Reverse: true,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list2, err := db.ZScan(zScanOption2)
	require.NoError(t, err)
	require.Len(t, list2.Items, 2)
	require.Equal(t, list2.Items[0].Item.Key, []byte(`key4`))
	require.Equal(t, list2.Items[1].Item.Key, []byte(`key3`))
}

func TestFloat(t *testing.T) {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	for i := 0; i < 100; i++ {
		n := r.Float64()
		bs := common.Float642bytes(n)
		require.NotNil(t, bs)
		require.True(t, len(bs) == 8)
		require.Equal(t, n, common.Bytes2float(bs))
	}
}

/*func TestStore_ZScanInvalidSet(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	opt := &schema.ZScanRequest{
		Set: []byte{tsPrefix},
	}
	_, err := db.ZScan(opt)
	require.Error(t, err, ErrInvalidSet)
}

func TestStore_ZScanInvalidOffset(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	opt := &schema.ZScanRequest{
		Set:    []byte(`set`),
		Offset: []byte{tsPrefix},
	}
	_, err := db.ZScan(opt)
	require.Error(t, err, ErrInvalidOffset)
}*/

func TestStore_ZScanOnEqualKeysWithSameScoreAreReturnedOrderedByTS(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx0, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-C`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key2`), Value: []byte(`val2-A`)}}})
	idx2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-B`)}}})
	db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key3`), Value: []byte(`val3-A`)}}})
	idx4, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`key1`), Value: []byte(`val1-A`)}}})

	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: &schema.Score{Score: 0},
		Key:   []byte(`key1`),
		AtTx:  int64(idx2.Id),
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: &schema.Score{Score: 0},
		Key:   []byte(`key1`),
		AtTx:  int64(idx0.Id),
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: &schema.Score{Score: 0},
		Key:   []byte(`key2`),
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: &schema.Score{Score: 0},
		Key:   []byte(`key3`),
	})
	db.ZAdd(&schema.ZAddRequest{
		Set:   []byte(`mySet`),
		Score: &schema.Score{Score: 0},
		Key:   []byte(`key1`),
		AtTx:  int64(idx4.Id),
	})

	ZScanRequest := &schema.ZScanRequest{
		Set: []byte(`mySet`),
	}

	list, err := db.ZScan(ZScanRequest)

	require.NoError(t, err)
	// same key, sorted by internal timestamp
	require.Exactly(t, []byte(`val1-C`), list.Items[0].Item.Value)
	require.Exactly(t, []byte(`val1-B`), list.Items[1].Item.Value)
	require.Exactly(t, []byte(`val1-A`), list.Items[2].Item.Value)
	require.Exactly(t, []byte(`val2-A`), list.Items[3].Item.Value)
	require.Exactly(t, []byte(`val3-A`), list.Items[4].Item.Value)
}

func TestStoreZScanOnZAddIndexReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	i1, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)}}})
	i2, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)}}})
	i3, _ := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)}}})

	zaddOpts1 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(1)},
		AtTx:  int64(i1.Id),
	}

	reference1, err1 := db.ZAdd(zaddOpts1)

	require.NoError(t, err1)
	require.Exactly(t, reference1.Id, uint64(3))
	require.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(2)},
		AtTx:  int64(i2.Id),
	}

	reference2, err2 := db.ZAdd(zaddOpts2)

	require.NoError(t, err2)
	require.Exactly(t, reference2.Id, uint64(4))
	require.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := &schema.ZAddRequest{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(3)},
		AtTx:  int64(i3.Id),
	}

	reference3, err3 := db.ZAdd(zaddOpts3)

	require.NoError(t, err3)
	require.Exactly(t, reference3.Id, uint64(5))
	require.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := &schema.ZScanRequest{
		Set:     []byte(`hashA`),
		Reverse: false,
	}

	itemList1, err := db.ZScan(zscanOpts1)

	require.NoError(t, err)
	require.Len(t, itemList1.Items, 3)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[0].Item.Key)
	require.Equal(t, []byte(`SignerId1`), itemList1.Items[1].Item.Key)
	require.Equal(t, []byte(`SignerId2`), itemList1.Items[2].Item.Key)

}
