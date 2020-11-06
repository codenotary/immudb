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

package store

import (
	"math/rand"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
)

func TestStoreIndexExists(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)})
	st.Set(schema.KeyValue{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)})
	st.Set(schema.KeyValue{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)})

	zaddOpts1 := schema.ZAddOptions{
		Key:   []byte(`myFirstElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(14.6)},
	}

	reference1, err1 := st.ZAdd(zaddOpts1)

	assert.NoError(t, err1)
	assert.Exactly(t, reference1.Index, uint64(3))
	assert.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := schema.ZAddOptions{
		Key:   []byte(`mySecondElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(5)},
	}

	reference2, err2 := st.ZAdd(zaddOpts2)

	assert.NoError(t, err2)
	assert.Exactly(t, reference2.Index, uint64(4))
	assert.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := schema.ZAddOptions{
		Key:   []byte(`myThirdElementKey`),
		Set:   []byte(`firstIndex`),
		Score: &schema.Score{Score: float64(14.5)},
	}

	reference3, err3 := st.ZAdd(zaddOpts3)

	assert.NoError(t, err3)
	assert.Exactly(t, reference3.Index, uint64(5))
	assert.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := schema.ZScanOptions{
		Set:     []byte(`firstIndex`),
		Reverse: false,
	}

	itemList1, err := st.ZScan(zscanOpts1)

	assert.NoError(t, err)
	assert.Len(t, itemList1.Items, 3)
	assert.Equal(t, []byte(`mySecondElementKey`), itemList1.Items[0].Item.Key)
	assert.Equal(t, []byte(`myThirdElementKey`), itemList1.Items[1].Item.Key)
	assert.Equal(t, []byte(`myFirstElementKey`), itemList1.Items[2].Item.Key)

	zscanOpts2 := schema.ZScanOptions{
		Set:     []byte(`firstIndex`),
		Reverse: true,
	}

	itemList2, err := st.ZScan(zscanOpts2)

	assert.NoError(t, err)
	assert.Len(t, itemList2.Items, 3)
	assert.Equal(t, []byte(`myFirstElementKey`), itemList2.Items[0].Item.Key)
	assert.Equal(t, []byte(`myThirdElementKey`), itemList2.Items[1].Item.Key)
	assert.Equal(t, []byte(`mySecondElementKey`), itemList2.Items[2].Item.Key)
}

func TestStoreIndexEqualKeys(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	i1, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)})
	i2, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)})
	i3, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`SignerId1`),
		Index: i1,
	}

	reference1, err1 := st.ZAdd(zaddOpts1)

	assert.NoError(t, err1)
	assert.Exactly(t, reference1.Index, uint64(3))
	assert.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := schema.ZAddOptions{
		Key:   []byte(`SignerId1`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(2)},
		Index: i2,
	}

	reference2, err2 := st.ZAdd(zaddOpts2)

	assert.NoError(t, err2)
	assert.Exactly(t, reference2.Index, uint64(4))
	assert.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := schema.ZAddOptions{
		Key:   []byte(`SignerId2`),
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(3)},
		Index: i3,
	}

	reference3, err3 := st.ZAdd(zaddOpts3)

	assert.NoError(t, err3)
	assert.Exactly(t, reference3.Index, uint64(5))
	assert.NotEmptyf(t, reference3, "Should not be empty")

	zscanOpts1 := schema.ZScanOptions{
		Set:     []byte(`hashA`),
		Reverse: false,
	}

	itemList1, err := st.ZScan(zscanOpts1)

	assert.NoError(t, err)
	assert.Len(t, itemList1.Items, 3)
	assert.Equal(t, []byte(`SignerId1`), itemList1.Items[0].Item.Key)
	assert.Equal(t, []byte(`SignerId1`), itemList1.Items[1].Item.Key)
	assert.Equal(t, []byte(`SignerId2`), itemList1.Items[2].Item.Key)

}

func TestStoreIndexEqualKeysMismatchError(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	i1, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   []byte(`hashA`),
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`WrongKey`),
		Index: i1,
	}

	_, err := st.ZAdd(zaddOpts1)

	assert.Error(t, err)
	assert.Equal(t, err, ErrIndexKeyMismatch)
}

// TestStore_ZScanMinMax
// set1
// key: key1, score: 1
// key: key2, score: 1
// key: key3, score: 2
// key: key4, score: 2
// key: key5, score: 2
// key: key6, score: 3
func TestStore_ZScanPagination(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	setName := []byte(`set1`)
	i1, _ := st.Set(schema.KeyValue{Key: []byte(`key1`), Value: []byte(`val1`)})
	i2, _ := st.Set(schema.KeyValue{Key: []byte(`key2`), Value: []byte(`val2`)})
	i3, _ := st.Set(schema.KeyValue{Key: []byte(`key3`), Value: []byte(`val3`)})
	i4, _ := st.Set(schema.KeyValue{Key: []byte(`key4`), Value: []byte(`val4`)})
	i5, _ := st.Set(schema.KeyValue{Key: []byte(`key5`), Value: []byte(`val5`)})
	i6, _ := st.Set(schema.KeyValue{Key: []byte(`key6`), Value: []byte(`val6`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key1`),
		Index: i1,
	}
	zaddOpts2 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(1)},
		Key:   []byte(`key2`),
		Index: i2,
	}
	zaddOpts3 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key3`),
		Index: i3,
	}
	zaddOpts4 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key4`),
		Index: i4,
	}
	zaddOpts5 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(2)},
		Key:   []byte(`key5`),
		Index: i5,
	}
	zaddOpts6 := schema.ZAddOptions{
		Set:   setName,
		Score: &schema.Score{Score: float64(3)},
		Key:   []byte(`key6`),
		Index: i6,
	}

	st.ZAdd(zaddOpts1)
	st.ZAdd(zaddOpts2)
	st.ZAdd(zaddOpts3)
	st.ZAdd(zaddOpts4)
	st.ZAdd(zaddOpts5)
	st.ZAdd(zaddOpts6)

	zScanOption1 := schema.ZScanOptions{
		Set:     setName,
		Offset:  nil,
		Limit:   2,
		Reverse: false,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list1, err := st.ZScan(zScanOption1)
	assert.NoError(t, err)
	assert.Len(t, list1.Items, 2)
	assert.Equal(t, list1.Items[0].Item.Key, []byte(`key3`))
	assert.Equal(t, list1.Items[1].Item.Key, []byte(`key4`))

	zScanOption2 := schema.ZScanOptions{
		Set:     setName,
		Offset:  list1.Items[len(list1.Items)-1].CurrentOffset,
		Limit:   2,
		Reverse: false,
		Min:     &schema.Score{Score: 2},
		Max:     &schema.Score{Score: 3},
	}

	list, err := st.ZScan(zScanOption2)
	assert.NoError(t, err)
	assert.Len(t, list.Items, 2)
	assert.Equal(t, list.Items[0].Item.Key, []byte(`key5`))
	assert.Equal(t, list.Items[1].Item.Key, []byte(`key6`))
}

func TestFloat(t *testing.T) {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	for i := 0; i < 100; i++ {
		n := r.Float64()
		bs := Float642bytes(n)
		assert.NotNil(t, bs)
		assert.True(t, len(bs) == 8)
		assert.Equal(t, n, Bytes2float(bs))
	}

}
