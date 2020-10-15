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
		Score: float64(14.6),
	}

	reference1, err1 := st.ZAdd(zaddOpts1)

	assert.NoError(t, err1)
	assert.Exactly(t, reference1.Index, uint64(3))
	assert.NotEmptyf(t, reference1, "Should not be empty")

	zaddOpts2 := schema.ZAddOptions{
		Key:   []byte(`mySecondElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(5),
	}

	reference2, err2 := st.ZAdd(zaddOpts2)

	assert.NoError(t, err2)
	assert.Exactly(t, reference2.Index, uint64(4))
	assert.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := schema.ZAddOptions{
		Key:   []byte(`myThirdElementKey`),
		Set:   []byte(`firstIndex`),
		Score: float64(14.5),
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
	assert.Equal(t, []byte(`mySecondElementKey`), itemList1.Items[0].Key)
	assert.Equal(t, []byte(`myThirdElementKey`), itemList1.Items[1].Key)
	assert.Equal(t, []byte(`myFirstElementKey`), itemList1.Items[2].Key)

	zscanOpts2 := schema.ZScanOptions{
		Set:     []byte(`firstIndex`),
		Reverse: true,
	}

	itemList2, err := st.ZScan(zscanOpts2)

	assert.NoError(t, err)
	assert.Len(t, itemList2.Items, 3)
	assert.Equal(t, []byte(`myFirstElementKey`), itemList2.Items[0].Key)
	assert.Equal(t, []byte(`myThirdElementKey`), itemList2.Items[1].Key)
	assert.Equal(t, []byte(`mySecondElementKey`), itemList2.Items[2].Key)
}

func TestStoreIndexEqualKeys(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	i1, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId1`), Value: []byte(`firstValue`)})
	i2, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId1`), Value: []byte(`secondValue`)})
	i3, _ := st.Set(schema.KeyValue{Key: []byte(`SignerId2`), Value: []byte(`thirdValue`)})

	zaddOpts1 := schema.ZAddOptions{
		Set:   []byte(`hashA`),
		Score: float64(1),
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
		Score: float64(2),
		Index: i2,
	}

	reference2, err2 := st.ZAdd(zaddOpts2)

	assert.NoError(t, err2)
	assert.Exactly(t, reference2.Index, uint64(4))
	assert.NotEmptyf(t, reference2, "Should not be empty")

	zaddOpts3 := schema.ZAddOptions{
		Key:   []byte(`hashA.SignerId2`),
		Set:   []byte(`hashA`),
		Score: float64(3),
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
	assert.Equal(t, []byte(`SignerId1`), itemList1.Items[0].Key)
	assert.Equal(t, []byte(`SignerId1`), itemList1.Items[1].Key)
	assert.Equal(t, []byte(`SignerId2`), itemList1.Items[2].Key)

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
