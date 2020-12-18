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

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func TestStoreReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	meta, err := db.Set(req)

	item, err := db.Get(&schema.KeyRequest{Key: []byte(`firstKey`), SinceTx: meta.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstKey`), item.Key)
	require.Equal(t, []byte(`firstValue`), item.Value)

	refOpts := &schema.ReferenceRequest{
		Reference: []byte(`myTag`),
		Key:       []byte(`firstKey`),
		SinceTx:   meta.Id,
	}
	meta, err = db.SetReference(refOpts)
	require.NoError(t, err)

	require.Equal(t, uint64(2), meta.Id)

	firstItemRet, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag`), SinceTx: meta.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`firstValue`), firstItemRet.Value, "Should have referenced item value")
}

func TestStore_GetReferenceWithIndexResolution(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	set, err := db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value1`)}}})
	require.NoError(t, err)

	_, err = db.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`aaa`), Value: []byte(`value2`)}}})
	require.NoError(t, err)

	ref, err := db.SetReference(&schema.ReferenceRequest{Reference: []byte(`myTag1`), Key: []byte(`aaa`), AtTx: set.Id})
	require.NoError(t, err)

	tag3, err := db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref.Id})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag3.Key)
	require.Equal(t, []byte(`value1`), tag3.Value)
}

func TestStoreCyclicReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	req := &schema.SetRequest{KVs: []*schema.KeyValue{{Key: []byte(`firstKey`), Value: []byte(`firstValue`)}}}
	meta, err := db.Set(req)

	ref1, err := db.SetReference(&schema.ReferenceRequest{Reference: []byte(`myTag1`), Key: []byte(`firstKey`), AtTx: meta.Id})
	require.NoError(t, err)

	_, err = db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref1.Id})
	require.NoError(t, err)

	ref2, err := db.SetReference(&schema.ReferenceRequest{Reference: []byte(`myTag2`), Key: []byte(`myTag1`)})
	require.NoError(t, err)

	ref11, err := db.SetReference(&schema.ReferenceRequest{Reference: []byte(`myTag1`), Key: []byte(`myTag2`), AtTx: ref2.Id})
	require.NoError(t, err)

	_, err = db.Get(&schema.KeyRequest{Key: []byte(`myTag1`), SinceTx: ref11.Id})
	require.Error(t, ErrMaxKeyResolutionLimitReached, err)
}

/*
func TestStoreReferenceAsyncCommit(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	firstIndex, _ := db.Set(&schema.KeyValue{Key: []byte(`firstKey`), Value: []byte(`firstValue`)})
	secondIndex, _ := db.Set(&schema.KeyValue{Key: []byte(`secondKey`), Value: []byte(`secondValue`)})

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		var itemKey []byte
		if n%2 == 0 {
			itemKey = []byte(`firstKey`)
		} else {
			itemKey = []byte(`secondKey`)
		}
		refOpts := &schema.ReferenceOptions{
			Reference: tag,
			Key:       itemKey,
		}
		ref, err := db.Reference(refOpts)
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, n+1+2, ref.GetIndex(), "n=%d", n)
	}

	db.waitForIndexing(64 + 2)

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		var itemKey []byte
		var itemVal []byte
		var index uint64
		if n%2 == 0 {
			itemKey = []byte(`firstKey`)
			itemVal = []byte(`firstValue`)
			index = firstIndex.GetIndex()
		} else {
			itemKey = []byte(`secondKey`)
			itemVal = []byte(`secondValue`)
			index = secondIndex.GetIndex()
		}
		item, err := db.Get(&schema.Key{Key: tag})
		require.NoError(t, err, "n=%d", n)
		require.Equal(t, index, item.Index, "n=%d", n)
		require.Equal(t, itemVal, item.Value, "n=%d", n)
		require.Equal(t, itemKey, item.Key, "n=%d", n)
	}
}

func TestStoreMultipleReferenceOnSameKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx0, err := db.Set(&schema.KeyValue{Key: []byte(`firstKey`), Value: []byte(`firstValue`)})
	idx1, err := db.Set(&schema.KeyValue{Key: []byte(`firstKey`), Value: []byte(`secondValue`)})

	require.NoError(t, err)

	refOpts1 := &schema.ReferenceOptions{
		Reference: []byte(`myTag1`),
		Key:       []byte(`firstKey`),
		Index:     &schema.Index{Index: idx0.GetIndex()},
	}
	db.waitForIndexing(2)
	reference1, err := db.Reference(refOpts1)
	require.NoError(t, err)
	require.Exactly(t, uint64(3), reference1.GetIndex())
	require.NotEmptyf(t, reference1, "Should not be empty")

	refOpts2 := &schema.ReferenceOptions{
		Reference: []byte(`myTag2`),
		Key:       []byte(`firstKey`),
		Index:     &schema.Index{Index: idx0.GetIndex()},
	}
	reference2, err := db.Reference(refOpts2)
	db.waitForIndexing(3)
	require.NoError(t, err)
	require.Exactly(t, uint64(4), reference2.GetIndex())
	require.NotEmptyf(t, reference2, "Should not be empty")

	refOpts3 := &schema.ReferenceOptions{
		Reference: []byte(`myTag3`),
		Key:       []byte(`firstKey`),
		Index:     &schema.Index{Index: idx1.GetIndex()},
	}
	reference3, err := db.Reference(refOpts3)
	db.waitForIndexing(4)
	require.NoError(t, err)
	require.Exactly(t, uint64(5), reference3.GetIndex())
	require.NotEmptyf(t, reference3, "Should not be empty")

	firstTagRet, err := db.GetReference(&schema.Key{Key: []byte(`myTag1`)})

	require.NoError(t, err)
	require.NotEmptyf(t, firstTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), firstTagRet.Value, "Should have referenced item value")

	secondTagRet, err := db.GetReference(&schema.Key{Key: []byte(`myTag2`)})

	require.NoError(t, err)
	require.NotEmptyf(t, secondTagRet, "Should not be empty")
	require.Equal(t, []byte(`firstValue`), secondTagRet.Value, "Should have referenced item value")

	thirdItemRet, err := db.Get(&schema.Key{Key: []byte(`myTag3`)})

	require.NoError(t, err)
	require.NotEmptyf(t, thirdItemRet, "Should not be empty")
	require.Equal(t, []byte(`secondValue`), thirdItemRet.Value, "Should have referenced item value")
}

func TestStoreIndexReference(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item2`)})

	db.waitForIndexing(2)

	db.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag1`), Index: &schema.Index{Index: idx1.GetIndex()}})
	db.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag2`), Index: &schema.Index{Index: idx2.GetIndex()}})

	db.waitForIndexing(4)

	tag1, err := db.GetReference(&schema.Key{Key: []byte(`myTag1`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1.Key)
	require.Equal(t, []byte(`item1`), tag1.Value)

	tag2, err := db.Get(&schema.Key{Key: []byte(`myTag2`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item2`), tag2.Value)

}

func TestStoreReferenceKeyNotProvided(t *testing.T) {
	db, closer := makeDb()
	defer closer()
	_, err := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag1`), Index: &schema.Index{Index: 123}})
	require.Equal(t, err, ErrReferenceKeyMissing)
}

func TestStore_GetOnReferenceOnSameKeyReturnsAlwaysLastValue(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item2`)})

	db.waitForIndexing(idx2.GetIndex())

	ref, _ := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag1`), Key: []byte(`aaa`), Index: &schema.Index{Index: idx1.GetIndex()}})

	db.waitForIndexing(ref.GetIndex())

	ref2, _ := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag2`), Key: []byte(`aaa`), Index: &schema.Index{Index: idx2.GetIndex()}})

	db.waitForIndexing(ref2.GetIndex())

	tag2, err := db.Get(&schema.Key{Key: []byte(`myTag2`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item2`), tag2.Value)

	tag1b, err := db.Get(&schema.Key{Key: []byte(`myTag1`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1b.Key)
	require.Equal(t, []byte(`item2`), tag1b.Value)
}

func TestStore_GetOnReferenceOnSameKeyMixReturnsAlwaysLastValue(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item2`)})
	idx3, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item3`)})

	db.waitForIndexing(idx3.GetIndex())

	ref1, _ := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag1`), Key: []byte(`aaa`), Index: &schema.Index{Index: idx1.GetIndex()}})
	db.waitForIndexing(ref1.GetIndex())
	ref2, _ := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag2`), Key: []byte(`aaa`), Index: &schema.Index{Index: idx2.GetIndex()}})
	db.waitForIndexing(ref2.GetIndex())
	ref3, _ := db.Reference(&schema.ReferenceOptions{Reference: []byte(`myTag3`), Key: []byte(`aaa`)})
	db.waitForIndexing(ref3.GetIndex())

	tag2, err := db.Get(&schema.Key{Key: []byte(`myTag2`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag2.Key)
	require.Equal(t, []byte(`item3`), tag2.Value)

	tag1, err := db.Get(&schema.Key{Key: []byte(`myTag1`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag1.Key)
	require.Equal(t, []byte(`item3`), tag1.Value)

	tag3, err := db.Get(&schema.Key{Key: []byte(`myTag3`)})
	require.NoError(t, err)
	require.Equal(t, []byte(`aaa`), tag3.Key)
	require.Equal(t, []byte(`item3`), tag3.Value)
}

func TestStore_ReferenceIllegalArgument(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Reference(nil)
	require.Equal(t, err, store.ErrIllegalArguments)
}

func TestStore_ReferencedItemNotFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`notExists`)})
	require.Equal(t, err, errors.New("unexpected error key not found during Reference"))
}

func TestStore_GetReferenceIllegalArgument(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.GetReference(nil)
	require.Equal(t, err, store.ErrIllegalArguments)
}
func TestStore_GetReferencedItemNotFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.GetReference(&schema.Key{Key: []byte(`aaa`)})
	require.Equal(t, err, errors.New("key not found"))
}

func TestStore_GetReferencedNoReferenceProvided(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})

	db.waitForIndexing(idx1.GetIndex())

	_, err := db.GetReference(&schema.Key{Key: []byte(`aaa`)})
	require.Equal(t, err, ErrNoReferenceProvided)
}

func TestStore_getReferenceValErrIndexKeyMismatch(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, _ = db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := db.Set(&schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})

	db.waitForIndexing(idx2.GetIndex())

	_, err := db.getReferenceVal(&schema.ReferenceOptions{Key: []byte(`bbb`), Index: &schema.Index{Index: 1}, Reference: []byte(`myTag`)}, false)

	require.Equal(t, err, ErrIndexKeyMismatch)
}

func TestStore_getReferenceValEOF(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx1, _ := db.Set(&schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})

	db.waitForIndexing(idx1.GetIndex())

	_, err := db.getReferenceVal(&schema.ReferenceOptions{Key: []byte(`aaa`), Index: &schema.Index{Index: 99999999}, Reference: []byte(`myTag`)}, false)

	require.Equal(t, err, errors.New("EOF"))
}*/
