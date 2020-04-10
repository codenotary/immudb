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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestStoreReference(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, err := st.Set(schema.KeyValue{Key: []byte(`firstKey`), Value: []byte(`firstValue`)})

	assert.NoError(t, err)

	refOpts := &schema.ReferenceOptions{
		Reference: []byte(`myTag`),
		Key:       []byte(`firstKey`),
	}

	reference, err := st.Reference(refOpts)

	assert.NoError(t, err)
	assert.Exactly(t, reference.Index, uint64(1))
	assert.NotEmptyf(t, reference, "Should not be empty")

	firstItemRet, err := st.Get(schema.Key{Key: []byte(`myTag`)})

	assert.NoError(t, err)
	assert.NotEmptyf(t, firstItemRet, "Should not be empty")
	assert.Equal(t, firstItemRet.Value, []byte(`firstValue`), "Should have referenced item value")
}

func TestStoreReferenceAsyncCommit(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	firstIndex, _ := st.Set(schema.KeyValue{Key: []byte(`firstKey`), Value: []byte(`firstValue`)})
	secondIndex, _ := st.Set(schema.KeyValue{Key: []byte(`secondKey`), Value: []byte(`secondValue`)})

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
		index, err := st.Reference(refOpts, WithAsyncCommit(true))
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n+2, index.Index, "n=%d", n)
	}

	st.Wait()

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		var itemKey []byte
		var itemVal []byte
		var index uint64
		if n%2 == 0 {
			itemKey = []byte(`firstKey`)
			itemVal = []byte(`firstValue`)
			index = firstIndex.Index

		} else {
			itemKey = []byte(`secondKey`)
			itemVal = []byte(`secondValue`)
			index = secondIndex.Index
		}
		item, err := st.Get(schema.Key{Key: tag})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, index, item.Index, "n=%d", n)
		assert.Equal(t, itemVal, item.Value, "n=%d", n)
		assert.Equal(t, itemKey, item.Key, "n=%d", n)
	}
}
