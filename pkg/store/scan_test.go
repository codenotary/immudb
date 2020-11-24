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
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Set(schema.KeyValue{Key: []byte(`abc`), Value: []byte(`item3`)})

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  nil,
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 2, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))
	assert.Equal(t, list.Items[1].Key, []byte(`abc`))
	assert.Equal(t, list.Items[1].Value, []byte(`item3`))

	scanOptions1 := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  nil,
		Limit:   0,
		Reverse: true,
		Deep:    false,
	}

	list1, err1 := st.Scan(scanOptions1)
	assert.NoError(t, err1)
	assert.Exactly(t, 2, len(list1.Items))
	assert.Equal(t, list1.Items[0].Key, []byte(`abc`))
	assert.Equal(t, list1.Items[0].Value, []byte(`item3`))
	assert.Equal(t, list1.Items[1].Key, []byte(`aaa`))
	assert.Equal(t, list1.Items[1].Value, []byte(`item1`))
}

func TestStoreScanOffsetted(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Set(schema.KeyValue{Key: []byte(`bcc`), Value: []byte(`item3`)})

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(``),
		Offset:  []byte(``),
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 3, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))

	scanOptions = schema.ScanOptions{
		Prefix:  []byte(``),
		Offset:  []byte(`bbb`),
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err = st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`bcc`))
	assert.Equal(t, list.Items[0].Value, []byte(`item3`))

	scanOptions = schema.ScanOptions{
		Prefix:  []byte(`b`),
		Offset:  []byte(`bbb`),
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err = st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`bcc`))
	assert.Equal(t, list.Items[0].Value, []byte(`item3`))

	scanOptions = schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  []byte(`a`),
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err = st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 0, len(list.Items))
}

func TestStoreScanReverseOffsetted(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Set(schema.KeyValue{Key: []byte(`bcc`), Value: []byte(`item3`)})

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(``),
		Offset:  []byte(``),
		Limit:   0,
		Reverse: true,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 3, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`bcc`))
	assert.Equal(t, list.Items[0].Value, []byte(`item3`))

	scanOptions = schema.ScanOptions{
		Prefix:  []byte(``),
		Offset:  []byte(`bbb`),
		Limit:   0,
		Reverse: true,
		Deep:    false,
	}

	list, err = st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))

	scanOptions = schema.ScanOptions{
		Prefix:  []byte(`b`),
		Offset:  []byte(`b`),
		Limit:   0,
		Reverse: true,
		Deep:    false,
	}

	list, err = st.Scan(scanOptions)
	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`bbb`))
	assert.Equal(t, list.Items[0].Value, []byte(`item2`))
}

func TestStoreReferenceScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Reference(&schema.ReferenceOptions{Key: []byte(`aab`), Reference: []byte(`aaa`)})
	st.Reference(&schema.ReferenceOptions{Key: []byte(`abb`), Reference: []byte(`bbb`)})

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  nil,
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))
}

func TestStoreReferenceDeepScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`aab`)})
	st.Reference(&schema.ReferenceOptions{Key: []byte(`bbb`), Reference: []byte(`abb`)})

	deepScanOptions := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  nil,
		Limit:   0,
		Reverse: false,
		Deep:    true,
	}

	list, err := st.Scan(deepScanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 3, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))
	assert.Equal(t, list.Items[1].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[1].Value, []byte(`item1`))
	assert.Equal(t, list.Items[2].Key, []byte(`bbb`))
	assert.Equal(t, list.Items[2].Value, []byte(`item2`))
}

func TestIScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	idx, err := st.Set(schema.KeyValue{Key: []byte(`0`), Value: []byte(`itemZERO`)})
	require.NoError(t, err)
	idx, err = st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	require.NoError(t, err)
	idx, err = st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	require.NoError(t, err)
	idx, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`aab`)})
	require.NoError(t, err)
	idx, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`bbb`), Reference: []byte(`abb`)})
	require.NoError(t, err)
	idx, err = st.Set(schema.KeyValue{Key: []byte(`zzz`), Value: []byte(`itemzzz`)})
	require.NoError(t, err)
	require.IsType(t, uint64(0), idx.Index)

	st.tree.WaitUntil(5)

	deepScanOptions := schema.IScanOptions{
		PageSize:   3,
		PageNumber: 1,
	}

	page, err := st.IScan(deepScanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 3, len(page.Items))
	assert.Equal(t, true, page.More)
	assert.Equal(t, uint64(0), page.Items[0].Index)
	assert.Equal(t, uint64(1), page.Items[1].Index)
	assert.Equal(t, uint64(2), page.Items[2].Index)

	deepScanOptions1 := schema.IScanOptions{
		PageSize:   3,
		PageNumber: 2,
	}

	page1, err1 := st.IScan(deepScanOptions1)

	assert.NoError(t, err1)
	assert.Exactly(t, 3, len(page1.Items))
	assert.Equal(t, false, page1.More)
	assert.Equal(t, uint64(3), page1.Items[0].Index)
	assert.Equal(t, uint64(4), page1.Items[1].Index)
	assert.Equal(t, uint64(5), page1.Items[2].Index)

	deepScanOptions2 := schema.IScanOptions{
		PageSize:   3,
		PageNumber: 3,
	}

	_, err2 := st.IScan(deepScanOptions2)
	assert.Error(t, ErrIndexNotFound, err2)
}

func TestStoreScanPagination(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Set(schema.KeyValue{Key: []byte(`abc`), Value: []byte(`item3`)})
	st.Set(schema.KeyValue{Key: []byte(`acc`), Value: []byte(`item4`)})
	st.Set(schema.KeyValue{Key: []byte(`acd`), Value: []byte(`item5`)})

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  []byte(`ab`),
		Limit:   1,
		Reverse: false,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 1, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`acc`))
	assert.Equal(t, list.Items[0].Value, []byte(`item4`))

}

func TestStoreScanPagination2(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Set(schema.KeyValue{Key: []byte(`abc`), Value: []byte(`item3`)})
	st.Set(schema.KeyValue{Key: []byte(`acc`), Value: []byte(`item4`)})
	st.Set(schema.KeyValue{Key: []byte(`acd`), Value: []byte(`item5`)})
	st.Set(schema.KeyValue{Key: []byte(`ace`), Value: []byte(`item6`)})
	st.Set(schema.KeyValue{Key: []byte(`acf`), Value: []byte(`item7`)})
	st.Set(schema.KeyValue{Key: []byte(`acg`), Value: []byte(`item8`)})
	st.Set(schema.KeyValue{Key: []byte(`ach`), Value: []byte(`item9`)})
	st.Set(schema.KeyValue{Key: []byte(`aci`), Value: []byte(`item10`)})

	scanOptionsP1 := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  []byte(`ab`),
		Limit:   2,
		Reverse: false,
		Deep:    false,
	}

	list1, err := st.Scan(scanOptionsP1)

	assert.NoError(t, err)
	assert.Exactly(t, 2, len(list1.Items))
	assert.Equal(t, list1.Items[0].Key, []byte(`acc`))
	assert.Equal(t, list1.Items[0].Value, []byte(`item4`))
	assert.Equal(t, list1.Items[1].Key, []byte(`acd`))
	assert.Equal(t, list1.Items[1].Value, []byte(`item5`))

	scanOptionsP2 := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  list1.Items[len(list1.Items)-1].Key,
		Limit:   2,
		Reverse: false,
		Deep:    false,
	}

	list2, err := st.Scan(scanOptionsP2)

	assert.NoError(t, err)
	assert.Exactly(t, 2, len(list2.Items))
	assert.Equal(t, list2.Items[0].Key, []byte(`ace`))
	assert.Equal(t, list2.Items[0].Value, []byte(`item6`))
	assert.Equal(t, list2.Items[1].Key, []byte(`acf`))
	assert.Equal(t, list2.Items[1].Value, []byte(`item7`))

	scanOptionsP3 := schema.ScanOptions{
		Prefix:  []byte(`a`),
		Offset:  list2.Items[len(list2.Items)-1].Key,
		Limit:   3,
		Reverse: false,
		Deep:    false,
	}

	list3, err := st.Scan(scanOptionsP3)

	assert.NoError(t, err)
	assert.Exactly(t, 3, len(list3.Items))
	assert.Equal(t, list3.Items[0].Key, []byte(`acg`))
	assert.Equal(t, list3.Items[0].Value, []byte(`item8`))
	assert.Equal(t, list3.Items[1].Key, []byte(`ach`))
	assert.Equal(t, list3.Items[1].Value, []byte(`item9`))
	assert.Equal(t, list3.Items[2].Key, []byte(`aci`))
	assert.Equal(t, list3.Items[2].Value, []byte(`item10`))
}

func TestStore_ScanInvalidKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	opt := schema.ScanOptions{
		Prefix: []byte{tsPrefix},
	}
	_, err := st.Scan(opt)
	assert.Error(t, err, ErrInvalidKey)
}

func TestStore_ScanInvalidOffset(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	opt := schema.ScanOptions{
		Prefix: []byte(`prefix`),
		Offset: []byte{tsPrefix},
	}
	_, err := st.Scan(opt)
	assert.Error(t, err, ErrInvalidOffset)
}

func TestStoreIndexReferenceDeepScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	idx1, _ := st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item2`)})
	_, err := st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag1`), Index: idx1})
	assert.NoError(t, err)
	_, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag2`), Index: idx2})
	assert.NoError(t, err)

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(`myTag`),
		Offset:  nil,
		Limit:   0,
		Reverse: false,
		Deep:    true,
	}

	list, err := st.Scan(scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 2, len(list.Items))
	assert.Equal(t, list.Items[0].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[0].Value, []byte(`item1`))
	assert.Equal(t, list.Items[1].Key, []byte(`aaa`))
	assert.Equal(t, list.Items[1].Value, []byte(`item2`))
}

func TestStoreIndexReferenceScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	idx1, _ := st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx2, _ := st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item2`)})
	_, err := st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag1`), Index: idx1})
	assert.NoError(t, err)
	_, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`myTag2`), Index: idx2})
	assert.NoError(t, err)

	scanOptions := schema.ScanOptions{
		Prefix:  []byte(`myTag`),
		Offset:  nil,
		Limit:   0,
		Reverse: false,
		Deep:    false,
	}

	list, err := st.Scan(scanOptions)

	assert.NoError(t, err)
	assert.Exactly(t, 0, len(list.Items))
}
