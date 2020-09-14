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

	var zero []byte
	zero = append(zero, byte(0))
	idx, err := st.Set(schema.KeyValue{Key: zero, Value: []byte(`item0`)})
	idx, err = st.Set(schema.KeyValue{Key: []byte(`0`), Value: []byte(`itemZERO`)})
	idx, err = st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	idx, err = st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	idx, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`aaa`), Reference: []byte(`aab`)})
	idx, err = st.Reference(&schema.ReferenceOptions{Key: []byte(`bbb`), Reference: []byte(`abb`)})
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
