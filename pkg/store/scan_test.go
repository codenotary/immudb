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
	"testing"
)

func TestStoreReferenceScan(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`aaa`), Value: []byte(`item1`)})
	st.Set(schema.KeyValue{Key: []byte(`bbb`), Value: []byte(`item2`)})
	st.Reference(&schema.ReferenceOptions{Key: &schema.Key{ Key: []byte(`aab`)}, Reference: &schema.Key{ Key: []byte(`aaa`)}})
	st.Reference(&schema.ReferenceOptions{Key: &schema.Key{ Key: []byte(`abb`)}, Reference: &schema.Key{ Key: []byte(`bbb`)}})

	scanOptions := schema.ScanOptions{
		Prefix:               []byte(`a`),
		Offset:               nil,
		Limit:                0,
		Reverse:              false,
		Deep:                 false,
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
	st.Reference(&schema.ReferenceOptions{Key: &schema.Key{ Key: []byte(`aaa`)}, Reference: &schema.Key{ Key: []byte(`aab`)}})
	st.Reference(&schema.ReferenceOptions{Key: &schema.Key{ Key: []byte(`bbb`)}, Reference: &schema.Key{ Key: []byte(`abb`)}})

	deepScanOptions := schema.ScanOptions{
		Prefix:               []byte(`a`),
		Offset:               nil,
		Limit:                0,
		Reverse:              false,
		Deep:                 true,
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
