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
	"strconv"
	"testing"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/tree"
	"github.com/stretchr/testify/assert"
)

func TestStoreSafeSet(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	root, err := st.CurrentRoot()
	assert.NotNil(t, root)
	assert.NoError(t, err)

	for n := uint64(0); n <= 64; n++ {
		opts := schema.SafeSetOptions{
			Kv: &schema.KeyValue{
				Key:   []byte(strconv.FormatUint(n, 10)),
				Value: []byte(strconv.FormatUint(n, 10)),
			},
			RootIndex: &schema.Index{
				Index: root.Index,
			},
		}
		proof, err := st.SafeSet(opts)
		assert.NoError(t, err, "n=%d", n)
		assert.NotNil(t, proof, "n=%d", n)
		assert.Equal(t, n, proof.Index, "n=%d", n)

		leaf := api.Digest(proof.Index, opts.Kv.Key, opts.Kv.Value)
		verified := proof.Verify(leaf[:], *root)
		assert.True(t, verified, "n=%d", n)

		root.Index = proof.At
		root.Root = proof.Root
	}

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: key})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)
	}

	assert.Equal(t, root64th, tree.Root(st.tree))
}

func BenchmarkStoreSafeSet(b *testing.B) {
	st, closer := makeStore()
	defer closer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		opts := schema.SafeSetOptions{
			Kv: &schema.KeyValue{
				Key:   []byte(strconv.FormatUint(uint64(i), 10)),
				Value: []byte{0, 1, 3, 4, 5, 6, 7},
			},
			RootIndex: &schema.Index{
				Index: uint64(i),
			},
		}
		st.SafeSet(opts)
	}
	b.StopTimer()
}
