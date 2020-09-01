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
	"crypto/sha256"
	"strconv"
	"testing"

	"github.com/codenotary/merkletree"

	"github.com/codenotary/immudb/pkg/api/schema"

	"github.com/stretchr/testify/assert"
)

func TestInclusion(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	var n uint64
	for n = 0; n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
	}

	index := uint64(5)
	at := n - 1

	st.tree.WaitUntil(at)

	proof, err := st.InclusionProof(schema.Index{Index: index})
	leaf := st.tree.Get(0, index)
	assert.NoError(t, err)
	assert.Equal(t, index, proof.Index)
	assert.Equal(t, at, proof.At)
	assert.Equal(t, root64th[:], proof.Root)
	assert.Equal(t, leaf[:], proof.Leaf)
	assert.True(t, proof.Verify(index, leaf[:]))
}

func TestConsistency(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	var root5th [sha256.Size]byte
	testIndex := uint64(5)

	var n uint64
	for n = 0; n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		kv := schema.KeyValue{
			Key:   key,
			Value: key,
		}
		index, err := st.Set(kv)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index.Index, "n=%d", n)
		if index.Index == testIndex {
			st.tree.WaitUntil(testIndex)
			root5th = merkletree.Root(st.tree)
		}
	}

	index := testIndex
	at := n - 1

	st.tree.WaitUntil(at)

	proof, err := st.ConsistencyProof(schema.Index{Index: index})
	assert.NoError(t, err)
	assert.Equal(t, index, proof.First)
	assert.Equal(t, at, proof.Second)
	assert.Equal(t, root64th[:], proof.SecondRoot)
	assert.True(t, proof.Verify(schema.Root{Payload: &schema.RootIndex{Index: index, Root: root5th[:]}}))
	assert.Equal(t, root5th[:], proof.FirstRoot)
}
