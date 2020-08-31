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
	"sync"
	"testing"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/merkletree"
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
				Index: root.Payload.Index,
			},
		}
		proof, err := st.SafeSet(opts)
		assert.NoError(t, err, "n=%d", n)
		assert.NotNil(t, proof, "n=%d", n)
		assert.Equal(t, n, proof.Index, "n=%d", n)

		leaf := api.Digest(proof.Index, opts.Kv.Key, opts.Kv.Value)
		verified := proof.Verify(leaf[:], *root)
		assert.True(t, verified, "n=%d", n)

		root.Payload.Index = proof.At
		root.Payload.Root = proof.Root
	}

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: key})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, item.Index, "n=%d", n)
		assert.Equal(t, key, item.Value, "n=%d", n)
		assert.Equal(t, key, item.Key, "n=%d", n)
	}

	assert.Equal(t, root64th, merkletree.Root(st.tree))
}

func TestStoreMultithreadSafeSetWithKeyOverlap(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(tid int) {
			root, err := st.CurrentRoot()
			assert.NotNil(t, root)
			assert.NoError(t, err)

			for n := uint64(0); n < 256; n++ {
				opts := schema.SafeSetOptions{
					Kv: &schema.KeyValue{
						Key:   []byte(strconv.FormatUint(n, 10)),
						Value: []byte(strconv.FormatUint(uint64(tid), 10)),
					},
					RootIndex: &schema.Index{
						Index: root.Payload.Index,
					},
				}
				proof, err := st.SafeSet(opts)
				assert.NoError(t, err, "n=%d", n)
				assert.NotNil(t, proof, "n=%d", n)

				leaf := api.Digest(proof.Index, opts.Kv.Key, opts.Kv.Value)
				verified := proof.Verify(leaf[:], *root)
				assert.True(t, verified, "n=%d", n)

				root = &schema.Root{
					Payload: &schema.RootIndex{
						Index: proof.At,
						Root:  proof.Root,
					},
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestStoreMultithreadSafeSetWithoutKeyOverlap(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(tid int) {
			root, err := st.CurrentRoot()
			assert.NotNil(t, root)
			assert.NoError(t, err)

			for n := uint64(0); n < 256; n++ {
				opts := schema.SafeSetOptions{
					Kv: &schema.KeyValue{
						Key:   []byte(strconv.FormatUint(uint64(tid<<10)+n, 10)),
						Value: []byte(strconv.FormatUint(uint64(tid), 10)),
					},
					RootIndex: &schema.Index{
						Index: root.Payload.Index,
					},
				}

				proof, err := st.SafeSet(opts)
				assert.NoError(t, err, "n=%d", n)
				assert.NotNil(t, proof, "n=%d", n)

				leaf := api.Digest(proof.Index, opts.Kv.Key, opts.Kv.Value)
				verified := proof.Verify(leaf[:], *root)
				assert.True(t, verified, "n=%d", n)

				root = &schema.Root{
					Payload: &schema.RootIndex{
						Index: proof.At,
						Root:  proof.Root,
					},
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}

func TestStoreSafeGet(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	firstItem, err := st.Set(schema.KeyValue{Key: []byte(`first`), Value: []byte(`firstValue`)})
	assert.NoError(t, err)
	seconditem, err := st.Set(schema.KeyValue{Key: []byte(`second`), Value: []byte(`secondValue`)})
	assert.NoError(t, err)

	// first item, no prev root
	safeItem, err := st.SafeGet(schema.SafeGetOptions{
		Key: []byte(`first`),
		// no root index provided
	})
	assert.NoError(t, err)
	assert.NotNil(t, safeItem)
	assert.Equal(t, []byte(`first`), safeItem.Item.Key)
	assert.Equal(t, []byte(`firstValue`), safeItem.Item.Value)
	assert.Equal(t, firstItem.Index, safeItem.Item.Index)
	assert.True(t, safeItem.Proof.Verify(
		safeItem.Item.Hash(),
		schema.Root{Payload: &schema.RootIndex{}}, // zerovalue signals no prev root
	))

	// second item with prev root
	prevRoot := safeItem.Proof.NewRoot()
	safeItem, err = st.SafeGet(schema.SafeGetOptions{
		Key: []byte(`second`),
		RootIndex: &schema.Index{
			Index: prevRoot.Payload.Index,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, safeItem)
	assert.Equal(t, []byte(`second`), safeItem.Item.Key)
	assert.Equal(t, []byte(`secondValue`), safeItem.Item.Value)
	assert.Equal(t, seconditem.Index, safeItem.Item.Index)
	assert.True(t, safeItem.Proof.Verify(
		safeItem.Item.Hash(),
		*prevRoot,
	))

	lastRoot, err := st.CurrentRoot()
	assert.NoError(t, err)
	assert.NotNil(t, lastRoot)
	assert.Equal(t, *lastRoot.Payload, *safeItem.Proof.NewRoot().Payload)

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

func TestStoreSafeReference(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	root, _ := st.CurrentRoot()

	firstKey := []byte(`firstKey`)
	firstValue := []byte(`firstValue`)

	firstIndex, _ := st.Set(schema.KeyValue{Key: firstKey, Value: firstValue})

	for n := uint64(0); n <= 64; n++ {
		opts := schema.SafeReferenceOptions{
			Ro: &schema.ReferenceOptions{
				Reference: []byte(strconv.FormatUint(n, 10)),
				Key:       firstKey,
			},
			RootIndex: &schema.Index{
				Index: root.Payload.Index,
			},
		}
		proof, err := st.SafeReference(opts)
		assert.NoError(t, err, "n=%d", n)
		assert.NotNil(t, proof, "n=%d", n)
		assert.Equal(t, n+1, proof.Index, "n=%d", n)

		leaf := api.Digest(proof.Index, opts.Ro.Reference, opts.Ro.Key)
		verified := proof.Verify(leaf[:], *root)
		assert.True(t, verified, "n=%d", n)

		root.Payload.Index = proof.At
		root.Payload.Root = proof.Root
	}

	for n := uint64(0); n <= 64; n++ {
		tag := []byte(strconv.FormatUint(n, 10))
		item, err := st.Get(schema.Key{Key: tag})
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, firstIndex.Index, item.Index, "n=%d", n)
		assert.Equal(t, firstValue, item.Value, "n=%d", n)
		assert.Equal(t, firstKey, item.Key, "n=%d", n)
	}
}

func TestStoreSafeGetOnSafeReference(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	firstKey := []byte(`firstKey`)
	firstValue := []byte(`firstValue`)
	firstTag := []byte(`firstTag`)
	secondTag := []byte(`secondTag`)

	firstItem, err := st.Set(schema.KeyValue{Key: firstKey, Value: firstValue})
	assert.NoError(t, err)

	// first item, no prev root
	ref1 := schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: firstTag,
			Key:       firstKey,
		},
	}

	proof, err := st.SafeReference(ref1)
	assert.NoError(t, err)

	leaf := api.Digest(proof.Index, firstTag, firstKey)
	// Here verify if first reference was correctly inserted. We have no root yet.
	verified := proof.Verify(leaf[:], schema.Root{Payload: &schema.RootIndex{}})
	assert.True(t, verified)

	ref2 := schema.SafeReferenceOptions{
		Ro: &schema.ReferenceOptions{
			Reference: secondTag,
			Key:       firstKey,
		},
		RootIndex: &schema.Index{
			Index: proof.Index,
		},
	}

	proof2, err := st.SafeReference(ref2)
	assert.NoError(t, err)

	prevRoot := proof.NewRoot()
	leaf2 := api.Digest(proof2.Index, secondTag, firstKey)
	// Here verify if second reference was correctly inserted. We have root from safeReference 2.
	verified2 := proof2.Verify(leaf2[:], *prevRoot)
	assert.True(t, verified2)

	// first item by first tag , no prev root
	firstItem1, err := st.SafeGet(schema.SafeGetOptions{
		Key: firstTag,
		RootIndex: &schema.Index{
			Index: proof2.Index,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, firstItem1)
	assert.Equal(t, firstKey, firstItem1.Item.Key)
	assert.Equal(t, firstValue, firstItem1.Item.Value)
	assert.Equal(t, firstItem.Index, firstItem1.Item.Index)
	// here verify if the tree in witch the referenced item was inserted is correct
	assert.True(t, firstItem1.Proof.Verify(
		firstItem1.Item.Hash(),
		*proof2.NewRoot(),
	))

	// get first item by second tag with most fresh root
	firstItem2, err := st.SafeGet(schema.SafeGetOptions{
		Key: secondTag,
		RootIndex: &schema.Index{
			Index: proof2.Index,
		},
	})

	assert.NoError(t, err)
	assert.NotNil(t, firstItem2)
	assert.Equal(t, firstKey, firstItem2.Item.Key)
	assert.Equal(t, firstValue, firstItem2.Item.Value)
	assert.Equal(t, firstItem.Index, firstItem2.Item.Index)
	assert.True(t, firstItem2.Proof.Verify(
		firstItem2.Item.Hash(),
		*proof2.NewRoot(),
	))

	lastRoot, err := st.CurrentRoot()
	assert.NoError(t, err)
	assert.NotNil(t, lastRoot)
	assert.Equal(t, *lastRoot.Payload, *firstItem2.Proof.NewRoot().Payload)
}

func TestStoreSafeZAdd(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)})
	st.Set(schema.KeyValue{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)})
	st.Set(schema.KeyValue{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)})

	safeZAddOptions1 := schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   []byte(`FirstSet`),
			Score: float64(43),
			Key:   []byte(`mySecondElementKey`),
		},
	}

	proof1, err := st.SafeZAdd(safeZAddOptions1)

	assert.NoError(t, err)
	assert.NotNil(t, proof1)
	assert.Equal(t, uint64(3), proof1.Index)

	key, _ := SetKey(safeZAddOptions1.Zopts.Key, safeZAddOptions1.Zopts.Set, safeZAddOptions1.Zopts.Score)

	leaf := api.Digest(proof1.Index, key, safeZAddOptions1.Zopts.Key)
	// Here verify if first reference was correctly inserted. We have no root yet.
	verified := proof1.Verify(leaf[:], schema.Root{Payload: &schema.RootIndex{}})
	assert.True(t, verified)

	root := schema.Root{
		Payload: &schema.RootIndex{
			Index: proof1.Index,
			Root:  proof1.Root,
		},
	}
	safeZAddOptions2 := schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   []byte(`FirstSet`),
			Score: float64(43.548),
			Key:   []byte(`myThirdElementKey`),
		},
		RootIndex: &schema.Index{
			Index: proof1.Index,
		},
	}

	proof2, err2 := st.SafeZAdd(safeZAddOptions2)

	assert.NoError(t, err2)
	assert.NotNil(t, proof2)
	assert.Equal(t, uint64(4), proof2.Index)

	key2, _ := SetKey(safeZAddOptions2.Zopts.Key, safeZAddOptions2.Zopts.Set, safeZAddOptions2.Zopts.Score)

	leaf2 := api.Digest(proof2.Index, key2, safeZAddOptions2.Zopts.Key)
	// Here verify if first reference was correctly inserted. We have no root yet.
	verified2 := proof2.Verify(leaf2[:], root)
	assert.True(t, verified2)
}

func TestStoreBySafeIndex(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, err := st.Set(schema.KeyValue{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)})
	assert.NoError(t, err)
	_, err = st.Set(schema.KeyValue{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)})
	assert.NoError(t, err)
	_, err = st.Set(schema.KeyValue{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)})
	assert.NoError(t, err)

	sio1 := schema.SafeIndexOptions{
		Index: uint64(1),
	}

	st.tree.WaitUntil(2)

	safeItem, err := st.BySafeIndex(sio1)

	assert.NoError(t, err)
	assert.NotNil(t, safeItem)
	assert.Equal(t, []byte(`mySecondElementKey`), safeItem.Item.Key)
	assert.Equal(t, []byte(`secondValue`), safeItem.Item.Value)
	assert.Equal(t, uint64(1), safeItem.Item.Index)
	assert.True(t, safeItem.Proof.Verify(
		safeItem.Item.Hash(),
		schema.Root{Payload: &schema.RootIndex{}}, // zerovalue signals no prev root
	))

	// second item with prev root
	prevRoot := safeItem.Proof.NewRoot()
	sio2 := schema.SafeIndexOptions{
		Index: uint64(2),
		RootIndex: &schema.Index{
			Index: prevRoot.Payload.Index,
		},
	}

	safeItem2, err := st.BySafeIndex(sio2)

	assert.NoError(t, err)
	assert.NotNil(t, safeItem2)
	assert.Equal(t, []byte(`myThirdElementKey`), safeItem2.Item.Key)
	assert.Equal(t, []byte(`thirdValue`), safeItem2.Item.Value)
	assert.Equal(t, uint64(2), safeItem2.Item.Index)
	assert.True(t, safeItem2.Proof.Verify(
		safeItem2.Item.Hash(),
		*prevRoot,
	))

	lastRoot, err := st.CurrentRoot()
	assert.NoError(t, err)
	assert.NotNil(t, lastRoot)
	assert.Equal(t, *lastRoot.Payload, *safeItem2.Proof.NewRoot().Payload)
}

func TestStoreBySafeIndexOnSafeZAdd(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	st.Set(schema.KeyValue{Key: []byte(`myFirstElementKey`), Value: []byte(`firstValue`)})
	st.Set(schema.KeyValue{Key: []byte(`mySecondElementKey`), Value: []byte(`secondValue`)})
	st.Set(schema.KeyValue{Key: []byte(`myThirdElementKey`), Value: []byte(`thirdValue`)})

	safeZAddOptions1 := schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   []byte(`FirstSet`),
			Score: float64(43),
			Key:   []byte(`mySecondElementKey`),
		},
	}

	proof1, err := st.SafeZAdd(safeZAddOptions1)

	assert.NoError(t, err)
	assert.NotNil(t, proof1)
	assert.Equal(t, uint64(3), proof1.Index)

	key, _ := SetKey(safeZAddOptions1.Zopts.Key, safeZAddOptions1.Zopts.Set, safeZAddOptions1.Zopts.Score)

	leaf := api.Digest(proof1.Index, key, safeZAddOptions1.Zopts.Key)
	// Here verify if first reference was correctly inserted. We have no root yet.
	verified := proof1.Verify(leaf[:], schema.Root{Payload: &schema.RootIndex{}})
	assert.True(t, verified)

	root := schema.Root{
		Payload: &schema.RootIndex{
			Index: proof1.Index,
			Root:  proof1.Root,
		},
	}
	safeZAddOptions2 := schema.SafeZAddOptions{
		Zopts: &schema.ZAddOptions{
			Set:   []byte(`FirstSet`),
			Score: float64(43.548),
			Key:   []byte(`myThirdElementKey`),
		},
		RootIndex: &schema.Index{
			Index: proof1.Index,
		},
	}

	proof2, err2 := st.SafeZAdd(safeZAddOptions2)

	assert.NoError(t, err2)
	assert.NotNil(t, proof2)
	assert.Equal(t, uint64(4), proof2.Index)

	key2, _ := SetKey(safeZAddOptions2.Zopts.Key, safeZAddOptions2.Zopts.Set, safeZAddOptions2.Zopts.Score)

	leaf2 := api.Digest(proof2.Index, key2, safeZAddOptions2.Zopts.Key)
	// Here verify if first reference was correctly inserted. We have no root yet.
	verified2 := proof2.Verify(leaf2[:], root)
	assert.True(t, verified2)

	// second item with prev root
	prevRoot := schema.Root{
		Payload: &schema.RootIndex{
			Index: proof2.Index,
			Root:  proof2.Root,
		},
	}
	sio2 := schema.SafeIndexOptions{
		Index: uint64(2),
		RootIndex: &schema.Index{
			Index: prevRoot.Payload.Index,
		},
	}

	safeItem2, err := st.BySafeIndex(sio2)

	assert.NoError(t, err)
	assert.NotNil(t, safeItem2)
	assert.Equal(t, []byte(`myThirdElementKey`), safeItem2.Item.Key)
	assert.Equal(t, []byte(`thirdValue`), safeItem2.Item.Value)
	assert.Equal(t, uint64(2), safeItem2.Item.Index)
	assert.True(t, safeItem2.Proof.Verify(
		safeItem2.Item.Hash(),
		prevRoot,
	))
}
