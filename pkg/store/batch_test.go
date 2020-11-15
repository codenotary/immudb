package store

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestSetBatch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	batchSize := 100

	for b := 0; b < 10; b++ {
		kvList := make([]*schema.KeyValue, batchSize)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			kvList[i] = &schema.KeyValue{
				Key:   key,
				Value: value,
			}
		}

		index, err := st.SetBatch(schema.KVList{KVs: kvList})
		assert.NoError(t, err)
		assert.Equal(t, uint64((b+1)*batchSize), index.GetIndex()+1)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			item, err := st.Get(schema.Key{Key: key})
			assert.NoError(t, err)
			assert.Equal(t, value, item.Value)
			assert.Equal(t, uint64(b*batchSize+i), item.Index)

			safeItem, err := st.SafeGet(schema.SafeGetOptions{Key: key}) //no prev root
			assert.NoError(t, err)
			assert.Equal(t, key, safeItem.Item.Key)
			assert.Equal(t, value, safeItem.Item.Value)
			assert.Equal(t, item.Index, safeItem.Item.Index)
			assert.True(t, safeItem.Proof.Verify(
				safeItem.Item.Hash(),
				schema.Root{Payload: &schema.RootIndex{}}, // zerovalue signals no prev root
			))
		}
	}
}

func TestSetBatchInvalidKvKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, err := st.SetBatch(schema.KVList{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte{0},
				Value: []byte(`val`),
			},
		}})
	assert.Equal(t, ErrInvalidKey, err)
}

func TestSetBatchDuplicatedKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, err := st.SetBatch(schema.KVList{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte(`key`),
				Value: []byte(`val`),
			},
			{
				Key:   []byte(`key`),
				Value: []byte(`val`),
			},
		}},
	)
	assert.Equal(t, schema.ErrDuplicateKeysNotSupported, err)
}

func TestSetBatchAsynch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, err := st.SetBatch(schema.KVList{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte(`key1`),
				Value: []byte(`val1`),
			},
			{
				Key:   []byte(`key2`),
				Value: []byte(`val2`),
			},
		}},
		WithAsyncCommit(true),
	)
	assert.NoError(t, err)
}

func TestSetBatchAtomicOperations(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	batchSize := 100

	for b := 0; b < 10; b++ {
		atomicOps := make([]*schema.AtomicOperation, batchSize*2)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			atomicOps[i] = &schema.AtomicOperation{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			}
		}

		for i := 0; i < batchSize; i++ {

			atomicOps[i+batchSize] = &schema.AtomicOperation{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   atomicOps[i].Operation.(*schema.AtomicOperation_KVs).KVs.Key,
						Index: nil,
					},
				},
			}
		}

		idx, err := st.SetBatchAtomicOperations(&schema.AtomicOperations{Operations: atomicOps})
		assert.NoError(t, err)
		assert.Equal(t, uint64((b+1)*batchSize*2), idx.GetIndex()+1)
	}

	zScanOpt := schema.ZScanOptions{
		Set: []byte(`mySet`),
	}
	zList, err := st.ZScan(zScanOpt)
	assert.NoError(t, err)
	assert.Len(t, zList.Items, batchSize)
}

func TestSetBatchAtomicOperationsZAddOnMixedAlreadyPersitedNotPersistedItems(t *testing.T) {
	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	_, _ = st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// AtomicOperations payload
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`notPersistedKey`),
						Value: []byte(`notPersistedVal`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   []byte(`notPersistedKey`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   []byte(`persistedKey`),
					},
				},
			},
		},
	}
	index, err := st.SetBatchAtomicOperations(aOps)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), index.Index)

	st.tree.close(true)
	st.Close()

	st, closer := makeStoreAt(dbDir)
	defer closer()

	st.tree.WaitUntil(3)

	list, err := st.ZScan(schema.ZScanOptions{
		Set: []byte(`mySet`),
	})

	assert.Equal(t, []byte(`notPersistedKey`), list.Items[0].Item.Key)
	assert.Equal(t, []byte(`persistedKey`), list.Items[1].Item.Key)
}

func TestSetBatchAtomicOperationsEmptyList(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{},
	}
	_, err := st.SetBatchAtomicOperations(aOps)
	assert.Equal(t, schema.ErrEmptySet, err)
}

func TestSetBatchAtomicOperationsInvalidKvKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte{0},
						Value: []byte(`val`),
					},
				},
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps)
	assert.Equal(t, ErrInvalidKey, err)
}

func TestSetBatchAtomicOperationsZAddKeyNotFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte{0},
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestSetBatchAtomicOperationsNilElementFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: nil,
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps)
	assert.Equal(t, schema.ErrEmptySet, err)
}

func TestSetBatchAtomicOperationsUnexpectedType(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_Unexpected{},
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps)
	assert.Equal(t, fmt.Errorf("batch operation has unexpected type *schema.AtomicOperation_Unexpected"), err)
}

func TestSetBatchAtomicOperationsDuplicatedKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`key`),
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps)

	assert.Equal(t, schema.ErrDuplicateKeysNotSupported, err)
}

func TestSetBatchAtomicOperationsAsynch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	aOps := &schema.AtomicOperations{
		Operations: []*schema.AtomicOperation{
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key1`),
						Value: []byte(`val1`),
					},
				},
			},
			{
				Operation: &schema.AtomicOperation_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`key`),
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
		},
	}
	_, err := st.SetBatchAtomicOperations(aOps, WithAsyncCommit(true))

	assert.NoError(t, err)
}
