package store

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"strconv"
	"sync"
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
	assert.Equal(t, schema.ErrDuplicatedKeysNotSupported, err)
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
		atomicOps := make([]*schema.BatchOp, batchSize*2)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			atomicOps[i] = &schema.BatchOp{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			}
		}

		for i := 0; i < batchSize; i++ {

			atomicOps[i+batchSize] = &schema.BatchOp{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   atomicOps[i].Operation.(*schema.BatchOp_KVs).KVs.Key,
						Index: nil,
					},
				},
			}
		}

		idx, err := st.SetBatchOps(&schema.BatchOps{Operations: atomicOps})
		assert.NoError(t, err)
		assert.Equal(t, uint64((b+1)*batchSize*2), idx.GetIndex()+1)
	}

	zScanOpt := schema.ZScanOptions{
		Set: []byte(`mySet`),
	}
	zList, err := st.ZScan(zScanOpt)
	assert.NoError(t, err)
	println(len(zList.Items))
	assert.Len(t, zList.Items, batchSize*10)
}

func TestSetBatchAtomicOperationsZAddOnMixedAlreadyPersitedNotPersistedItems(t *testing.T) {
	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	idx, _ := st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// BatchOps payload
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`notPersistedKey`),
						Value: []byte(`notPersistedVal`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   []byte(`notPersistedKey`)},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   []byte(`persistedKey`),
						Index: idx,
					},
				},
			},
		},
	}
	index, err := st.SetBatchOps(aOps)
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
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, schema.ErrEmptySet, err)
}

func TestSetBatchAtomicOperationsInvalidKvKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte{0},
						Value: []byte(`val`),
					},
				},
			},
		},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, ErrInvalidKey, err)
}

func TestSetBatchAtomicOperationsZAddKeyNotFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`key`),
						Score: &schema.Score{
							Score: 5.6,
						},
						Index: &schema.Index{
							Index: 4,
						},
					},
				},
			},
		},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, ErrIndexNotFound, err)
}

func TestSetBatchAtomicOperationsNilElementFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: nil,
			},
		},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, err, status.Error(codes.InvalidArgument, "batch operation is not set"))
}

func TestSetBatchAtomicOperationsUnexpectedType(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.Op_Unexpected{},
			},
		},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, err, status.Error(codes.InvalidArgument, "batch operation has unexpected type *schema.Op_Unexpected"))
}

func TestSetBatchAtomicOperationsDuplicatedKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
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
	_, err := st.SetBatchOps(aOps)

	assert.Equal(t, schema.ErrDuplicatedKeysNotSupported, err)
}

func TestSetBatchAtomicOperationsDuplicatedKeyZAdd(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`key`),
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
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
	_, err := st.SetBatchOps(aOps)

	assert.Equal(t, schema.ErrDuplicatedZAddNotSupported, err)
}

func TestSetBatchAtomicOperationsAsynch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key1`),
						Value: []byte(`val1`),
					},
				},
			},
			{
				Operation: &schema.BatchOp_ZOpts{
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
	_, err := st.SetBatchOps(aOps, WithAsyncCommit(true))

	assert.NoError(t, err)
}

func TestBatchOps_ValidateErrZAddIndexMissing(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, _ = st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// BatchOps payload
	aOps := &schema.BatchOps{
		Operations: []*schema.BatchOp{
			{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`persistedKey`),
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
		},
	}
	_, err := st.SetBatchOps(aOps)
	assert.Equal(t, err, ErrZAddIndexMissing)
}

func TestStore_SetBatchOpsConcurrent(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 1; i <= 10; i++ {
		aOps := &schema.BatchOps{
			Operations: []*schema.BatchOp{},
		}
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)
			aOp := &schema.BatchOp{
				Operation: &schema.BatchOp_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(key),
						Value: []byte(key),
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
			float, err := strconv.ParseFloat(fmt.Sprintf("%d", j), 64)
			if err != nil {
				log.Fatal(err)
			}

			set := val
			refKey := key
			aOp = &schema.BatchOp{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set: []byte(set),
						Key: []byte(refKey),
						Score: &schema.Score{
							Score: float,
						},
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
		}
		go func() {
			idx, err := st.SetBatchOps(aOps)
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			wg.Done()
		}()

	}
	wg.Wait()
	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)

		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		assert.NoError(t, err)
		assert.Len(t, zList.Items, 10)
		assert.Equal(t, zList.Items[i-1].Item.Value, []byte(strconv.FormatUint(uint64(i), 10)))

	}
}

func TestStore_SetBatchOpsConcurrentOnAlreadyPersistedKeys(t *testing.T) {
	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	for i := 1; i <= 10; i++ {
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			_, _ = st.Set(schema.KeyValue{
				Key:   []byte(key),
				Value: []byte(key),
			})
		}
	}

	st.tree.close(true)
	st.Close()

	st, closer := makeStoreAt(dbDir)
	defer closer()

	st.tree.WaitUntil(99)

	wg := sync.WaitGroup{}
	wg.Add(10)

	gIdx := uint64(0)
	for i := 1; i <= 10; i++ {
		aOps := &schema.BatchOps{
			Operations: []*schema.BatchOp{},
		}
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)

			float, err := strconv.ParseFloat(fmt.Sprintf("%d", j), 64)
			if err != nil {
				log.Fatal(err)
			}

			set := val
			refKey := key
			aOp := &schema.BatchOp{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set: []byte(set),
						Key: []byte(refKey),
						Score: &schema.Score{
							Score: float,
						},
						Index: &schema.Index{Index: gIdx},
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
			gIdx++
		}
		go func() {
			idx, err := st.SetBatchOps(aOps)
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)

		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		assert.NoError(t, err)
		assert.Len(t, zList.Items, 10)
		assert.Equal(t, zList.Items[i-1].Item.Value, []byte(strconv.FormatUint(uint64(i), 10)))
	}
}

func TestStore_SetBatchOpsConcurrentOnMixedPersistedAndNotKeys(t *testing.T) {
	// even items are stored on disk with regular sets
	// odd ones are stored inside batch operations
	// zAdd references all items

	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	for i := 1; i <= 10; i++ {
		for j := 1; j <= 10; j++ {
			// even
			if j%2 == 0 {
				key := strconv.FormatUint(uint64(j), 10)
				_, _ = st.Set(schema.KeyValue{
					Key:   []byte(key),
					Value: []byte(key),
				})
			}
		}
	}

	st.tree.close(true)
	st.Close()

	st, closer := makeStoreAt(dbDir)
	defer closer()

	st.tree.WaitUntil(49)

	wg := sync.WaitGroup{}
	wg.Add(10)

	gIdx := uint64(0)
	for i := 1; i <= 10; i++ {
		aOps := &schema.BatchOps{
			Operations: []*schema.BatchOp{},
		}
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)
			var index *schema.Index

			// odd
			if j%2 != 0 {
				aOp := &schema.BatchOp{
					Operation: &schema.BatchOp_KVs{
						KVs: &schema.KeyValue{
							Key:   []byte(key),
							Value: []byte(key),
						},
					},
				}
				aOps.Operations = append(aOps.Operations, aOp)
				index = nil
			} else {
				index = &schema.Index{Index: gIdx}
				gIdx++
			}
			float, err := strconv.ParseFloat(fmt.Sprintf("%d", j), 64)
			if err != nil {
				log.Fatal(err)
			}

			set := val
			refKey := key
			aOp := &schema.BatchOp{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set: []byte(set),
						Key: []byte(refKey),
						Score: &schema.Score{
							Score: float,
						},
						Index: index,
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
		}
		go func() {
			idx, err := st.SetBatchOps(aOps)
			assert.NoError(t, err)
			assert.NotNil(t, idx)
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)
		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		assert.NoError(t, err)
		assert.Len(t, zList.Items, 10)
		assert.Equal(t, zList.Items[i-1].Item.Value, []byte(strconv.FormatUint(uint64(i), 10)))
	}
}

func TestStore_SetBatchOpsConcurrentOnMixedPersistedAndNotOnEqualKeysAndEqualScore(t *testing.T) {
	// Inserting 100 items:
	// even items are stored on disk with regular sets
	// odd ones are stored inside batch operations
	// there are 50 batch Ops with zAdd operation for reference even items already stored
	// and in addition 50 batch Ops with 1 kv operation for odd items and zAdd operation for reference them onFly

	// items have same score. They will be returned in insertion order since key is composed by:
	// {separator}{set}{separator}{score}{key}{bit index presence flag}{index}

	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	keyA := "A"

	var index *schema.Index

	for i := 1; i <= 10; i++ {
		for j := 1; j <= 10; j++ {
			// even
			if j%2 == 0 {
				val := fmt.Sprintf("%d,%d", i, j)
				index, _ = st.Set(schema.KeyValue{
					Key:   []byte(keyA),
					Value: []byte(val),
				})
				assert.NotNil(t, index)
			}
		}
	}

	st.tree.close(true)
	st.Close()

	st, closer := makeStoreAt(dbDir)
	defer closer()

	st.tree.WaitUntil(49)

	wg := sync.WaitGroup{}
	wg.Add(100)

	gIdx := uint64(0)

	for i := 1; i <= 10; i++ {
		for j := 1; j <= 10; j++ {
			aOps := &schema.BatchOps{
				Operations: []*schema.BatchOp{},
			}

			// odd
			if j%2 != 0 {
				val := fmt.Sprintf("%d,%d", i, j)
				aOp := &schema.BatchOp{
					Operation: &schema.BatchOp_KVs{
						KVs: &schema.KeyValue{
							Key:   []byte(keyA),
							Value: []byte(val),
						},
					},
				}
				aOps.Operations = append(aOps.Operations, aOp)
				index = nil
			} else {
				index = &schema.Index{Index: gIdx}
				gIdx++
			}

			float, err := strconv.ParseFloat(fmt.Sprintf("%d", j), 64)
			if err != nil {
				log.Fatal(err)
			}

			refKey := keyA
			set := strconv.FormatUint(uint64(j), 10)
			aOp := &schema.BatchOp{
				Operation: &schema.BatchOp_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set: []byte(set),
						Key: []byte(refKey),
						Score: &schema.Score{
							Score: float,
						},
						Index: index,
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
			go func() {
				idx, err := st.SetBatchOps(aOps)
				assert.NoError(t, err)
				assert.NotNil(t, idx)
				wg.Done()
			}()
		}

	}
	wg.Wait()

	history, err := st.History(&schema.HistoryOptions{
		Key: []byte(keyA),
	})
	assert.NoError(t, err)
	assert.NotNil(t, history)
	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)
		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		assert.NoError(t, err)
		assert.NoError(t, err)
		// item are returned in insertion order since they have same score
		assert.Len(t, zList.Items, 10)
	}
}
