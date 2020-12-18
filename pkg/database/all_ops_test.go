package database

import (
	"strconv"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
)

/*
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
*/
func TestExecAllOps(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	batchSize := 100

	for b := 0; b < 10; b++ {
		atomicOps := make([]*schema.Op, batchSize*2)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			atomicOps[i] = &schema.Op{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   key,
						Value: value,
					},
				},
			}
		}

		for i := 0; i < batchSize; i++ {

			atomicOps[i+batchSize] = &schema.Op{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:     []byte(`mySet`),
						Score:   0.6,
						Key:     atomicOps[i].Operation.(*schema.Op_Kv).Kv.Key,
						AtTx:    0,
						SinceTx: uint64((b + 1) * batchSize),
					},
				},
			}
		}

		idx, err := db.ExecAll(&schema.ExecAllRequest{Operations: atomicOps})
		assert.NoError(t, err)
		assert.Equal(t, uint64(b+1), idx.Id)
	}

	zScanOpt := &schema.ZScanRequest{
		Set:     []byte(`mySet`),
		SinceTx: 10,
	}
	zList, err := db.ZScan(zScanOpt)
	assert.NoError(t, err)
	println(len(zList.Items))
	assert.Len(t, zList.Items, batchSize)
}

/*
func TestExecAllOpsZAddOnMixedAlreadyPersitedNotPersistedItems(t *testing.T) {
	dbDir := tmpDir()

	st, _ := makeStoreAt(dbDir)

	idx, _ := st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`notPersistedKey`),
						Value: []byte(`notPersistedVal`),
					},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Set:   []byte(`mySet`),
						Score: &schema.Score{Score: 0.6},
						Key:   []byte(`notPersistedKey`)},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
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
	index, err := st.ExecAllOps(aOps)
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

func TestExecAllOpsEmptyList(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, schema.ErrEmptySet, err)
}

func TestExecAllOpsInvalidKvKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte{0},
						Value: []byte(`val`),
					},
				},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, ErrInvalidKey, err)
}

func TestExecAllOpsZAddKeyNotFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ZOpts{
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
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, ErrIndexNotFound, err)
}

func TestExecAllOpsNilElementFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	bOps := make([]*schema.Op, 2)
	op := &schema.Op{
		Operation: &schema.Op_ZOpts{
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
	}
	bOps[1] = op
	aOps := &schema.Ops{Operations: bOps}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, status.Error(codes.InvalidArgument, "Op is not set"), err)
}

func TestSetOperationNilElementFound(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: nil,
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, err, status.Error(codes.InvalidArgument, "operation is not set"))
}

func TestExecAllOpsUnexpectedType(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Unexpected{},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, err, status.Error(codes.InvalidArgument, "batch operation has unexpected type *schema.Op_Unexpected"))
}

func TestExecAllOpsDuplicatedKey(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
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
	_, err := st.ExecAllOps(aOps)

	assert.Equal(t, schema.ErrDuplicatedKeysNotSupported, err)
}

func TestExecAllOpsDuplicatedKeyZAdd(t *testing.T) {
	st, closer := makeStore()
	defer closer()
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
					ZOpts: &schema.ZAddOptions{
						Key: []byte(`key`),
						Score: &schema.Score{
							Score: 5.6,
						},
					},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
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
	_, err := st.ExecAllOps(aOps)

	assert.Equal(t, schema.ErrDuplicatedZAddNotSupported, err)
}

func TestExecAllOpsAsynch(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key1`),
						Value: []byte(`val1`),
					},
				},
			},
			{
				Operation: &schema.Op_ZOpts{
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
	_, err := st.ExecAllOps(aOps, WithAsyncCommit(true))

	assert.NoError(t, err)
}

func TestOps_ValidateErrZAddIndexMissing(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	_, _ = st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ZOpts{
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
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, err, ErrZAddIndexMissing)
}

func TestStore_ExecAllOpsConcurrent(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	wg := sync.WaitGroup{}
	wg.Add(10)
	for i := 1; i <= 10; i++ {
		aOps := &schema.Ops{
			Operations: []*schema.Op{},
		}
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)
			aOp := &schema.Op{
				Operation: &schema.Op_KVs{
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
			aOp = &schema.Op{
				Operation: &schema.Op_ZOpts{
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
			idx, err := st.ExecAllOps(aOps)
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

func TestStore_ExecAllOpsConcurrentOnAlreadyPersistedKeys(t *testing.T) {
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
		aOps := &schema.Ops{
			Operations: []*schema.Op{},
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
			aOp := &schema.Op{
				Operation: &schema.Op_ZOpts{
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
			idx, err := st.ExecAllOps(aOps)
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

func TestStore_ExecAllOpsConcurrentOnMixedPersistedAndNotKeys(t *testing.T) {
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
		aOps := &schema.Ops{
			Operations: []*schema.Op{},
		}
		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)
			var index *schema.Index

			// odd
			if j%2 != 0 {
				aOp := &schema.Op{
					Operation: &schema.Op_KVs{
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
			aOp := &schema.Op{
				Operation: &schema.Op_ZOpts{
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
			idx, err := st.ExecAllOps(aOps)
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

func TestStore_ExecAllOpsConcurrentOnMixedPersistedAndNotOnEqualKeysAndEqualScore(t *testing.T) {
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
			aOps := &schema.Ops{
				Operations: []*schema.Op{},
			}

			// odd
			if j%2 != 0 {
				val := fmt.Sprintf("%d,%d", i, j)
				aOp := &schema.Op{
					Operation: &schema.Op_KVs{
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
			aOp := &schema.Op{
				Operation: &schema.Op_ZOpts{
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
				idx, err := st.ExecAllOps(aOps)
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

func TestExecAllOpsMonotoneTsRange(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	batchSize := 100

	atomicOps := make([]*schema.Op, batchSize)

	for i := 0; i < batchSize; i++ {
		key := []byte(strconv.FormatUint(uint64(i), 10))
		value := []byte(strconv.FormatUint(uint64(batchSize+batchSize+i), 10))
		atomicOps[i] = &schema.Op{
			Operation: &schema.Op_KVs{
				KVs: &schema.KeyValue{
					Key:   key,
					Value: value,
				},
			},
		}
	}
	idx, err := st.ExecAllOps(&schema.Ops{Operations: atomicOps})
	assert.NoError(t, err)
	assert.Equal(t, uint64(batchSize), idx.GetIndex()+1)

	for i := 0; i < batchSize; i++ {
		item, err := st.ByIndex(schema.Index{
			Index: uint64(i),
		})
		assert.NoError(t, err)
		assert.Equal(t, []byte(strconv.FormatUint(uint64(batchSize+batchSize+i), 10)), item.Value)
		assert.Equal(t, uint64(i), item.Index)
	}
}

func TestOps_ReferenceKeyAlreadyPersisted(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	idx0, _ := st.Set(schema.KeyValue{
		Key:   []byte(`persistedKey`),
		Value: []byte(`persistedVal`),
	})

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ROpts{
					ROpts: &schema.ReferenceOptions{
						Reference: []byte(`myReference`),
						Key:       []byte(`persistedKey`),
						Index:     idx0,
					},
				},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.NoError(t, err)

	ref, err := st.Get(schema.Key{Key: []byte(`myReference`)})

	assert.NoError(t, err)
	assert.NotEmptyf(t, ref, "Should not be empty")
	assert.Equal(t, []byte(`persistedVal`), ref.Value, "Should have referenced item value")
	assert.Equal(t, []byte(`persistedKey`), ref.Key, "Should have referenced item value")

}

func TestOps_ReferenceKeyNotYetPersisted(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_KVs{
					KVs: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_ROpts{
					ROpts: &schema.ReferenceOptions{
						Reference: []byte(`myTag`),
						Key:       []byte(`key`),
					},
				},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.NoError(t, err)

	ref, err := st.Get(schema.Key{Key: []byte(`myTag`)})

	assert.NoError(t, err)
	assert.NotEmptyf(t, ref, "Should not be empty")
	assert.Equal(t, []byte(`val`), ref.Value, "Should have referenced item value")
	assert.Equal(t, []byte(`key`), ref.Key, "Should have referenced item value")

}

func TestOps_ReferenceIndexNotExists(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ROpts{
					ROpts: &schema.ReferenceOptions{
						Reference: []byte(`myReference`),
						Key:       []byte(`persistedKey`),
						Index: &schema.Index{
							Index: 1234,
						},
					},
				},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, ErrIndexNotFound, err)
}

func TestOps_ReferenceIndexMissing(t *testing.T) {
	st, closer := makeStore()
	defer closer()

	// Ops payload
	aOps := &schema.Ops{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ROpts{
					ROpts: &schema.ReferenceOptions{
						Reference: []byte(`myReference`),
						Key:       []byte(`persistedKey`),
					},
				},
			},
		},
	}
	_, err := st.ExecAllOps(aOps)
	assert.Equal(t, ErrReferenceIndexMissing, err)
}
*/
