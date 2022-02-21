/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
package database

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

func compactIndex(db DB, timeout time.Duration) error {
	done := make(chan error)

	go func(done chan<- error) {
		err := db.CompactIndex()
		done <- err
	}(done)

	select {
	case err := <-done:
		{
			return err
		}
	case <-time.After(timeout):
		{
			return errors.New("compact index timed out")
		}
	}
}

func execAll(db DB, req *schema.ExecAllRequest, timeout time.Duration) error {
	done := make(chan error)

	go func(done chan<- error) {
		_, err := db.ExecAll(req)
		done <- err
	}(done)

	select {
	case err := <-done:
		{
			return err
		}
	case <-time.After(timeout):
		{
			return errors.New("execAll timed out")
		}
	}
}

func TestConcurrentCompactIndex(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	done := make(chan struct{})
	ack := make(chan struct{})

	cleanUpFreq := 2 * time.Second
	cleanUpTimeout := 3 * time.Second
	execAllTimeout := 5 * time.Second

	go func(ticker *time.Ticker, done <-chan struct{}, ack chan<- struct{}) {
		for {
			select {
			case <-done:
				{
					ack <- struct{}{}
					return
				}
			case <-ticker.C:
				{
					err := compactIndex(db, cleanUpTimeout)
					if err != nil && err != sql.ErrAlreadyClosed {
						panic(err)
					}
				}
			}
		}
	}(time.NewTicker(cleanUpFreq), done, ack)

	txCount := 10
	txSize := 32

	for i := 0; i < txCount; i++ {

		kvs := make([]*schema.Op, txSize)

		for j := 0; j < txSize; j++ {
			key := make([]byte, 32)
			rand.Read(key)

			kvs[j] = &schema.Op{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   key,
						Value: []byte{},
					},
				},
			}
		}

		err := execAll(db, &schema.ExecAllRequest{Operations: kvs}, execAllTimeout)
		require.NoError(t, err)

	}

	time.Sleep(4 * time.Second)

	done <- struct{}{}
	<-ack
}

func TestSetBatch(t *testing.T) {
	db, closer := makeDb()
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

		txhdr, err := db.Set(&schema.SetRequest{KVs: kvList})
		require.NoError(t, err)
		require.Equal(t, uint64(b+2), txhdr.Id)

		for i := 0; i < batchSize; i++ {
			key := []byte(strconv.FormatUint(uint64(i), 10))
			value := []byte(strconv.FormatUint(uint64(b*batchSize+batchSize+i), 10))
			entry, err := db.Get(&schema.KeyRequest{Key: key, SinceTx: txhdr.Id})
			require.NoError(t, err)
			require.Equal(t, value, entry.Value)
			require.Equal(t, uint64(b+2), entry.Tx)

			vitem, err := db.VerifiableGet(&schema.VerifiableGetRequest{KeyRequest: &schema.KeyRequest{Key: key}}) //no prev root
			require.NoError(t, err)
			require.Equal(t, key, vitem.Entry.Key)
			require.Equal(t, value, vitem.Entry.Value)
			require.Equal(t, entry.Tx, vitem.Entry.Tx)

			tx := schema.TxFromProto(vitem.VerifiableTx.Tx)

			entrySpec := EncodeEntrySpec(vitem.Entry.Key, schema.KVMetadataFromProto(vitem.Entry.Metadata), vitem.Entry.Value)

			entrySpecDigest, err := store.EntrySpecDigestFor(int(txhdr.Version))
			require.NoError(t, err)
			require.NotNil(t, entrySpecDigest)

			inclusionProof := schema.InclusionProofFromProto(vitem.InclusionProof)
			verifies := store.VerifyInclusion(
				inclusionProof,
				entrySpecDigest(entrySpec),
				tx.Header().Eh,
			)
			require.True(t, verifies)
		}
	}
}

func TestSetBatchInvalidKvKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte{},
				Value: []byte(`val`),
			},
		}})
	require.Equal(t, store.ErrIllegalArguments, err)
}

func TestSetBatchDuplicatedKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.Set(&schema.SetRequest{
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
	require.Equal(t, schema.ErrDuplicatedKeysNotSupported, err)
}

func TestExecAllOps(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	_, err := db.ExecAll(nil)
	require.Equal(t, store.ErrIllegalArguments, err)

	_, err = db.ExecAll(&schema.ExecAllRequest{})
	require.Error(t, err)

	_, err = db.ExecAll(&schema.ExecAllRequest{Operations: []*schema.Op{}})
	require.Error(t, err)

	_, err = db.ExecAll(&schema.ExecAllRequest{
		Operations: []*schema.Op{
			nil,
		},
	})
	require.Error(t, err)

	batchCount := 10
	batchSize := 100

	for b := 0; b < batchCount; b++ {
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
						Set:      []byte(`mySet`),
						Score:    0.6,
						Key:      atomicOps[i].Operation.(*schema.Op_Kv).Kv.Key,
						BoundRef: true,
					},
				},
			}
		}

		idx, err := db.ExecAll(&schema.ExecAllRequest{Operations: atomicOps})
		require.NoError(t, err)
		require.Equal(t, uint64(b+2), idx.Id)
	}

	zScanOpt := &schema.ZScanRequest{
		Set: []byte(`mySet`),
	}
	zList, err := db.ZScan(zScanOpt)
	require.NoError(t, err)
	println(len(zList.Entries))
	require.Len(t, zList.Entries, batchCount*batchSize)
}

func TestExecAllOpsZAddOnMixedAlreadyPersitedNotPersistedItems(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx, _ := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte(`persistedKey`),
				Value: []byte(`persistedVal`),
			},
		},
	})

	// Ops payload
	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`notPersistedKey`),
						Value: []byte(`notPersistedVal`),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte(`mySet`),
						Score: 0.6,
						Key:   []byte(`notPersistedKey`),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte(`mySet`),
						Score:    0.6,
						Key:      []byte(`persistedKey`),
						AtTx:     idx.Id,
						BoundRef: true,
					},
				},
			},
		},
	}

	index, err := db.ExecAll(aOps)
	require.NoError(t, err)
	require.Equal(t, uint64(3), index.Id)

	list, err := db.ZScan(&schema.ZScanRequest{
		Set:     []byte(`mySet`),
		SinceTx: index.Id,
	})
	require.NoError(t, err)
	require.Equal(t, []byte(`persistedKey`), list.Entries[0].Key)
	require.Equal(t, []byte(`notPersistedKey`), list.Entries[1].Key)
}

func TestExecAllOpsEmptyList(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{},
	}
	_, err := db.ExecAll(aOps)
	require.Equal(t, schema.ErrEmptySet, err)
}

func TestExecAllOpsInvalidKvKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte{},
						Value: []byte(`val`),
					},
				},
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.Equal(t, store.ErrIllegalArguments, err)

	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key"),
						Value: []byte("val"),
					},
				},
			},
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte("rkey"),
						ReferencedKey: []byte("key"),
						AtTx:          1,
						BoundRef:      false,
					},
				},
			},
		},
	}
	_, err = db.ExecAll(aOps)
	require.Equal(t, store.ErrIllegalArguments, err)

	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte("key"),
						Value: []byte("val"),
					},
				},
			},
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte("rkey"),
						ReferencedKey: []byte("key"),
						BoundRef:      true,
					},
				},
			},
		},
	}
	_, err = db.ExecAll(aOps)
	require.NoError(t, err)

	// Ops payload
	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte(`mySet`),
						Score: 0.6,
						Key:   []byte(`rkey`),
					},
				},
			},
		},
	}
	_, err = db.ExecAll(aOps)
	require.Equal(t, ErrReferencedKeyCannotBeAReference, err)
}

func TestExecAllOpsZAddKeyNotFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:      []byte("set"),
						Key:      []byte(`key`),
						Score:    5.6,
						AtTx:     4,
						BoundRef: true,
					},
				},
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.Equal(t, store.ErrTxNotFound, err)
}

func TestExecAllOpsNilElementFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	bOps := make([]*schema.Op, 1)
	bOps[0] = &schema.Op{
		Operation: &schema.Op_ZAdd{
			ZAdd: &schema.ZAddRequest{
				Key:   []byte(`key`),
				Score: 5.6,
				AtTx:  4,
			},
		},
	}

	_, err := db.ExecAll(&schema.ExecAllRequest{Operations: bOps})
	require.Equal(t, store.ErrIllegalArguments, err)
}

func TestSetOperationNilElementFound(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: nil,
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.Error(t, err)
}

func TestExecAllOpsUnexpectedType(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Unexpected{},
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.Error(t, err)
}

func TestExecAllOpsDuplicatedKey(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte(`set`),
						Key:   []byte(`key`),
						Score: 5.6,
					},
				},
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.ErrorIs(t, err, schema.ErrDuplicatedKeysNotSupported)
}

func TestExecAllOpsDuplicatedKeyZAdd(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
						Key:   []byte(`key`),
						Value: []byte(`val`),
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Key:   []byte(`key`),
						Score: 5.6,
					},
				},
			},
			{
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Key:   []byte(`key`),
						Score: 5.6,
					},
				},
			},
		},
	}

	_, err := db.ExecAll(aOps)
	require.Equal(t, schema.ErrDuplicatedZAddNotSupported, err)
}

func TestStore_ExecAllOpsConcurrent(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	wg := sync.WaitGroup{}
	wg.Add(10)

	for i := 1; i <= 10; i++ {
		aOps := &schema.ExecAllRequest{
			Operations: []*schema.Op{},
		}

		for j := 1; j <= 10; j++ {
			key := strconv.FormatUint(uint64(j), 10)
			val := strconv.FormatUint(uint64(i), 10)
			aOp := &schema.Op{
				Operation: &schema.Op_Kv{
					Kv: &schema.KeyValue{
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
				Operation: &schema.Op_ZAdd{
					ZAdd: &schema.ZAddRequest{
						Set:   []byte(set),
						Key:   []byte(refKey),
						Score: float,
					},
				},
			}
			aOps.Operations = append(aOps.Operations, aOp)
		}

		go func() {
			idx, err := db.ExecAll(aOps)
			require.NoError(t, err)
			require.NotNil(t, idx)
			wg.Done()
		}()

	}

	wg.Wait()

	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)

		zList, err := db.ZScan(&schema.ZScanRequest{
			Set:     []byte(set),
			SinceTx: 11,
		})
		require.NoError(t, err)
		require.Len(t, zList.Entries, 10)
		require.Equal(t, zList.Entries[i-1].Entry.Value, []byte(strconv.FormatUint(uint64(i), 10)))

	}
}

func TestExecAllNoWait(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	t.Run("ExecAll with NoWait should be self-contained", func(t *testing.T) {
		aOps := &schema.ExecAllRequest{
			Operations: []*schema.Op{
				{
					Operation: &schema.Op_Ref{
						Ref: &schema.ReferenceRequest{
							Key:           []byte(`ref`),
							ReferencedKey: []byte(`key`),
						},
					},
				},
			},
			NoWait: true,
		}
		_, err := db.ExecAll(aOps)
		require.ErrorIs(t, err, store.ErrIllegalArguments)
		require.ErrorIs(t, err, ErrNoWaitOperationMustBeSelfContained)
	})

	t.Run("ExecAll with NoWait should be self-contained", func(t *testing.T) {
		aOps := &schema.ExecAllRequest{
			Operations: []*schema.Op{
				{
					Operation: &schema.Op_ZAdd{
						ZAdd: &schema.ZAddRequest{
							Set:      []byte("set"),
							Key:      []byte(`key`),
							Score:    5.6,
							AtTx:     4,
							BoundRef: true,
						},
					},
				},
			},
			NoWait: true,
		}
		_, err := db.ExecAll(aOps)
		require.ErrorIs(t, err, store.ErrIllegalArguments)
		require.ErrorIs(t, err, ErrNoWaitOperationMustBeSelfContained)
	})

	t.Run("ExecAll with NoWait consistent key switching from key into reference", func(t *testing.T) {
		_, err := db.Set(&schema.SetRequest{
			KVs: []*schema.KeyValue{
				{Key: []byte("key"), Value: []byte("value")},
			},
		})
		require.NoError(t, err)

		_, err = db.SetReference(&schema.ReferenceRequest{
			Key: []byte("ref"), ReferencedKey: []byte("key"),
		})
		require.NoError(t, err)

		_, err = db.ZAdd(&schema.ZAddRequest{
			Set: []byte("set"),
			Key: []byte("key"),
		})
		require.NoError(t, err)

		aOps := &schema.ExecAllRequest{
			Operations: []*schema.Op{
				{
					Operation: &schema.Op_Kv{
						Kv: &schema.KeyValue{
							Key:   []byte(`key1`),
							Value: []byte(`value1`),
						},
					},
				},
				{
					Operation: &schema.Op_Ref{
						Ref: &schema.ReferenceRequest{
							Key:           []byte(`key`),
							ReferencedKey: []byte(`key1`),
							BoundRef:      true,
						},
					},
				},
			},
			NoWait: true,
		}
		hdr, err := db.ExecAll(aOps)
		require.NoError(t, err)

		entry, err := db.Get(&schema.KeyRequest{Key: []byte("key"), SinceTx: hdr.Id})
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, []byte(`key1`), entry.Key)
		require.Equal(t, []byte(`value1`), entry.Value)
		require.NotNil(t, entry.ReferencedBy)
		require.Equal(t, []byte(`key`), entry.ReferencedBy.Key)
		require.Equal(t, hdr.Id, entry.ReferencedBy.Tx)

		// ref became a reference of a reference
		_, err = db.Get(&schema.KeyRequest{Key: []byte("ref")})
		require.ErrorIs(t, err, ErrMaxKeyResolutionLimitReached)

		// "key" became a reference
		_, err = db.ZScan(&schema.ZScanRequest{
			Set: []byte("set"),
		})
		require.ErrorIs(t, err, ErrMaxKeyResolutionLimitReached)
	})
}

/*
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
			require.NoError(t, err)
			require.NotNil(t, idx)
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)

		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		require.NoError(t, err)
		require.Len(t, zList.Items, 10)
		require.Equal(t, zList.Items[i-1].Item.Value, []byte(strconv.FormatUint(uint64(i), 10)))
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
			require.NoError(t, err)
			require.NotNil(t, idx)
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)
		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		require.NoError(t, err)
		require.Len(t, zList.Items, 10)
		require.Equal(t, zList.Items[i-1].Item.Value, []byte(strconv.FormatUint(uint64(i), 10)))
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
				require.NotNil(t, index)
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
				require.NoError(t, err)
				require.NotNil(t, idx)
				wg.Done()
			}()
		}

	}
	wg.Wait()

	history, err := st.History(&schema.HistoryOptions{
		Key: []byte(keyA),
	})
	require.NoError(t, err)
	require.NotNil(t, history)
	for i := 1; i <= 10; i++ {
		set := strconv.FormatUint(uint64(i), 10)
		zList, err := st.ZScan(schema.ZScanOptions{
			Set: []byte(set),
		})
		require.NoError(t, err)
		require.NoError(t, err)
		// item are returned in insertion order since they have same score
		require.Len(t, zList.Items, 10)
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
	require.NoError(t, err)
	require.Equal(t, uint64(batchSize), idx.GetIndex()+1)

	for i := 0; i < batchSize; i++ {
		item, err := st.ByIndex(schema.Index{
			Index: uint64(i),
		})
		require.NoError(t, err)
		require.Equal(t, []byte(strconv.FormatUint(uint64(batchSize+batchSize+i), 10)), item.Value)
		require.Equal(t, uint64(i), item.Index)
	}
}
*/

func TestOps_ReferenceKeyAlreadyPersisted(t *testing.T) {
	db, closer := makeDb()
	defer closer()

	idx0, _ := db.Set(&schema.SetRequest{
		KVs: []*schema.KeyValue{
			{
				Key:   []byte(`persistedKey`),
				Value: []byte(`persistedVal`),
			},
		},
	})

	aOps := &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           nil,
						ReferencedKey: nil,
						AtTx:          idx0.Id,
					},
				},
			},
		},
	}
	_, err := db.ExecAll(aOps)
	require.Equal(t, store.ErrIllegalArguments, err)

	// Ops payload
	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte(`myReference`),
						ReferencedKey: []byte(`persistedKey`),
						AtTx:          idx0.Id,
						BoundRef:      true,
					},
				},
			},
		},
	}
	idx1, err := db.ExecAll(aOps)
	require.NoError(t, err)

	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte(`myReference1`),
						ReferencedKey: []byte(`myReference`),
						AtTx:          idx1.Id,
						BoundRef:      true,
					},
				},
			},
		},
	}
	_, err = db.ExecAll(aOps)
	require.Equal(t, ErrReferencedKeyCannotBeAReference, err)

	aOps = &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte(`persistedKey`),
						ReferencedKey: []byte(`persistedKey`),
						AtTx:          idx0.Id,
						BoundRef:      true,
					},
				},
			},
		},
	}
	_, err = db.ExecAll(aOps)
	require.Equal(t, ErrFinalKeyCannotBeConvertedIntoReference, err)

	ref, err := db.Get(&schema.KeyRequest{Key: []byte(`myReference`), SinceTx: idx1.Id})
	require.NoError(t, err)
	require.NotEmptyf(t, ref, "Should not be empty")
	require.Equal(t, []byte(`persistedVal`), ref.Value, "Should have referenced item value")
	require.Equal(t, []byte(`persistedKey`), ref.Key, "Should have referenced item value")
}

/*
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
	require.NoError(t, err)

	ref, err := st.Get(schema.Key{Key: []byte(`myTag`)})

	require.NoError(t, err)
	require.NotEmptyf(t, ref, "Should not be empty")
	require.Equal(t, []byte(`val`), ref.Value, "Should have referenced item value")
	require.Equal(t, []byte(`key`), ref.Key, "Should have referenced item value")

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
	require.Equal(t, ErrIndexNotFound, err)
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
	require.Equal(t, ErrReferenceIndexMissing, err)
}
*/
