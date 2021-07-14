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
package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/tbtree"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImmudbStoreConcurrency(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open("data_concurrency", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_concurrency")

	require.NotNil(t, immuStore)

	txCount := 100
	eCount := 1000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for i := 0; i < txCount; i++ {
			kvs := make([]*KV, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				kvs[j] = &KV{Key: k, Value: v}
			}

			txMetadata, err := immuStore.Commit(kvs, false)
			if err != nil {
				panic(err)
			}

			if uint64(i+1) != txMetadata.ID {
				panic(fmt.Errorf("expected %v but actual %v", uint64(i+1), txMetadata.ID))
			}
		}

		wg.Done()
	}()

	go func() {
		txID := uint64(1)
		tx := immuStore.NewTx()

		for {
			time.Sleep(time.Duration(100) * time.Millisecond)

			txReader, err := immuStore.NewTxReader(txID, false, tx)
			if err != nil {
				panic(err)
			}

			for {
				time.Sleep(time.Duration(10) * time.Millisecond)

				tx, err := txReader.Read()
				if err == ErrNoMoreEntries {
					break
				}
				if err != nil {
					panic(err)
				}

				if tx.ID == uint64(txCount) {
					wg.Done()
					return
				}

				txID = tx.ID
			}
		}
	}()

	wg.Wait()

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreConcurrentCommits(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(5)
	immuStore, err := Open("data_concurrent_commits", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_concurrent_commits")

	require.NotNil(t, immuStore)

	txCount := 100
	eCount := 100

	var wg sync.WaitGroup
	wg.Add(10)

	txs := make([]*Tx, 10)
	for c := 0; c < 10; c++ {
		txs[c] = immuStore.NewTx()
	}

	for c := 0; c < 10; c++ {
		go func(tx *Tx) {
			for c := 0; c < txCount; {
				kvs := make([]*KV, eCount)

				for j := 0; j < eCount; j++ {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(j))

					v := make([]byte, 8)
					binary.BigEndian.PutUint64(v, uint64(c))

					kvs[j] = &KV{Key: k, Value: v}
				}

				md, err := immuStore.Commit(kvs, false)
				if err == ErrMaxConcurrencyLimitExceeded {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				if err != nil {
					panic(err)
				}

				err = immuStore.ReadTx(md.ID, tx)
				if err != nil {
					panic(err)
				}

				for _, e := range tx.Entries() {
					_, err := immuStore.ReadValue(tx, e.key())
					if err != nil {
						panic(err)
					}
				}

				c++
			}

			wg.Done()
		}(txs[c])
	}

	wg.Wait()

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreOpenWithInvalidPath(t *testing.T) {
	_, err := Open("immustore_test.go", DefaultOptions())
	require.Equal(t, ErrorPathIsNotADirectory, err)
}

func TestImmudbStoreOnClosedStore(t *testing.T) {
	immuStore, err := Open("closed_store", DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)
	defer os.RemoveAll("closed_store")

	err = immuStore.ReadTx(1, nil)
	require.Equal(t, ErrTxNotFound, err)

	err = immuStore.Close()
	require.NoError(t, err)

	err = immuStore.Close()
	require.Equal(t, ErrAlreadyClosed, err)

	err = immuStore.Sync()
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrAlreadyClosed, err)

	err = immuStore.ReadTx(1, nil)
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.Equal(t, ErrAlreadyClosed, err)
}

func TestImmudbStoreSettings(t *testing.T) {
	immuStore, err := Open("store_settings", DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)
	defer os.RemoveAll("store_settings")

	require.Equal(t, DefaultOptions().ReadOnly, immuStore.ReadOnly())
	require.Equal(t, DefaultOptions().Synced, immuStore.Synced())
	require.Equal(t, 1, immuStore.MaxConcurrency())
	require.Equal(t, DefaultOptions().MaxIOConcurrency, immuStore.MaxIOConcurrency())
	require.Equal(t, DefaultOptions().MaxTxEntries, immuStore.MaxTxEntries())
	require.Equal(t, DefaultOptions().MaxKeyLen, immuStore.MaxKeyLen())
	require.Equal(t, DefaultOptions().MaxValueLen, immuStore.MaxValueLen())
	require.Equal(t, DefaultOptions().MaxLinearProofLen, immuStore.MaxLinearProofLen())
}

func TestImmudbStoreEdgeCases(t *testing.T) {
	defer os.RemoveAll("edge_cases")

	_, err := Open("edge_cases", nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = OpenWith("edge_cases", nil, nil, nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	opts := DefaultOptions().WithMaxConcurrency(1)

	_, err = OpenWith("edge_cases", nil, nil, nil, opts)
	require.Equal(t, ErrIllegalArguments, err)

	vLog := &mocked.MockedAppendable{}
	vLogs := []appendable.Appendable{vLog}
	txLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	// Should fail reading fileSize from metadata
	cLog.MetadataFn = func() []byte {
		return nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail reading maxTxEntries from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		md.PutInt(metaMaxKeyLen, 8)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		md.PutInt(metaMaxKeyLen, 8)
		md.PutInt(metaMaxValueLen, 16)
		return md.Bytes()
	}

	// Should fail reading cLogSize
	cLog.SizeFn = func() (int64, error) {
		return 0, errors.New("error")
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail validating cLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail reading cLog
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, errors.New("error")
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail reading txLogSize
	cLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	txLog.SizeFn = func() (int64, error) {
		return 0, errors.New("error")
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	// Should fail validating txLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		for i := 0; i < len(bs); i++ {
			bs[i]++
		}
		return minInt(len(bs), 8+4+8+8), nil
	}
	txLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.Error(t, err)

	immuStore, err := Open("edge_cases", opts)
	require.NoError(t, err)

	var zeroTime time.Time

	immuStore.lastNotification = zeroTime
	immuStore.notify(Info, false, "info message")
	immuStore.lastNotification = zeroTime
	immuStore.notify(Warn, false, "warn message")
	immuStore.lastNotification = zeroTime
	immuStore.notify(Error, false, "error message")

	tx1, err := immuStore.fetchAllocTx()
	require.NoError(t, err)

	tx2, err := immuStore.fetchAllocTx()
	require.NoError(t, err)

	_, err = immuStore.fetchAllocTx()
	require.Equal(t, ErrMaxConcurrencyLimitExceeded, err)

	immuStore.releaseAllocTx(tx1)
	immuStore.releaseAllocTx(tx2)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = immuStore.DualProof(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	sourceTx := NewTx(1, 1)
	sourceTx.ID = 2
	targetTx := NewTx(1, 1)
	targetTx.ID = 1
	_, err = immuStore.DualProof(sourceTx, targetTx)
	require.Equal(t, ErrSourceTxNewerThanTargetTx, err)

	_, err = immuStore.LinearProof(2, 1)
	require.Equal(t, ErrSourceTxNewerThanTargetTx, err)

	_, err = immuStore.LinearProof(1, uint64(1+immuStore.maxLinearProofLen))
	require.Equal(t, ErrLinearProofMaxLenExceeded, err)

	_, err = immuStore.ReadValue(sourceTx, []byte{1, 2, 3})
	require.Equal(t, ErrKeyNotFound, err)

	err = immuStore.validateEntries(nil)
	require.Equal(t, ErrorNoEntriesProvided, err)

	err = immuStore.validateEntries(make([]*KV, immuStore.maxTxEntries+1))
	require.Equal(t, ErrorMaxTxEntriesLimitExceeded, err)

	entry := &KV{Key: nil, Value: nil}
	err = immuStore.validateEntries([]*KV{entry})
	require.Equal(t, ErrNullKey, err)

	entry = &KV{Key: make([]byte, immuStore.maxKeyLen+1), Value: make([]byte, 1)}
	err = immuStore.validateEntries([]*KV{entry})
	require.Equal(t, ErrorMaxKeyLenExceeded, err)

	entry = &KV{Key: make([]byte, 1), Value: make([]byte, immuStore.maxValueLen+1)}
	err = immuStore.validateEntries([]*KV{entry})
	require.Equal(t, ErrorMaxValueLenExceeded, err)
}

func TestImmudbSetBlErr(t *testing.T) {
	opts := DefaultOptions().WithMaxConcurrency(1)
	immuStore, err := Open("data_bl_err", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_bl_err")

	immuStore.SetBlErr(errors.New("error"))

	_, err = immuStore.BlInfo()
	require.Error(t, err)
}

func TestImmudbTxOffsetAndSize(t *testing.T) {
	opts := DefaultOptions().WithMaxConcurrency(1)
	immuStore, err := Open("data_tx_off_sz", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_tx_off_sz")

	immuStore.mutex.Lock()
	defer immuStore.mutex.Unlock()

	_, _, err = immuStore.txOffsetAndSize(0)
	require.Equal(t, ErrIllegalArguments, err)
}

func TestImmudbStoreIndexing(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_indexing", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_indexing")

	require.NotNil(t, immuStore)

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	_, err = immuStore.Commit([]*KV{
		{Key: []byte("key"), Value: []byte("value")},
		{Key: []byte("key"), Value: []byte("value")},
	}, false)
	require.Equal(t, ErrDuplicatedKey, err)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			for {
				txID, _ := immuStore.Alh()

				snap, err := immuStore.SnapshotSince(txID)
				if err != nil {
					panic(err)
				}

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						v := make([]byte, 8)
						binary.BigEndian.PutUint64(v, snap.Ts()-1)

						val, _, _, err := snap.Get(k)
						if err != nil {
							if err != tbtree.ErrKeyNotFound {
								panic(err)
							}
						}

						if err == nil {
							if !bytes.Equal(v, val) {
								panic(fmt.Errorf("expected %v actual %v", v, val))
							}
						}
					}
				}

				if snap.Ts() == uint64(txCount) {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(eCount-1))

					v1, tx1, _, err := immuStore.Get(k)
					if err != nil {
						panic(err)
					}

					v2, tx2, _, err := snap.Get(k)
					if err != nil {
						panic(err)
					}

					if !bytes.Equal(v1, v2) {
						panic(fmt.Errorf("expected %v actual %v", v1, v2))
					}

					if tx1 != tx2 {
						panic(fmt.Errorf("expected %d actual %d", tx1, tx2))
					}

					txs, err := immuStore.History(k, 0, false, txCount)
					if err != nil {
						panic(err)
					}

					if len(txs) != txCount {
						panic(fmt.Errorf("expected %d actual %d", txCount, len(txs)))
					}

					snap.Close()
					break
				}

				snap.Close()
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreUniqueCommit(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, _ := Open("data_unique", opts)
	defer os.RemoveAll("data_unique")

	_, err := immuStore.Commit([]*KV{{Key: []byte{1, 2, 3}, Value: []byte{3, 2, 1}, Unique: false}}, false)
	require.NoError(t, err)

	_, err = immuStore.Commit([]*KV{{Key: []byte{1, 2, 3}, Value: []byte{1, 1, 1}, Unique: true}}, false)
	require.Equal(t, ErrKeyAlreadyExists, err)

	v, tx, _, err := immuStore.Get([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, []byte{3, 2, 1}, v)
	require.Equal(t, uint64(1), tx)

	_, err = immuStore.Commit([]*KV{{Key: []byte{0, 0, 0}, Value: []byte{1, 1, 1}, Unique: true}}, false)
	require.NoError(t, err)

	_, err = immuStore.Commit([]*KV{{Key: []byte{1, 0, 0}, Value: []byte{0, 0, 1}, Unique: true}, {Key: []byte{1, 0, 0}, Value: []byte{0, 1, 1}, Unique: true}}, false)
	require.Equal(t, ErrDuplicatedKey, err)
}

func TestImmudbStoreCommitWith(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_commit_with", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_commit_with")

	require.NotNil(t, immuStore)
	defer immuStore.Close()

	_, err = immuStore.CommitWith(nil, false)
	require.Equal(t, ErrIllegalArguments, err)

	callback := func(txID uint64, index KeyIndex) ([]*KV, error) {
		return nil, nil
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	callback = func(txID uint64, index KeyIndex) ([]*KV, error) {
		return nil, errors.New("error")
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Error(t, err)

	callback = func(txID uint64, index KeyIndex) ([]*KV, error) {
		return []*KV{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: []byte("value")},
		}, nil
	}

	md, err := immuStore.CommitWith(callback, false)
	require.NoError(t, err)

	tx := immuStore.NewTx()
	immuStore.ReadTx(md.ID, tx)

	val, err := immuStore.ReadValue(tx, []byte(fmt.Sprintf("keyInsertedAtTx%d", md.ID)))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
}

func TestImmudbStoreHistoricalValues(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	opts.WithIndexOptions(opts.IndexOpts.WithFlushThld(10))

	immuStore, err := Open("data_historical", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_historical")

	require.NotNil(t, immuStore)

	txCount := 10
	eCount := 10

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	_, err = immuStore.Commit([]*KV{
		{Key: []byte("key"), Value: []byte("value")},
		{Key: []byte("key"), Value: []byte("value")},
	}, false)
	require.Equal(t, ErrDuplicatedKey, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	time.Sleep(100 * time.Millisecond)

	err = immuStore.CompactIndex()
	require.NoError(t, err)

	err = immuStore.CompactIndex()
	require.Equal(t, tbtree.ErrCompactionThresholdNotReached, err)

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			tx := immuStore.NewTx()

			for {
				snap, err := immuStore.Snapshot()
				if err != nil {
					panic(err)
				}

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						txIDs, err := snap.History(k, 0, false, txCount)
						if err != nil {
							panic(err)
						}
						if int(snap.Ts()) != len(txIDs) {
							panic(fmt.Errorf("expected %v actual %v", int(snap.Ts()), len(txIDs)))
						}

						for _, txID := range txIDs {
							v := make([]byte, 8)
							binary.BigEndian.PutUint64(v, txID-1)

							immuStore.ReadTx(txID, tx)

							val, err := immuStore.ReadValue(tx, k)
							if err != nil {
								panic(err)
							}

							if !bytes.Equal(v, val) {
								panic(fmt.Errorf("expected %v actual %v", v, val))
							}
						}
					}
				}

				snap.Close()

				if snap.Ts() == uint64(txCount) {
					break
				}

				time.Sleep(time.Duration(100) * time.Millisecond)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreCompactionFailureForRemoteStorage(t *testing.T) {
	opts := DefaultOptions().WithCompactionDisabled(true)
	immuStore, err := Open("data_historical", opts)
	require.NoError(t, err)

	err = immuStore.CompactIndex()
	require.Equal(t, ErrCompactionUnsupported, err)
}

func TestImmudbStoreInclusionProof(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_inclusion_proof", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_inclusion_proof")

	require.NotNil(t, immuStore)

	txCount := 100
	eCount := 100

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, err = immuStore.Commit([]*KV{{Key: []byte{}, Value: []byte{}}}, false)
	require.Equal(t, ErrAlreadyClosed, err)

	_, err = immuStore.CommitWith(func(txID uint64, index KeyIndex) ([]*KV, error) {
		return []*KV{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: nil},
		}, nil
	}, false)
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data_inclusion_proof", opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, false, immuStore.NewTx())
	require.NoError(t, err)

	for i := 0; i < txCount; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for j, e := range txEntries {
			proof, err := tx.Proof(e.key())
			require.NoError(t, err)

			key := txEntries[j].key()

			ki, err := tx.IndexOf(key)
			require.NoError(t, err)
			require.Equal(t, j, ki)

			value := make([]byte, txEntries[j].vLen)
			_, err = immuStore.ReadValueAt(value, txEntries[j].VOff(), txEntries[j].HVal())
			require.NoError(t, err)

			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			require.Equal(t, k, key)
			require.Equal(t, v, value)

			kv := &KV{Key: key, Value: value}

			verifies := htree.VerifyInclusion(proof, kv.Digest(), tx.Eh())
			require.True(t, verifies)

			v, err = immuStore.ReadValue(tx, key)
			require.Equal(t, value, v)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestLeavesMatchesAHTSync(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxLinearProofLen(0).WithMaxConcurrency(1)
	immuStore, err := Open("data_leaves_alh", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_leaves_alh")

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)

		err = immuStore.WaitForTx(txMetadata.ID, nil)
		require.NoError(t, err)

		err = immuStore.WaitForIndexingUpto(txMetadata.ID, nil)
		require.NoError(t, err)

		exists, err := immuStore.ExistKeyWith(kvs[0].Key, nil, false)
		require.NoError(t, err)
		require.True(t, exists)
	}

	for {
		n, err := immuStore.BlInfo()
		require.NoError(t, err)
		if n == uint64(txCount) {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	tx := immuStore.NewTx()

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), tx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), tx.ID)

		p, err := immuStore.aht.DataAt(uint64(i + 1))
		require.NoError(t, err)

		alh := tx.Alh
		require.Equal(t, alh[:], p)
	}
}

func TestLeavesMatchesAHTASync(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_leaves_alh_async", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_leaves_alh_async")

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	for {
		n, err := immuStore.BlInfo()
		require.NoError(t, err)
		if n == uint64(txCount) {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	tx := immuStore.NewTx()

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), tx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), tx.ID)

		p, err := immuStore.aht.DataAt(uint64(i + 1))
		require.NoError(t, err)

		alh := tx.Alh
		require.Equal(t, alh[:], p)
	}
}

func TestImmudbStoreConsistencyProof(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_consistency_proof", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof")

	require.NotNil(t, immuStore)

	txCount := 16
	eCount := 10

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	sourceTx := immuStore.NewTx()
	targetTx := immuStore.NewTx()

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.ID)

		for j := i; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.ID)

			dproof, err := immuStore.DualProof(sourceTx, targetTx)
			require.NoError(t, err)

			verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh, targetTx.Alh)
			require.True(t, verifies)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreConsistencyProofAgainstLatest(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_consistency_proof_latest", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof_latest")

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 10

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)
	}

	for {
		n, err := immuStore.BlInfo()
		require.NoError(t, err)
		if n == uint64(txCount) {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	sourceTx := immuStore.NewTx()
	targetTx := immuStore.NewTx()

	targetTxID := uint64(txCount)
	err = immuStore.ReadTx(targetTxID, targetTx)
	require.NoError(t, err)
	require.Equal(t, uint64(txCount), targetTx.ID)

	for i := 0; i < txCount-1; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.ID)

		dproof, err := immuStore.DualProof(sourceTx, targetTx)
		require.NoError(t, err)

		verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh, targetTx.Alh)
		require.True(t, verifies)
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreConsistencyProofReopened(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_consistency_proof_reopen", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof_reopen")

	require.NotNil(t, immuStore)

	txCount := 16
	eCount := 100

	_, err = immuStore.Commit(nil, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txMetadata.ID)

		currentID, currentAlh := immuStore.Alh()
		require.Equal(t, txMetadata.ID, currentID)
		require.Equal(t, txMetadata.Alh(), currentAlh)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, err = immuStore.Commit([]*KV{{Key: []byte{}, Value: []byte{}}}, false)
	require.Equal(t, ErrAlreadyClosed, err)

	os.RemoveAll("data_consistency_proof_reopen/aht")

	immuStore, err = Open("data_consistency_proof_reopen", opts.WithMaxValueLen(opts.MaxValueLen-1))
	require.NoError(t, err)

	tx := immuStore.NewTx()

	for i := 0; i < txCount; i++ {
		txID := uint64(i + 1)

		ri, err := immuStore.NewTxReader(txID, false, tx)
		require.NoError(t, err)

		txi, err := ri.Read()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txi.ID)
	}

	sourceTx := immuStore.NewTx()
	targetTx := immuStore.NewTx()

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.ID)

		for j := i + 1; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.ID)

			lproof, err := immuStore.LinearProof(sourceTxID, targetTxID)
			require.NoError(t, err)

			verifies := VerifyLinearProof(lproof, sourceTxID, targetTxID, sourceTx.Alh, targetTx.Alh)
			require.True(t, verifies)

			dproof, err := immuStore.DualProof(sourceTx, targetTx)
			require.NoError(t, err)

			verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh, targetTx.Alh)
			require.True(t, verifies)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestReOpenningImmudbStore(t *testing.T) {
	defer os.RemoveAll("data_reopenning")

	itCount := 3
	txCount := 100
	eCount := 10

	for it := 0; it < itCount; it++ {
		opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
		immuStore, err := Open("data_reopenning", opts)
		require.NoError(t, err)

		for i := 0; i < txCount; i++ {
			kvs := make([]*KV, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &KV{Key: k, Value: v}
			}

			txMetadata, err := immuStore.Commit(kvs, false)
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txMetadata.ID)
		}

		err = immuStore.Close()
		require.NoError(t, err)
	}
}

func TestReOpenningWithCompressionEnabledImmudbStore(t *testing.T) {
	defer os.RemoveAll("data_compression")

	itCount := 3
	txCount := 100
	eCount := 10

	for it := 0; it < itCount; it++ {
		opts := DefaultOptions().
			WithSynced(false).
			WithCompressionFormat(appendable.GZipCompression).
			WithCompresionLevel(appendable.DefaultCompression).
			WithMaxConcurrency(1)

		immuStore, err := Open("data_compression", opts)
		require.NoError(t, err)

		for i := 0; i < txCount; i++ {
			kvs := make([]*KV, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &KV{Key: k, Value: v}
			}

			txMetadata, err := immuStore.Commit(kvs, false)
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txMetadata.ID)
		}

		err = immuStore.Close()
		require.NoError(t, err)
	}
}

func TestUncommittedTxOverwriting(t *testing.T) {
	path := "data_overwriting"
	err := os.Mkdir(path, 0700)
	require.NoError(t, err)
	defer os.RemoveAll("data_overwriting")

	opts := DefaultOptions().WithMaxConcurrency(1)

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaFileSize, opts.FileSize)
	metadata.PutInt(metaMaxTxEntries, opts.MaxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.MaxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.MaxValueLen)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithSynced(opts.Synced).
		WithFileMode(opts.FileMode).
		WithMetadata(metadata.Bytes())

	vLogPath := filepath.Join(path, "val_0")
	appendableOpts.WithFileExt("val")
	vLog, err := multiapp.Open(vLogPath, appendableOpts)
	require.NoError(t, err)

	txLogPath := filepath.Join(path, "tx")
	appendableOpts.WithFileExt("tx")
	txLog, err := multiapp.Open(txLogPath, appendableOpts)
	require.NoError(t, err)

	cLogPath := filepath.Join(path, "commit")
	appendableOpts.WithFileExt("txi")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	require.NoError(t, err)

	failingVLog := &FailingAppendable{vLog, 2}
	failingTxLog := &FailingAppendable{txLog, 5}
	failingCLog := &FailingAppendable{cLog, 5}

	immuStore, err := OpenWith(path, []appendable.Appendable{failingVLog}, failingTxLog, failingCLog, opts)
	require.NoError(t, err)

	txReader, err := immuStore.NewTxReader(1, false, immuStore.NewTx())
	require.NoError(t, err)

	_, err = txReader.Read()
	require.Equal(t, ErrNoMoreEntries, err)

	txCount := 100
	eCount := 64

	emulatedFailures := 0

	for i := 0; i < txCount; i++ {
		kvs := make([]*KV, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(j+1))

			kvs[j] = &KV{Key: k, Value: v}
		}

		txMetadata, err := immuStore.Commit(kvs, false)
		if err != nil {
			require.Equal(t, errEmulatedAppendableError, err)
			emulatedFailures++
		} else {
			require.Equal(t, uint64(i+1-emulatedFailures), txMetadata.ID)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(path, opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, false, immuStore.NewTx())
	require.NoError(t, err)

	for i := 0; i < txCount-emulatedFailures; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for _, e := range txEntries {
			proof, err := tx.Proof(e.key())
			require.NoError(t, err)

			value := make([]byte, e.vLen)
			_, err = immuStore.ReadValueAt(value, e.vOff, e.hVal)
			require.NoError(t, err)

			kv := &KV{Key: e.key(), Value: value}

			verifies := htree.VerifyInclusion(proof, kv.Digest(), tx.Eh())
			require.True(t, verifies)
		}
	}

	_, err = r.Read()
	require.Equal(t, ErrNoMoreEntries, err)

	require.Equal(t, uint64(txCount-emulatedFailures), immuStore.TxCount())

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestExportAndReplicateTx(t *testing.T) {
	masterStore, err := Open("data_master_export_replicate", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data_master_export_replicate")

	replicaStore, err := Open("data_replica_export_replicate", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data_replica_export_replicate")

	md, err := masterStore.Commit([]*KV{{Key: []byte("key1"), Value: []byte("value1")}}, false)
	require.NoError(t, err)
	require.NotNil(t, md)

	tx := masterStore.NewTx()
	etx, err := masterStore.ExportTx(1, tx)
	require.NoError(t, err)

	rmd, err := replicaStore.ReplicateTx(etx, false)
	require.NoError(t, err)
	require.NotNil(t, rmd)

	require.Equal(t, md.ID, rmd.ID)
	require.Equal(t, md.Alh(), rmd.Alh())

	_, err = replicaStore.ReplicateTx(nil, false)
	require.Equal(t, ErrIllegalArguments, err)
}

var errEmulatedAppendableError = errors.New("emulated appendable error")

type FailingAppendable struct {
	appendable.Appendable
	errorRate int
}

func (la *FailingAppendable) Append(bs []byte) (off int64, n int, err error) {
	if rand.Intn(100) < la.errorRate {
		return 0, 0, errEmulatedAppendableError
	}

	return la.Appendable.Append(bs)
}

func BenchmarkSyncedAppend(b *testing.B) {
	opts := DefaultOptions().WithMaxConcurrency(1)
	immuStore, _ := Open("data_synced_bench", opts)
	defer os.RemoveAll("data_synced_bench")

	for i := 0; i < b.N; i++ {
		txCount := 1000
		eCount := 100

		for i := 0; i < txCount; i++ {
			kvs := make([]*KV, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &KV{Key: k, Value: v}
			}

			_, err := immuStore.Commit(kvs, false)
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, _ := Open("data_async_bench", opts)
	defer os.RemoveAll("data_async_bench")

	for i := 0; i < b.N; i++ {
		txCount := 1000
		eCount := 1000

		for i := 0; i < txCount; i++ {
			kvs := make([]*KV, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &KV{Key: k, Value: v}
			}

			_, err := immuStore.Commit(kvs, false)
			if err != nil {
				panic(err)
			}
		}
	}
}
