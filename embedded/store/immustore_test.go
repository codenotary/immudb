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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/ahtree"
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
			tx, err := immuStore.NewWriteOnlyTx()
			if err != nil {
				panic(err)
			}

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				err = tx.Set(k, nil, v)
				if err != nil {
					panic(err)
				}
			}

			txhdr, err := tx.AsyncCommit()
			if err != nil {
				panic(err)
			}

			if uint64(i+1) != txhdr.ID {
				panic(fmt.Errorf("expected %v but actual %v", uint64(i+1), txhdr.ID))
			}
		}

		wg.Done()
	}()

	go func() {
		txID := uint64(1)
		tx := immuStore.NewTxHolder()

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

				if tx.header.ID == uint64(txCount) {
					wg.Done()
					return
				}

				txID = tx.header.ID
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
		txs[c] = immuStore.NewTxHolder()
	}

	for c := 0; c < 10; c++ {
		go func(txHolder *Tx) {
			for c := 0; c < txCount; {
				tx, err := immuStore.NewWriteOnlyTx()
				if err != nil {
					panic(err)
				}

				for j := 0; j < eCount; j++ {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(j))

					v := make([]byte, 8)
					binary.BigEndian.PutUint64(v, uint64(c))

					err = tx.Set(k, nil, v)
					if err != nil {
						panic(err)
					}
				}

				hdr, err := tx.AsyncCommit()
				if err == ErrMaxConcurrencyLimitExceeded {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				if err != nil {
					panic(err)
				}

				err = immuStore.ReadTx(hdr.ID, txHolder)
				if err != nil {
					panic(err)
				}

				for _, e := range txHolder.Entries() {
					_, _, err := immuStore.ReadValue(txHolder, e.key())
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

	_, err = immuStore.commit(nil, nil, false)
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

	_, err = Open("invalid\x00_dir_name", DefaultOptions())
	require.EqualError(t, err, "stat invalid\x00_dir_name: invalid argument")

	require.NoError(t, os.MkdirAll("ro_path", 0500))
	defer os.RemoveAll("ro_path")

	_, err = Open("ro_path/subpath", DefaultOptions())
	require.EqualError(t, err, "mkdir ro_path/subpath: permission denied")

	for _, failedAppendable := range []string{"tx", "commit", "val_0"} {
		injectedError := fmt.Errorf("Injected error for: %s", failedAppendable)
		_, err = Open("edge_cases", DefaultOptions().WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			if subPath == failedAppendable {
				return nil, injectedError
			}
			return &mocked.MockedAppendable{}, nil
		}))
		require.ErrorIs(t, err, injectedError)
	}

	vLog := &mocked.MockedAppendable{}
	vLogs := []appendable.Appendable{vLog}
	txLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	// Should fail reading fileSize from metadata
	cLog.MetadataFn = func() []byte {
		return nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, ErrCorruptedCLog)

	// Should fail reading maxTxEntries from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, ErrCorruptedCLog)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, ErrCorruptedCLog)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		md.PutInt(metaMaxKeyLen, 8)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, ErrCorruptedCLog)

	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		md.PutInt(metaMaxKeyLen, 8)
		md.PutInt(metaMaxValueLen, 16)
		return md.Bytes()
	}

	// Should fail reading cLogSize
	injectedError := errors.New("error")
	cLog.SizeFn = func() (int64, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

	// Should fail setting cLog offset
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize - 1, nil
	}
	cLog.SetOffsetFn = func(off int64) error {
		return injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

	// Should fail validating cLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize - 1, nil
	}
	cLog.SetOffsetFn = func(off int64) error {
		return nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.NoError(t, err)

	// Should fail reading cLog
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

	// Should fail reading txLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	cLog.SetOffsetFn = func(off int64) error {
		return nil
	}
	txLog.SizeFn = func() (int64, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

	// Should fail reading txLogSize
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
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

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
	require.ErrorIs(t, err, ErrorCorruptedTxData)

	// Fail to read last transaction
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		buff := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		require.Less(t, off, int64(len(buff)))
		return copy(bs, buff[off:]), nil
	}
	txLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, injectedError
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.ErrorIs(t, err, injectedError)

	// Fail to initialize aht when opening appendable
	cLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	optsCopy := *opts
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog,
		optsCopy.WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			if strings.HasPrefix(subPath, "aht/") {
				return nil, injectedError
			}
			return &mocked.MockedAppendable{}, nil
		}),
	)
	require.ErrorIs(t, err, injectedError)

	// Fail to initialize indexer
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog,
		optsCopy.WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			if strings.HasPrefix(subPath, "index/") {
				return nil, injectedError
			}
			return &mocked.MockedAppendable{
				SizeFn: func() (int64, error) { return 0, nil },
			}, nil
		}),
	)
	require.ErrorIs(t, err, injectedError)

	// Incorrect tx in indexer
	vLog.CloseFn = func() error { return nil }
	txLog.CloseFn = func() error { return nil }
	cLog.CloseFn = func() error { return nil }
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog,
		optsCopy.WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			switch subPath {
			case "index/nodes":
				return &mocked.MockedAppendable{
					ReadAtFn: func(bs []byte, off int64) (int, error) {
						buff := []byte{
							tbtree.LeafNodeType,
							0, 0, 0, 0,
							0, 0, 0, 1, // One node
							0, 0, 0, 1, // Key size
							'k',        // key
							0, 0, 0, 1, // Value size
							'v',                    // value
							0, 0, 0, 0, 0, 0, 0, 1, // Timestamp
							0, 0, 0, 0, 0, 0, 0, 0, // hOffs
							0, 0, 0, 0, 0, 0, 0, 0, // hSize
						}
						require.Less(t, off, int64(len(buff)))
						return copy(bs, buff[off:]), nil
					},
					CloseFn: func() error { return nil },
				}, nil
			case "index/commit":
				return &mocked.MockedAppendable{
					SizeFn: func() (int64, error) {
						// One clog entry
						return 8, nil
					},
					ReadAtFn: func(bs []byte, off int64) (int, error) {
						buff := []byte{0, 0, 0, 0, 0, 0, 0, 0}
						require.Less(t, off, int64(len(buff)))
						return copy(bs, buff[off:]), nil
					},
					MetadataFn: func() []byte {
						md := appendable.NewMetadata(nil)
						md.PutInt(tbtree.MetaMaxNodeSize, tbtree.DefaultMaxNodeSize)
						return md.Bytes()
					},
					CloseFn: func() error { return nil },
				}, nil
			}
			return &mocked.MockedAppendable{
				SizeFn:   func() (int64, error) { return 0, nil },
				OffsetFn: func() int64 { return 0 },
				CloseFn:  func() error { return nil },
			}, nil
		}),
	)
	require.ErrorIs(t, err, ErrCorruptedCLog)

	// Errors during sync
	mockedApps := []*mocked.MockedAppendable{vLog, txLog, cLog}
	for _, app := range mockedApps {
		app.SyncFn = func() error { return nil }
	}
	for i, checkApp := range mockedApps {
		injectedError = fmt.Errorf("Injected error %d", i)
		checkApp.SyncFn = func() error { return injectedError }

		store, err := OpenWith("edge_cases", vLogs, txLog, cLog, opts)
		require.NoError(t, err)
		err = store.Sync()
		require.ErrorIs(t, err, injectedError)
		err = store.Close()
		require.NoError(t, err)

		checkApp.SyncFn = func() error { return nil }
	}

	// Errors during close
	store, err := OpenWith("edge_cases", vLogs, txLog, cLog, opts)
	require.NoError(t, err)
	err = store.aht.Close()
	require.NoError(t, err)
	err = store.Close()
	require.ErrorIs(t, err, ahtree.ErrAlreadyClosed)

	for i, checkApp := range mockedApps {
		injectedError = fmt.Errorf("Injected error %d", i)
		checkApp.CloseFn = func() error { return injectedError }

		store, err := OpenWith("edge_cases", vLogs, txLog, cLog, opts)
		require.NoError(t, err)
		err = store.Close()
		require.ErrorIs(t, err, injectedError)

		checkApp.CloseFn = func() error { return nil }
	}

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

	_, err = immuStore.fetchAllocTx()
	require.Equal(t, ErrMaxConcurrencyLimitExceeded, err)

	immuStore.releaseAllocTx(tx1)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = immuStore.DualProof(nil, nil)
	require.Equal(t, ErrIllegalArguments, err)

	sourceTx := newTx(1, 1)
	sourceTx.header.ID = 2
	targetTx := newTx(1, 1)
	targetTx.header.ID = 1
	_, err = immuStore.DualProof(sourceTx, targetTx)
	require.Equal(t, ErrSourceTxNewerThanTargetTx, err)

	_, err = immuStore.LinearProof(2, 1)
	require.Equal(t, ErrSourceTxNewerThanTargetTx, err)

	_, err = immuStore.LinearProof(1, uint64(1+immuStore.maxLinearProofLen))
	require.Equal(t, ErrLinearProofMaxLenExceeded, err)

	_, _, err = immuStore.ReadValue(sourceTx, []byte{1, 2, 3})
	require.Equal(t, ErrKeyNotFound, err)

	err = immuStore.validateEntries(nil)
	require.Equal(t, ErrorNoEntriesProvided, err)

	err = immuStore.validateEntries(make([]*EntrySpec, immuStore.maxTxEntries+1))
	require.Equal(t, ErrorMaxTxEntriesLimitExceeded, err)

	entry := &EntrySpec{Key: nil, Value: nil}
	err = immuStore.validateEntries([]*EntrySpec{entry})
	require.Equal(t, ErrNullKey, err)

	entry = &EntrySpec{Key: make([]byte, immuStore.maxKeyLen+1), Value: make([]byte, 1)}
	err = immuStore.validateEntries([]*EntrySpec{entry})
	require.Equal(t, ErrorMaxKeyLenExceeded, err)

	entry = &EntrySpec{Key: make([]byte, 1), Value: make([]byte, immuStore.maxValueLen+1)}
	err = immuStore.validateEntries([]*EntrySpec{entry})
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

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
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

						valRef, err := snap.Get(k)
						if err != nil {
							if err != tbtree.ErrKeyNotFound {
								panic(err)
							}
						}

						val, err := valRef.Resolve()
						if err != nil {
							panic(err)
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

					valRef1, err := immuStore.Get(k)
					if err != nil {
						panic(err)
					}

					v1, err := valRef1.Resolve()
					if err != nil {
						panic(err)
					}

					valRef2, err := snap.Get(k)
					if err != nil {
						panic(err)
					}

					v2, err := valRef2.Resolve()
					if err != nil {
						panic(err)
					}

					if !bytes.Equal(v1, v2) {
						panic(fmt.Errorf("expected %v actual %v", v1, v2))
					}

					if valRef1.Tx() != valRef2.Tx() {
						panic(fmt.Errorf("expected %d actual %d", valRef1.Tx(), valRef2.Tx()))
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

	t.Run("latest set value should be committed", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set([]byte("key"), nil, []byte("value1"))
		require.NoError(t, err)

		err = tx.Set([]byte("key"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx.Commit()
		require.NoError(t, err)

		valRef, err := immuStore.Get([]byte("key"))
		require.NoError(t, err)

		val, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), val)
	})

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreRWTransactions(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, _ := Open("data_tx", opts)
	defer os.RemoveAll("data_tx")

	tx, err := immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1})
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	_, err = tx.Commit()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = tx.Cancel()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	t.Run("cancelled transaction should not produce effects", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		_, err = tx.Get([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		err = tx.Cancel()
		require.NoError(t, err)

		err = tx.Cancel()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.Commit()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		valRef, err := immuStore.Get([]byte{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, uint64(1), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte{3, 2, 1}, v)
	})

	t.Run("read-your-own-writes should be possible before commit", func(t *testing.T) {
		_, err = immuStore.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		tx, err = immuStore.NewTx()
		require.NoError(t, err)

		_, err = tx.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		err = tx.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		valRef, err := tx.Get([]byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(0), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)

		_, err = immuStore.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = tx.Commit()
		require.NoError(t, err)

		valRef, err = immuStore.Get([]byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(2), valRef.Tx())

		v, err = valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)
	})

	t.Run("second ongoing tx after the first commit should fail", func(t *testing.T) {
		tx1, err := immuStore.NewTx()
		require.NoError(t, err)

		tx2, err := immuStore.NewTx()
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		_, err = tx1.Commit()
		require.NoError(t, err)

		_, err = tx2.Commit()
		require.ErrorIs(t, err, ErrTxReadConflict)

		valRef, err := immuStore.Get([]byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(3), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx1"), v)
	})

	t.Run("second ongoing tx after the first cancelation should succeed", func(t *testing.T) {
		tx1, err := immuStore.NewTx()
		require.NoError(t, err)

		tx2, err := immuStore.NewTx()
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		err = tx1.Cancel()
		require.NoError(t, err)

		_, err = tx2.Commit()
		require.NoError(t, err)

		valRef, err := immuStore.Get([]byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(4), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx2"), v)
	})

	t.Run("deleted keys should not be reachable", func(t *testing.T) {
		tx, err := immuStore.NewTx()
		require.NoError(t, err)

		err = tx.Delete([]byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Delete([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = tx.Commit()
		require.NoError(t, err)

		_, err = immuStore.Get([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = immuStore.GetWith([]byte{1, 2, 3}, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		valRef, err := immuStore.GetWith([]byte{1, 2, 3})
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.True(t, valRef.KVMetadata().Deleted())
	})
}

func TestImmudbStoreKVMetadata(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, _ := Open("data_kv_metadata", opts)
	defer os.RemoveAll("data_kv_metadata")

	tx, err := immuStore.NewTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	tx.WithMetadata(NewTxMetadata())

	err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1})
	require.NoError(t, err)

	err = tx.Set([]byte{1, 2, 3}, NewKVMetadata().AsDeleted(true), []byte{3, 2, 1})
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	_, err = immuStore.Get([]byte{1, 2, 3})
	require.ErrorIs(t, err, ErrKeyNotFound)

	valRef, err := immuStore.GetWith([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), valRef.Tx())
	require.True(t, valRef.KVMetadata().Deleted())
	require.Equal(t, uint64(1), valRef.HC())
	require.Equal(t, uint32(3), valRef.Len())
	require.Equal(t, sha256.Sum256([]byte{3, 2, 1}), valRef.HVal())
	require.True(t, NewTxMetadata().Equal(valRef.TxMetadata()))

	v, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte{3, 2, 1}, v)

	t.Run("read deleted key from snapshot should return key not found", func(t *testing.T) {
		snap, err := immuStore.Snapshot()
		require.NoError(t, err)
		require.NotNil(t, snap)

		_, err = snap.Get([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	tx, err = immuStore.NewTx()
	require.NoError(t, err)

	_, err = tx.Get([]byte{1, 2, 3})
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tx.Set([]byte{1, 2, 3}, nil, []byte{1, 1, 1})
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)
	valRef, err = immuStore.Get([]byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(2), valRef.Tx())

	v, err = valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1, 1}, v)
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

	callback := func(txID uint64, index KeyIndex) ([]*EntrySpec, error) {
		return nil, nil
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, error) {
		return nil, errors.New("error")
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Error(t, err)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: []byte("value")},
		}, nil
	}

	hdr, err := immuStore.CommitWith(callback, true)
	require.NoError(t, err)

	require.Equal(t, uint64(1), immuStore.IndexInfo())

	tx := immuStore.NewTxHolder()
	immuStore.ReadTx(hdr.ID, tx)

	_, val, err := immuStore.ReadValue(tx, []byte(fmt.Sprintf("keyInsertedAtTx%d", hdr.ID)))
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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
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
			tx := immuStore.NewTxHolder()

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

							_, val, err := immuStore.ReadValue(tx, k)
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
	immuStore, err := Open("data_compaction_remote_storage", opts)
	require.NoError(t, err)

	defer os.RemoveAll("data_compaction_remote_storage")

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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		summary := fmt.Sprintf("summary%d", i)
		tx.WithMetadata(NewTxMetadata().WithSummary([]byte(summary)))

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, NewKVMetadata(), v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, err = immuStore.CommitWith(func(txID uint64, index KeyIndex) ([]*EntrySpec, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: nil},
		}, nil
	}, false)
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data_inclusion_proof", opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, false, immuStore.NewTxHolder())
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

			e := &EntrySpec{Key: key, Metadata: NewKVMetadata(), Value: value}

			verifies := htree.VerifyInclusion(proof, e.Digest(), tx.header.Eh)
			require.True(t, verifies)

			_, v, err = immuStore.ReadValue(tx, key)
			require.NoError(t, err)
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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)

		err = immuStore.WaitForTx(txhdr.ID, nil)
		require.NoError(t, err)

		err = immuStore.WaitForIndexingUpto(txhdr.ID, nil)
		require.NoError(t, err)

		var k0 [8]byte
		exists, err := immuStore.ExistKeyWith(k0[:], nil)
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

	tx := immuStore.NewTxHolder()

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), tx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), tx.header.ID)

		p, err := immuStore.aht.DataAt(uint64(i + 1))
		require.NoError(t, err)

		alh := tx.header.Alh()
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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	for {
		n, err := immuStore.BlInfo()
		require.NoError(t, err)
		if n == uint64(txCount) {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	tx := immuStore.NewTxHolder()

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), tx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), tx.header.ID)

		p, err := immuStore.aht.DataAt(uint64(i + 1))
		require.NoError(t, err)

		alh := tx.header.Alh()
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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		summary := fmt.Sprintf("summary%d", i)
		tx.WithMetadata(NewTxMetadata().WithSummary([]byte(summary)))

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.Commit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	sourceTx := immuStore.NewTxHolder()
	targetTx := immuStore.NewTxHolder()

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		summary := fmt.Sprintf("summary%d", i)
		require.Equal(t, []byte(summary), sourceTx.header.Metadata.Summary())

		for j := i; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.header.ID)

			dproof, err := immuStore.DualProof(sourceTx, targetTx)
			require.NoError(t, err)

			verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
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

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	for {
		n, err := immuStore.BlInfo()
		require.NoError(t, err)
		if n == uint64(txCount) {
			break
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}

	sourceTx := immuStore.NewTxHolder()
	targetTx := immuStore.NewTxHolder()

	targetTxID := uint64(txCount)
	err = immuStore.ReadTx(targetTxID, targetTx)
	require.NoError(t, err)
	require.Equal(t, uint64(txCount), targetTx.header.ID)

	for i := 0; i < txCount-1; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		dproof, err := immuStore.DualProof(sourceTx, targetTx)
		require.NoError(t, err)

		verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
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

	tx, err := immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	_, err = tx.Commit()
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)

		currentID, currentAlh := immuStore.Alh()
		require.Equal(t, txhdr.ID, currentID)
		require.Equal(t, txhdr.Alh(), currentAlh)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	os.RemoveAll("data_consistency_proof_reopen/aht")

	immuStore, err = Open("data_consistency_proof_reopen", opts.WithMaxValueLen(opts.MaxValueLen-1))
	require.NoError(t, err)

	txholder := immuStore.NewTxHolder()

	for i := 0; i < txCount; i++ {
		txID := uint64(i + 1)

		ri, err := immuStore.NewTxReader(txID, false, txholder)
		require.NoError(t, err)

		txi, err := ri.Read()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txi.header.ID)
	}

	sourceTx := immuStore.NewTxHolder()
	targetTx := immuStore.NewTxHolder()

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		for j := i + 1; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.header.ID)

			lproof, err := immuStore.LinearProof(sourceTxID, targetTxID)
			require.NoError(t, err)

			verifies := VerifyLinearProof(lproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.True(t, verifies)

			dproof, err := immuStore.DualProof(sourceTx, targetTx)
			require.NoError(t, err)

			verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
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
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			txhdr, err := tx.AsyncCommit()
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txhdr.ID)
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
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			txhdr, err := tx.AsyncCommit()
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txhdr.ID)
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

	txReader, err := immuStore.NewTxReader(1, false, immuStore.NewTxHolder())
	require.NoError(t, err)

	_, err = txReader.Read()
	require.Equal(t, ErrNoMoreEntries, err)

	txCount := 100
	eCount := 64

	emulatedFailures := 0

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(j+1))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.Commit()
		if err != nil {
			require.Equal(t, errEmulatedAppendableError, err)
			emulatedFailures++
		} else {
			require.Equal(t, uint64(i+1-emulatedFailures), txhdr.ID)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(path, opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, false, immuStore.NewTxHolder())
	require.NoError(t, err)

	for i := 0; i < txCount-emulatedFailures; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for _, txe := range txEntries {
			proof, err := tx.Proof(txe.key())
			require.NoError(t, err)

			value := make([]byte, txe.vLen)
			_, err = immuStore.ReadValueAt(value, txe.vOff, txe.hVal)
			require.NoError(t, err)

			e := &EntrySpec{Key: txe.key(), Value: value}

			verifies := htree.VerifyInclusion(proof, e.Digest(), tx.header.Eh)
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

	tx, err := masterStore.NewWriteOnlyTx()
	require.NoError(t, err)

	tx.WithMetadata(NewTxMetadata())

	err = tx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr, err := tx.Commit()
	require.NoError(t, err)
	require.NotNil(t, hdr)

	txholder := masterStore.NewTxHolder()
	etx, err := masterStore.ExportTx(1, txholder)
	require.NoError(t, err)

	rhdr, err := replicaStore.ReplicateTx(etx, false)
	require.NoError(t, err)
	require.NotNil(t, rhdr)

	require.Equal(t, hdr.ID, rhdr.ID)
	require.Equal(t, hdr.Alh(), rhdr.Alh())

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
			tx, err := immuStore.NewWriteOnlyTx()
			if err != nil {
				panic(err)
			}

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				if err != nil {
					panic(err)
				}
			}

			_, err = tx.Commit()
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
			tx, err := immuStore.NewWriteOnlyTx()
			if err != nil {
				panic(err)
			}

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				if err != nil {
					panic(err)
				}
			}

			_, err = tx.Commit()
			if err != nil {
				panic(err)
			}
		}
	}
}

func TestImmudbStoreIncompleteCommitWrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_incomplete_commit_write")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	immuStore, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	tx, err := immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val1"))
	require.NoError(t, err)

	hdr, err := tx.Commit()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	// Append garbage at the end of files, immudb must be able to recover
	// as long as the full commit log entry is not created

	append := func(path string, bytes int) {
		fl, err := os.OpenFile(filepath.Join(dir, path), os.O_APPEND|os.O_WRONLY, 0644)
		require.NoError(t, err)
		defer fl.Close()

		buff := make([]byte, bytes)
		_, err = rand.Read(buff)
		require.NoError(t, err)

		_, err = fl.Write(buff)
		require.NoError(t, err)
	}

	append("commit/00000000.txi", 11) // Commit log entry is 12 bytes, must add less than that
	append("tx/00000000.tx", 100)
	append("val_0/00000000.val", 100)

	// Force reindexing and rebuilding the aht tree
	err = os.RemoveAll(filepath.Join(dir, "aht"))
	require.NoError(t, err)

	immuStore, err = Open(dir, DefaultOptions())
	require.NoError(t, err)

	valRef, err := immuStore.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr.ID, valRef.Tx())

	value, err := valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val1"), value)

	err = immuStore.Close()
	require.NoError(t, err)

}

func TestImmudbStoreTruncatedCommitLog(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_truncated_commit_log")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	immuStore, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	tx, err := immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val1"))
	require.NoError(t, err)

	hdr1, err := tx.Commit()
	require.NoError(t, err)
	require.NotNil(t, hdr1)

	tx, err = immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val2"))
	require.NoError(t, err)

	hdr2, err := tx.Commit()
	require.NoError(t, err)
	require.NotNil(t, hdr2)
	require.NotEqual(t, hdr1.ID, hdr2.ID)

	err = immuStore.Close()
	require.NoError(t, err)

	// Truncate the commit log - it must discard the last transaction but other than
	// that the immudb should work correctly
	// Note: This may change once the truthly appendable interface is implemented
	//       (https://github.com/codenotary/immudb/issues/858)

	txFile := filepath.Join(dir, "commit/00000000.txi")
	stat, err := os.Stat(txFile)
	require.NoError(t, err)

	err = os.Truncate(txFile, stat.Size()-1)
	require.NoError(t, err)

	// Remove the index, it does not support truncation of commits now
	err = os.RemoveAll(filepath.Join(dir, "index"))
	require.NoError(t, err)

	immuStore, err = Open(dir, DefaultOptions())
	require.NoError(t, err)

	err = immuStore.WaitForIndexingUpto(hdr1.ID, make(<-chan struct{}))
	require.NoError(t, err)

	valRef, err := immuStore.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr1.ID, valRef.Tx())

	value, err := valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val1"), value)

	// ensure we can correctly write more data into the store
	tx, err = immuStore.NewWriteOnlyTx()
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val2"))
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	valRef, err = immuStore.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr2.ID, valRef.Tx())

	value, err = valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val2"), value)

	// test after reopening the store
	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(dir, DefaultOptions())
	require.NoError(t, err)

	valRef, err = immuStore.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr2.ID, valRef.Tx())

	value, err = valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val2"), value)

	err = immuStore.Close()
	require.NoError(t, err)
}
