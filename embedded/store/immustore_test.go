/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
					_, err := immuStore.ReadValue(e)
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
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.Sync()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.FlushIndex(100, true)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = immuStore.commit(&OngoingTx{entries: []*EntrySpec{
		{Key: []byte("key1")},
	}}, nil, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.ReadTx(1, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)
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
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = OpenWith("edge_cases", nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	opts := DefaultOptions().WithMaxConcurrency(1)

	_, err = OpenWith("edge_cases", nil, nil, nil, opts)
	require.ErrorIs(t, err, ErrIllegalArguments)

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

	vLog := &mocked.MockedAppendable{
		CloseFn: func() error { return nil },
	}

	vLogs := []appendable.Appendable{vLog}

	txLog := &mocked.MockedAppendable{
		CloseFn: func() error { return nil },
	}

	cLog := &mocked.MockedAppendable{
		CloseFn: func() error { return nil },
	}

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
				SizeFn:  func() (int64, error) { return 0, nil },
				CloseFn: func() error { return nil },
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
			nLog := &mocked.MockedAppendable{
				ReadAtFn: func(bs []byte, off int64) (int, error) {
					buff := []byte{
						tbtree.LeafNodeType,
						0, 1, // One node
						0, 1, // Key size
						'k',  // key
						0, 1, // Value size
						'v',                    // value
						0, 0, 0, 0, 0, 0, 0, 1, // Timestamp
						0, 0, 0, 0, 0, 0, 0, 0, // hOffs
						0, 0, 0, 0, 0, 0, 0, 0, // hSize
					}
					require.Less(t, off, int64(len(buff)))
					return copy(bs, buff[off:]), nil
				},
				SyncFn:  func() error { return nil },
				CloseFn: func() error { return nil },
			}

			hLog := &mocked.MockedAppendable{
				SetOffsetFn: func(off int64) error { return nil },
				SizeFn: func() (int64, error) {
					return 0, nil
				},
				SyncFn:  func() error { return nil },
				CloseFn: func() error { return nil },
			}

			switch subPath {
			case "index/nodes":
				return nLog, nil
			case "index/history":
				return hLog, nil
			case "index/commit":
				return &mocked.MockedAppendable{
					SizeFn: func() (int64, error) {
						// One clog entry
						return 100, nil
					},
					AppendFn: func(bs []byte) (off int64, n int, err error) {
						return 0, 0, nil
					},
					ReadAtFn: func(bs []byte, off int64) (int, error) {
						buff := [20 + 32 + 16 + 32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 33, 0, 0, 0, 33}
						nLogChecksum, err := appendable.Checksum(nLog, 0, 33)
						if err != nil {
							return 0, err
						}
						copy(buff[20:], nLogChecksum[:])

						hLogChecksum, err := appendable.Checksum(hLog, 0, 0)
						if err != nil {
							return 0, err
						}
						copy(buff[20+32+16:], hLogChecksum[:])

						require.Less(t, off, int64(len(buff)))
						return copy(bs, buff[off:]), nil
					},
					MetadataFn: func() []byte {
						md := appendable.NewMetadata(nil)
						md.PutInt(tbtree.MetaVersion, tbtree.Version)
						md.PutInt(tbtree.MetaMaxNodeSize, tbtree.DefaultMaxNodeSize)
						md.PutInt(tbtree.MetaMaxKeySize, tbtree.DefaultMaxKeySize)
						md.PutInt(tbtree.MetaMaxValueSize, tbtree.DefaultMaxValueSize)
						return md.Bytes()
					},
					SetOffsetFn: func(off int64) error { return nil },
					FlushFn: func() error {
						return nil
					},
					SyncFn: func() error {
						return nil
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
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = immuStore.DualProof(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

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

	_, err = sourceTx.EntryOf([]byte{1, 2, 3})
	require.Equal(t, ErrKeyNotFound, err)

	t.Run("validateEntries", func(t *testing.T) {
		err = immuStore.validateEntries(nil)
		require.ErrorIs(t, err, ErrorNoEntriesProvided)

		err = immuStore.validateEntries(make([]*EntrySpec, immuStore.maxTxEntries+1))
		require.ErrorIs(t, err, ErrorMaxTxEntriesLimitExceeded)

		entry := &EntrySpec{Key: nil, Value: nil}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrNullKey)

		entry = &EntrySpec{Key: make([]byte, immuStore.maxKeyLen+1), Value: make([]byte, 1)}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrorMaxKeyLenExceeded)

		entry = &EntrySpec{Key: make([]byte, 1), Value: make([]byte, immuStore.maxValueLen+1)}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrorMaxValueLenExceeded)
	})

	t.Run("validatePreconditions", func(t *testing.T) {
		err = immuStore.validatePreconditions(nil)
		require.NoError(t, err)

		err = immuStore.validatePreconditions([]Precondition{
			nil,
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionNull)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyMustExist{},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionNullKey)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyMustExist{
				Key: make([]byte, immuStore.maxKeyLen+1),
			},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionMaxKeyLenExceeded)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyMustNotExist{},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionNullKey)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyMustNotExist{
				Key: make([]byte, immuStore.maxKeyLen+1),
			},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionMaxKeyLenExceeded)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyNotModifiedAfterTx{
				TxID: 1,
			},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionNullKey)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyNotModifiedAfterTx{
				Key:  make([]byte, immuStore.maxKeyLen+1),
				TxID: 1,
			},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionMaxKeyLenExceeded)

		err = immuStore.validatePreconditions([]Precondition{
			&PreconditionKeyNotModifiedAfterTx{
				Key:  []byte("key"),
				TxID: 0,
			},
		})
		require.ErrorIs(t, err, ErrInvalidPrecondition)
		require.ErrorIs(t, err, ErrInvalidPreconditionInvalidTxID)
	})
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
	require.ErrorIs(t, err, ErrIllegalArguments)
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

					txs, hCount, err := immuStore.History(k, 0, false, txCount)
					if err != nil {
						panic(err)
					}

					if len(txs) != txCount {
						panic(fmt.Errorf("expected %d actual %d", txCount, len(txs)))
					}

					if int(hCount) != txCount {
						panic(fmt.Errorf("expected %d actual %d", txCount, hCount))
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

	err = immuStore.FlushIndex(-10, true)
	require.ErrorIs(t, err, tbtree.ErrIllegalArguments)

	err = immuStore.FlushIndex(100, true)
	require.NoError(t, err)

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

	t.Run("after closing write-only tx edge cases", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		require.Nil(t, tx.Metadata())

		err = tx.Set(nil, nil, []byte{3, 2, 1})
		require.ErrorIs(t, err, ErrNullKey)

		err = tx.Set(make([]byte, immuStore.maxKeyLen+1), nil, []byte{3, 2, 1})
		require.ErrorIs(t, err, ErrorMaxKeyLenExceeded)

		err = tx.Set([]byte{1, 2, 3}, nil, make([]byte, immuStore.maxValueLen+1))
		require.ErrorIs(t, err, ErrorMaxValueLenExceeded)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1})
		require.NoError(t, err)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1, 0})
		require.NoError(t, err)

		_, err = tx.Get([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, err = tx.ExistKeyWith([]byte{1}, []byte{1})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		err = tx.Delete([]byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, err = tx.NewKeyReader(&KeyReaderSpec{})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, err = tx.Commit()
		require.NoError(t, err)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1, 0})
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.NewKeyReader(&KeyReaderSpec{})
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.Commit()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		err = tx.Cancel()
		require.ErrorIs(t, err, ErrAlreadyClosed)
	})

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
		require.Equal(t, uint64(1), valRef.HC())
		require.Equal(t, uint32(4), valRef.Len())
		require.Nil(t, valRef.KVMetadata())
		require.Nil(t, valRef.TxMetadata())
		require.Equal(t, sha256.Sum256([]byte{3, 2, 1, 0}), valRef.HVal())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte{3, 2, 1, 0}, v)
	})

	t.Run("read-your-own-writes should be possible before commit", func(t *testing.T) {
		_, err := immuStore.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		tx, err := immuStore.NewTx()
		require.NoError(t, err)

		_, err = tx.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		err = tx.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		exists, err := tx.ExistKeyWith([]byte("key1"), []byte("key"))
		require.NoError(t, err)
		require.True(t, exists)

		r, err := tx.NewKeyReader(&KeyReaderSpec{Prefix: []byte("key")})
		require.NoError(t, err)
		require.NotNil(t, r)

		k, _, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)

		_, err = tx.Commit()
		require.ErrorIs(t, err, tbtree.ErrReadersNotClosed)

		err = r.Close()
		require.NoError(t, err)

		valRef, err := tx.Get([]byte("key1"))
		require.NoError(t, err)
		require.Equal(t, uint64(0), valRef.Tx())
		require.Equal(t, uint64(1), valRef.HC())
		require.Equal(t, uint32(6), valRef.Len())
		require.Nil(t, valRef.KVMetadata())
		require.Nil(t, valRef.TxMetadata())
		require.Equal(t, sha256.Sum256([]byte("value1")), valRef.HVal())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)

		_, err = immuStore.Get([]byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = tx.Commit()
		require.NoError(t, err)

		_, err = tx.ExistKeyWith([]byte("key1"), []byte("key1"))
		require.ErrorIs(t, err, ErrAlreadyClosed)

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

	t.Run("second ongoing tx with multiple entries after the first commit should fail", func(t *testing.T) {
		tx1, err := immuStore.NewTx()
		require.NoError(t, err)

		tx2, err := immuStore.NewTx()
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key2"), nil, []byte("value2_tx2"))
		require.NoError(t, err)

		_, err = tx1.Commit()
		require.NoError(t, err)

		_, err = tx2.Commit()
		require.ErrorIs(t, err, ErrTxReadConflict)

		valRef, err := immuStore.Get([]byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(4), valRef.Tx())

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
		require.Equal(t, uint64(5), valRef.Tx())

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
		require.NotNil(t, valRef.KVMetadata())
		require.False(t, valRef.KVMetadata().IsExpirable())
	})

	t.Run("non-expired keys should be reachable", func(t *testing.T) {
		nearFuture := time.Now().Add(2 * time.Second)

		tx, err := immuStore.NewTx()
		require.NoError(t, err)

		md := NewKVMetadata()
		err = md.ExpiresAt(nearFuture)
		require.NoError(t, err)

		err = tx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
		require.NoError(t, err)

		_, err = tx.Commit()
		require.NoError(t, err)

		valRef, err := immuStore.Get([]byte("expirableKey"))
		require.NoError(t, err)
		require.NotNil(t, valRef)

		val, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("expirableValue"), val)

		time.Sleep(2 * time.Second)

		// already expired
		_, err = immuStore.Get([]byte("expirableKey"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)

		// expired entries can not be resolved
		valRef, err = immuStore.GetWith([]byte("expirableKey"))
		require.NoError(t, err)
		_, err = valRef.Resolve()
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)

		// expired entries are not returned
		_, err = immuStore.GetWith([]byte("expirableKey"), IgnoreExpired)
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)
	})

	t.Run("expired keys should not be reachable", func(t *testing.T) {
		now := time.Now()

		tx, err := immuStore.NewTx()
		require.NoError(t, err)

		md := NewKVMetadata()
		err = md.ExpiresAt(now)
		require.NoError(t, err)

		err = tx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
		require.NoError(t, err)

		_, err = tx.Commit()
		require.NoError(t, err)

		// already expired
		_, err = immuStore.Get([]byte("expirableKey"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		// expired entries can not be resolved
		valRef, err := immuStore.GetWith([]byte("expirableKey"))
		require.NoError(t, err)
		_, err = valRef.Resolve()
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)
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

	md := NewKVMetadata()
	err = md.AsDeleted(true)
	require.NoError(t, err)

	err = tx.Set([]byte{1, 2, 3}, md, []byte{3, 2, 1})
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

func TestImmudbStoreNonIndexableEntries(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, _ := Open("data_kv_metadata_non_indexable", opts)
	defer os.RemoveAll("data_kv_metadata_non_indexable")

	tx, err := immuStore.NewTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	md := NewKVMetadata()
	err = md.AsNonIndexable(true)
	require.NoError(t, err)

	err = tx.Set([]byte("nonIndexedKey"), md, []byte("nonIndexedValue"))
	require.NoError(t, err)

	err = tx.Set([]byte("indexedKey"), nil, []byte("indexedValue"))
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	_, err = immuStore.Get([]byte("nonIndexedKey"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	valRef, err := immuStore.Get([]byte("indexedKey"))
	require.NoError(t, err)
	require.NotNil(t, valRef)

	val, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("indexedValue"), val)

	// commit tx with all non-indexable entries
	tx, err = immuStore.NewTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Set([]byte("nonIndexedKey1"), md, []byte("nonIndexedValue1"))
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	_, err = immuStore.Get([]byte("nonIndexedKey1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	// commit simple tx with an indexable entry
	tx, err = immuStore.NewTx()
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Set([]byte("indexedKey1"), nil, []byte("indexedValue1"))
	require.NoError(t, err)

	_, err = tx.Commit()
	require.NoError(t, err)

	valRef, err = immuStore.Get([]byte("indexedKey1"))
	require.NoError(t, err)
	require.NotNil(t, valRef)

	val, err = valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("indexedValue1"), val)
}

func TestImmudbStoreCommitWith(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open("data_commit_with", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_commit_with")

	require.NotNil(t, immuStore)
	defer immuStore.Close()

	_, err = immuStore.CommitWith(nil, false)
	require.ErrorIs(t, err, ErrIllegalArguments)

	callback := func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return nil, nil, nil
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Equal(t, ErrorNoEntriesProvided, err)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return nil, nil, errors.New("error")
	}
	_, err = immuStore.CommitWith(callback, false)
	require.Error(t, err)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: []byte("value")},
		}, nil, nil
	}

	hdr, err := immuStore.CommitWith(callback, true)
	require.NoError(t, err)

	require.Equal(t, uint64(1), immuStore.IndexInfo())

	_, err = immuStore.ReadValue(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	txHolder := immuStore.NewTxHolder()
	immuStore.ReadTx(hdr.ID, txHolder)

	entry, err := txHolder.EntryOf([]byte(fmt.Sprintf("keyInsertedAtTx%d", hdr.ID)))
	require.NoError(t, err)

	val, err := immuStore.ReadValue(entry)
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
	require.ErrorIs(t, err, tbtree.ErrCompactionThresholdNotReached)

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

						txIDs, hCount, err := snap.History(k, 0, false, txCount)
						if err != nil {
							panic(err)
						}
						if int(snap.Ts()) != len(txIDs) {
							panic(fmt.Errorf("expected %v actual %v", int(snap.Ts()), len(txIDs)))
						}
						if int(snap.Ts()) != int(hCount) {
							panic(fmt.Errorf("expected %v actual %v", int(snap.Ts()), hCount))
						}

						for _, txID := range txIDs {
							v := make([]byte, 8)
							binary.BigEndian.PutUint64(v, txID-1)

							err = immuStore.ReadTx(txID, tx)
							require.NoError(t, err)

							entry, err := tx.EntryOf(k)
							require.NoError(t, err)

							val, err := immuStore.ReadValue(entry)
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

		tx.WithMetadata(NewTxMetadata())

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

	_, err = immuStore.CommitWith(func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: nil},
		}, nil, nil
	}, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	immuStore, err = Open("data_inclusion_proof", opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, false, immuStore.NewTxHolder())
	require.NoError(t, err)

	for i := 0; i < txCount; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		entrySpecDigest, err := EntrySpecDigestFor(tx.header.Version)
		require.NoError(t, err)
		require.NotNil(t, entrySpecDigest)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for j, e := range txEntries {
			require.True(t, e.readonly)

			proof, err := tx.Proof(e.key())
			require.NoError(t, err)

			key := txEntries[j].key()

			ki, err := tx.IndexOf(key)
			require.NoError(t, err)
			require.Equal(t, j, ki)

			value := make([]byte, txEntries[j].vLen)
			_, err = immuStore.readValueAt(value, txEntries[j].VOff(), txEntries[j].HVal())
			require.NoError(t, err)

			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			require.Equal(t, k, key)
			require.Equal(t, v, value)

			eSpec := &EntrySpec{Key: key, Metadata: NewKVMetadata(), Value: value}

			verifies := htree.VerifyInclusion(proof, entrySpecDigest(eSpec), tx.header.Eh)
			require.True(t, verifies)

			v, err = immuStore.ReadValue(e)
			require.NoError(t, err)
			require.Equal(t, value, v)
		}
	}

	t.Run("reading value from non-readonly entry should fail", func(t *testing.T) {
		_, err = immuStore.ReadValue(NewTxEntry([]byte("key"), NewKVMetadata(), 0, sha256.Sum256(nil), 0))
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

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

		tx.WithMetadata(NewTxMetadata())

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

		entrySpecDigest, err := EntrySpecDigestFor(tx.header.Version)
		require.NoError(t, err)
		require.NotNil(t, entrySpecDigest)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for _, txe := range txEntries {
			proof, err := tx.Proof(txe.key())
			require.NoError(t, err)

			value := make([]byte, txe.vLen)
			_, err = immuStore.readValueAt(value, txe.vOff, txe.hVal)
			require.NoError(t, err)

			e := &EntrySpec{Key: txe.key(), Value: value}

			verifies := htree.VerifyInclusion(proof, entrySpecDigest(e), tx.header.Eh)
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
	require.ErrorIs(t, err, ErrIllegalArguments)
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

func TestImmudbStoreCommitWithPreconditions(t *testing.T) {
	immuStore, err := Open("preconditions_store", DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)

	defer immuStore.Close()
	defer os.RemoveAll("preconditions_store")

	// set initial value
	otx, err := immuStore.NewTx()
	require.NoError(t, err)

	err = otx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr1, err := otx.Commit()
	require.NoError(t, err)

	// delete entry
	otx, err = immuStore.NewTx()
	require.NoError(t, err)

	err = otx.Delete([]byte("key1"))
	require.NoError(t, err)

	_, err = otx.Commit()
	require.NoError(t, err)

	t.Run("must not exist constraint should pass when evaluated over a deleted key", func(t *testing.T) {
		otx, err := immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustNotExist{[]byte("key1")})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.NoError(t, err)
	})

	t.Run("must exist constraint should pass when evaluated over an existent key", func(t *testing.T) {
		otx, err := immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key3"), nil, []byte("value3"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustExist{[]byte("key2")})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.NoError(t, err)
	})

	t.Run("must not be modified after constraint should not pass when key is deleted after specified tx", func(t *testing.T) {
		otx, err := immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("key1"), TxID: hdr1.ID})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})

	t.Run("must not be modified after constraint should pass when if key does not exist", func(t *testing.T) {
		otx, err = immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("nonExistentKey"), TxID: 1})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.NoError(t, err)
	})

	// insert an expirable entry
	otx, err = immuStore.NewTx()
	require.NoError(t, err)

	md := NewKVMetadata()
	err = md.ExpiresAt(time.Now().Add(1 * time.Second))
	require.NoError(t, err)

	err = otx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
	require.NoError(t, err)

	hdr, err := otx.Commit()
	require.NoError(t, err)

	// wait for entry to be expired
	for i := 0; ; i++ {
		require.Less(t, i, 20, "entry expiration failed")

		time.Sleep(100 * time.Millisecond)

		_, err = immuStore.Get([]byte("expirableKey"))
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			break
		}

		require.NoError(t, err)
	}

	t.Run("must not be modified after constraint should not pass when if expired and expiration was set after specified tx", func(t *testing.T) {
		otx, err = immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("expirableKey"), TxID: hdr.ID - 1})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})

	t.Run("must not exist constraint should pass when if expired", func(t *testing.T) {
		otx, err = immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustNotExist{Key: []byte("expirableKey")})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.NoError(t, err)
	})

	t.Run("must exist constraint should not pass when if expired", func(t *testing.T) {
		otx, err = immuStore.NewTx()
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustExist{Key: []byte("expirableKey")})
		require.NoError(t, err)

		_, err = otx.Commit()
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})
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

func TestImmudbPrecodnitionIndexing(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_precondition_indexing")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	immuStore, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	t.Run("commit", func(t *testing.T) {

		// First add some entries that are not indexed
		immuStore.indexer.Pause()

		for i := 1; i < 100; i++ {
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("key_%d", i)), nil, []byte(fmt.Sprintf("value_%d", i)))
			require.NoError(t, err)

			_, err = tx.AsyncCommit()
			require.NoError(t, err)
		}

		// Next prepare transaction with preconditions - this must wait for the indexer
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set([]byte("key"), nil, []byte("value"))
		require.NoError(t, err)

		err = tx.AddPrecondition(&PreconditionKeyMustExist{
			Key: []byte("key_99"),
		})
		require.NoError(t, err)

		err = tx.AddPrecondition(&PreconditionKeyMustNotExist{
			Key: []byte("key_100"),
		})
		require.NoError(t, err)

		go func() {
			time.Sleep(100 * time.Millisecond)
			immuStore.indexer.Resume()
		}()

		_, err = tx.Commit()
		require.NoError(t, err)
	})

	t.Run("commitWith", func(t *testing.T) {

		// First add some entries that are not indexed
		immuStore.indexer.Pause()

		for i := 1; i < 100; i++ {
			tx, err := immuStore.NewWriteOnlyTx()
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("key2_%d", i)), nil, []byte(fmt.Sprintf("value2_%d", i)))
			require.NoError(t, err)

			_, err = tx.AsyncCommit()
			require.NoError(t, err)
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			immuStore.indexer.Resume()
		}()

		// Next prepare transaction with preconditions - this must wait for the indexer
		_, err = immuStore.CommitWith(func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
			return []*EntrySpec{{
					Key:   []byte("key2"),
					Value: []byte("value2"),
				}}, []Precondition{
					&PreconditionKeyMustExist{
						Key: []byte("key2_99"),
					},
					&PreconditionKeyMustNotExist{
						Key: []byte("key2_100"),
					},
				},
				nil
		}, false)
		require.NoError(t, err)
	})
}

func TestTimeBasedTxLookup(t *testing.T) {
	dir, err := ioutil.TempDir("", "test_time_based_tx_lookup")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	immuStore, err := Open(dir, DefaultOptions())
	require.NoError(t, err)

	start := time.Now()

	time.Sleep(1 * time.Second)

	_, err = immuStore.FirstTxSince(start)
	require.ErrorIs(t, err, ErrTxNotFound)

	_, err = immuStore.LastTxUntil(start)
	require.ErrorIs(t, err, ErrTxNotFound)

	var txts []int64

	const txCount = 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx()
		require.NoError(t, err)

		err = tx.Set([]byte("key1"), nil, []byte("val1"))
		require.NoError(t, err)

		hdr, err := tx.Commit()
		require.NoError(t, err)
		require.NotNil(t, hdr)

		txts = append(txts, hdr.Ts)

		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	t.Run("no tx should be returned when requesting a tx since a future time", func(t *testing.T) {
		_, err = immuStore.FirstTxSince(time.Now().Add(1 * time.Second))
		require.ErrorIs(t, err, ErrTxNotFound)
	})

	t.Run("the last tx should be returned when requesting a tx until a future time", func(t *testing.T) {
		tx, err := immuStore.LastTxUntil(time.Now().Add(1 * time.Second))
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Equal(t, uint64(txCount), tx.header.ID)
	})

	t.Run("the first tx should be returned when requesting from a past time", func(t *testing.T) {
		tx, err := immuStore.FirstTxSince(start)
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.Equal(t, uint64(1), tx.header.ID)
	})

	t.Run("no tx should be returned when requesting a tx until a past time", func(t *testing.T) {
		_, err = immuStore.LastTxUntil(start)
		require.ErrorIs(t, err, ErrTxNotFound)
	})

	for i, ts := range txts {
		tx, err := immuStore.FirstTxSince(time.Unix(ts, 0))
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.LessOrEqual(t, ts, tx.header.Ts)
		require.GreaterOrEqual(t, uint64(i+1), tx.header.ID)

		if tx.header.ID > 1 {
			require.Less(t, txts[tx.header.ID-2], ts)
		}

		tx, err = immuStore.LastTxUntil(time.Unix(ts, 0))
		require.NoError(t, err)
		require.NotNil(t, tx)
		require.GreaterOrEqual(t, ts, tx.header.Ts)

		if int(tx.header.ID) < len(txts) {
			require.Greater(t, txts[tx.header.ID], ts)
		}
	}
}
