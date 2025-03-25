/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded"
	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/mocked"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/tbtree"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func immustoreClose(t *testing.T, immuStore *ImmuStore) {
	if immuStore.IsClosed() {
		return
	}

	err := immuStore.Close()
	if !t.Failed() {
		require.NoError(t, err)
	}
}

func tempTxHolder(t *testing.T, immuStore *ImmuStore) *Tx {
	return NewTx(immuStore.maxTxEntries, immuStore.maxKeyLen)
}

func TestImmudbStoreConcurrency(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(4)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	txCount := 100
	eCount := 1000

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for i := 0; i < txCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			txhdr, err := tx.AsyncCommit(context.Background())
			require.NoError(t, err)
			require.EqualValues(t, i+1, txhdr.ID)
		}
	}()

	go func() {
		defer wg.Done()

		txID := uint64(1)

		for {
			time.Sleep(time.Duration(100) * time.Millisecond)

			txReader, err := immuStore.NewTxReader(txID, false, tempTxHolder(t, immuStore))
			require.NoError(t, err)

			for {
				time.Sleep(time.Duration(10) * time.Millisecond)

				tx, err := txReader.Read()
				if err == ErrNoMoreEntries {
					break
				}
				require.NoError(t, err)

				if tx.header.ID == uint64(txCount) {
					return
				}

				txID = tx.header.ID
			}
		}
	}()

	wg.Wait()
}

func TestImmudbStoreConcurrentCommits(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(5)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	txCount := 100
	eCount := 100

	var wg sync.WaitGroup
	wg.Add(10)

	txs := make([]*Tx, 10)
	for c := 0; c < 10; c++ {
		txs[c] = tempTxHolder(t, immuStore)
	}

	for c := 0; c < 10; c++ {
		go func(txHolder *Tx) {
			defer wg.Done()

			for c := 0; c < txCount; {
				tx, err := immuStore.NewWriteOnlyTx(context.Background())
				require.NoError(t, err)

				for j := 0; j < eCount; j++ {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(j))

					v := make([]byte, 8)
					binary.BigEndian.PutUint64(v, uint64(c))

					err = tx.Set(k, nil, v)
					require.NoError(t, err)
				}

				hdr, err := tx.AsyncCommit(context.Background())
				if err == ErrMaxConcurrencyLimitExceeded {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				require.NoError(t, err)

				err = immuStore.ReadTx(hdr.ID, false, txHolder)
				require.NoError(t, err)

				for _, e := range txHolder.Entries() {
					_, err := immuStore.ReadValue(e)
					require.NoError(t, err)
				}

				c++
			}
		}(txs[c])
	}

	wg.Wait()
}

func TestImmudbStoreConcurrentCommitsWithEmbeddedValues(t *testing.T) {
	opts := DefaultOptions().
		WithSynced(false).
		WithMaxConcurrency(5).
		WithEmbeddedValues(true).
		WithPreallocFiles(true).
		WithVLogCacheSize(10)

	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	txCount := 100
	eCount := 100

	var wg sync.WaitGroup
	wg.Add(10)

	txs := make([]*Tx, 10)
	for c := 0; c < 10; c++ {
		txs[c] = tempTxHolder(t, immuStore)
	}

	for c := 0; c < 10; c++ {
		go func(txHolder *Tx) {
			defer wg.Done()

			for c := 0; c < txCount; {
				tx, err := immuStore.NewWriteOnlyTx(context.Background())
				require.NoError(t, err)

				for j := 0; j < eCount; j++ {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(j))

					v := make([]byte, 8)
					binary.BigEndian.PutUint64(v, uint64(c))

					err = tx.Set(k, nil, v)
					require.NoError(t, err)
				}

				hdr, err := tx.AsyncCommit(context.Background())
				if err == ErrMaxConcurrencyLimitExceeded {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				require.NoError(t, err)

				err = immuStore.ReadTx(hdr.ID, false, txHolder)
				require.NoError(t, err)

				for _, e := range txHolder.Entries() {
					_, err := immuStore.ReadValue(e)
					require.NoError(t, err)
				}

				c++
			}
		}(txs[c])
	}

	wg.Wait()
}

func TestImmudbStoreOpenWithInvalidPath(t *testing.T) {
	_, err := Open("immustore_test.go", DefaultOptions())
	require.ErrorIs(t, err, ErrPathIsNotADirectory)
}

func TestImmudbStoreOnClosedStore(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)

	err = immuStore.ReadTx(1, false, nil)
	require.ErrorIs(t, err, ErrTxNotFound)

	err = immuStore.Close()
	require.NoError(t, err)

	err = immuStore.Close()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.Sync()
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.FlushIndexes(100, true)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = immuStore.commit(context.Background(), &OngoingTx{entries: []*EntrySpec{
		{Key: []byte("key1")},
	}}, nil, false, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	err = immuStore.ReadTx(1, false, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.ErrorIs(t, err, ErrAlreadyClosed)
}

func TestImmudbStoreSettings(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.Equal(t, DefaultOptions().ReadOnly, immuStore.ReadOnly())
	require.Equal(t, DefaultOptions().Synced, immuStore.Synced())
	require.Equal(t, 1, immuStore.MaxConcurrency())
	require.Equal(t, DefaultOptions().MaxIOConcurrency, immuStore.MaxIOConcurrency())
	require.Equal(t, DefaultOptions().MaxActiveTransactions, immuStore.MaxActiveTransactions())
	require.Equal(t, DefaultOptions().MVCCReadSetLimit, immuStore.MVCCReadSetLimit())
	require.Equal(t, DefaultOptions().MaxTxEntries, immuStore.MaxTxEntries())
	require.Equal(t, DefaultOptions().MaxKeyLen, immuStore.MaxKeyLen())
	require.Equal(t, DefaultOptions().MaxValueLen, immuStore.MaxValueLen())
}

func TestImmudbStoreWithTimeFunction(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	err = immuStore.UseTimeFunc(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	fixedTime := time.Now()

	// add some delay to ensure time has passed
	time.Sleep(10 * time.Microsecond)

	err = immuStore.UseTimeFunc(func() time.Time {
		return fixedTime
	})
	require.NoError(t, err)

	tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr, err := tx.Commit(context.Background())
	require.NoError(t, err)
	require.Equal(t, fixedTime.Unix(), hdr.Ts)
}

func TestImmudbStoreEdgeCases(t *testing.T) {
	t.Run("should fail with invalid options", func(t *testing.T) {
		_, err := Open(t.TempDir(), nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("should fail with invalid appendables", func(t *testing.T) {
		_, err := OpenWith(t.TempDir(), nil, nil, nil, DefaultOptions())
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("should fail with invalid appendables and invalid options", func(t *testing.T) {
		_, err := OpenWith(t.TempDir(), nil, nil, nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

	t.Run("should fail with invalid dir name", func(t *testing.T) {
		_, err := Open("invalid\x00_dir_name", DefaultOptions())
		require.EqualError(t, err, "stat invalid\x00_dir_name: invalid argument")
	})

	t.Run("should fail with permission denied", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ro_path")
		require.NoError(t, os.MkdirAll(path, 0500))

		_, err := Open(filepath.Join(path, "subpath"), DefaultOptions())
		require.ErrorContains(t, err, "subpath: permission denied")
	})

	t.Run("should fail when initiating appendables", func(t *testing.T) {
		for _, failedAppendable := range []string{"tx", "commit", "val_0"} {
			injectedError := fmt.Errorf("Injected error for: %s", failedAppendable)
			_, err := Open(t.TempDir(), DefaultOptions().
				WithEmbeddedValues(false).
				WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
					if subPath == failedAppendable {
						return nil, injectedError
					}
					return &mocked.MockedAppendable{
						MetadataFn: func() []byte { return nil },
					}, nil
				}))
			require.ErrorIs(t, err, injectedError)
		}
	})

	// basic appendable initialization
	vLog := &mocked.MockedAppendable{
		CloseFn: func() error { return nil },
	}

	vLogs := []appendable.Appendable{vLog}

	txLog := &mocked.MockedAppendable{
		CloseFn: func() error { return nil },
		ReadAtFn: func(bs []byte, off int64) (int, error) {
			return 0, io.EOF
		},
	}

	cLog := &mocked.MockedAppendable{
		MetadataFn: func() []byte { return nil },
		CloseFn:    func() error { return nil },
		AppendFn:   func(bs []byte) (off int64, n int, err error) { return 0, len(bs), nil },
		FlushFn:    func() error { return nil },
		SyncFn:     func() error { return nil },
	}

	t.Run("should fail reading fileSize from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			return nil
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, ErrCorruptedCLog)
	})

	t.Run("should fail reading maxTxEntries from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(metaVersion, 1)
			md.PutInt(metaFileSize, 1)
			return md.Bytes()
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, ErrCorruptedCLog)
	})

	t.Run("should fail reading maxKeyLen from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(metaVersion, 1)
			md.PutInt(metaFileSize, 1)
			md.PutInt(metaMaxTxEntries, 4)
			return md.Bytes()
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, ErrCorruptedCLog)
	})

	t.Run("should fail reading maxKeyLen from metadata", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(metaVersion, 1)
			md.PutInt(metaFileSize, 1)
			md.PutInt(metaMaxTxEntries, 4)
			md.PutInt(metaMaxKeyLen, 8)
			return md.Bytes()
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, ErrCorruptedCLog)
	})

	t.Run("should fail reading cLogSize", func(t *testing.T) {
		cLog.MetadataFn = func() []byte {
			md := appendable.NewMetadata(nil)
			md.PutInt(metaVersion, 1)
			md.PutInt(metaFileSize, 1)
			md.PutInt(metaMaxTxEntries, 4)
			md.PutInt(metaMaxKeyLen, 8)
			md.PutInt(metaMaxValueLen, 16)
			return md.Bytes()
		}

		injectedError := errors.New("error")

		cLog.SizeFn = func() (int64, error) {
			return 0, injectedError
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	injectedError := errors.New("error")

	t.Run("should fail setting cLog offset", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1 - 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return injectedError
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("should truncate cLog when validating cLogSize", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1 - 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return nil
		}

		st, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.NoError(t, err)

		err = st.Close()
		require.NoError(t, err)
	})

	t.Run("should fail reading cLog", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1, nil
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedError
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("should fail reading txLogSize", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1 + 1, nil
		}
		cLog.SetOffsetFn = func(off int64) error {
			return nil
		}
		txLog.SizeFn = func() (int64, error) {
			return 0, injectedError
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("should fail reading txLogSize", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1, nil
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

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("should fail validating txLogSize", func(t *testing.T) {
		cLog.SizeFn = func() (int64, error) {
			return cLogEntrySizeV1, nil
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

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, ErrCorruptedTxData)
	})

	t.Run("fail to read last transaction", func(t *testing.T) {
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			buff := []byte{0, 0, 0, 0, 0, 0, 0, 1}
			require.Less(t, off, int64(len(buff)))
			return copy(bs, buff[off:]), nil
		}
		txLog.SizeFn = func() (int64, error) {
			return 1, nil
		}
		txLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, injectedError
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("fail to initialize aht when opening appendable", func(t *testing.T) {
		txLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			return 0, io.EOF
		}

		cLog.SizeFn = func() (int64, error) {
			return 0, nil
		}

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog,
			DefaultOptions().
				WithEmbeddedValues(false).
				WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
					if strings.HasPrefix(subPath, "aht/") {
						return nil, injectedError
					}
					return &mocked.MockedAppendable{}, nil
				}),
		)
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("fail to initialize indexer", func(t *testing.T) {
		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog,
			DefaultOptions().
				WithEmbeddedValues(false).
				WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
					if strings.HasSuffix(rootPath, "index") {
						return nil, injectedError
					}
					return &mocked.MockedAppendable{
						SizeFn:      func() (int64, error) { return 0, nil },
						CloseFn:     func() error { return nil },
						SetOffsetFn: func(off int64) error { return nil },
					}, nil
				}),
		)
		require.ErrorIs(t, err, injectedError)
	})

	t.Run("incorrect tx in indexer", func(t *testing.T) {
		vLog.CloseFn = func() error { return nil }
		txLog.CloseFn = func() error { return nil }
		cLog.CloseFn = func() error { return nil }

		_, err := OpenWith(t.TempDir(), vLogs, txLog, cLog,
			DefaultOptions().
				WithEmbeddedValues(false).
				WithAppFactory(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
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
					case "nodes":
						return nLog, nil
					case "history":
						return hLog, nil
					case "commit":
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
		require.ErrorIs(t, err, ErrCorruptedIndex)
	})

	mockedApps := []*mocked.MockedAppendable{vLog, txLog, cLog}
	for _, app := range mockedApps {
		app.SyncFn = func() error { return nil }
	}

	t.Run("errors during sync", func(t *testing.T) {
		// Errors during sync
		vLog.AppendFn = func(bs []byte) (off int64, n int, err error) { return 0, len(bs), nil }
		vLog.FlushFn = func() error { return nil }

		txLog.AppendFn = func(bs []byte) (off int64, n int, err error) { return 0, len(bs), nil }
		txLog.SetOffsetFn = func(off int64) error { return nil }
		txLog.FlushFn = func() error { return nil }

		cLogBuf := bytes.NewBuffer(nil)
		cLog.AppendFn = func(bs []byte) (off int64, n int, err error) {
			off = int64(cLogBuf.Len())
			n, err = cLogBuf.Write(bs)
			return
		}
		cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
			buf := cLogBuf.Bytes()
			copy(bs, buf[off:])
			return len(buf) - int(off), nil
		}

		for i, checkApp := range mockedApps {
			injectedError = fmt.Errorf("Injected error %d", i)
			checkApp.SyncFn = func() error { return injectedError }

			opts := DefaultOptions().
				WithEmbeddedValues(false).
				WithSyncFrequency(time.Duration(1) * time.Second)

			store, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, opts)
			require.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				tx, err := store.NewWriteOnlyTx(context.Background())
				require.NoError(t, err)

				err = tx.Set([]byte("key"), nil, []byte("value"))
				require.NoError(t, err)

				_, err = tx.AsyncCommit(context.Background())
				require.ErrorIs(t, err, ErrAlreadyClosed)

				wg.Done()
			}()

			// wait for the tx to be waiting for sync to happen
			err = store.inmemPrecommitWHub.WaitFor(context.Background(), 1)
			require.NoError(t, err)

			err = store.Sync()
			require.ErrorIs(t, err, injectedError)

			err = store.Close()
			require.NoError(t, err)

			wg.Wait()

			checkApp.SyncFn = func() error { return nil }
		}
	})

	// Errors during close
	store, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
	require.NoError(t, err)

	err = store.aht.Close()
	require.NoError(t, err)

	err = store.Close()
	require.ErrorIs(t, err, ahtree.ErrAlreadyClosed)

	for i, checkApp := range mockedApps {
		injectedError = fmt.Errorf("Injected error %d", i)
		checkApp.CloseFn = func() error { return injectedError }

		store, err := OpenWith(t.TempDir(), vLogs, txLog, cLog, DefaultOptions().WithEmbeddedValues(false))
		require.NoError(t, err)

		err = store.Close()
		require.ErrorIs(t, err, injectedError)

		checkApp.CloseFn = func() error { return nil }
	}

	opts := DefaultOptions().
		WithMaxConcurrency(1).
		WithEmbeddedValues(false)

	immuStore, err := Open(t.TempDir(), opts)
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
	require.ErrorIs(t, err, ErrMaxConcurrencyLimitExceeded)

	immuStore.releaseAllocTx(tx1)

	_, err = immuStore.NewTxReader(1, false, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = immuStore.DualProof(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = immuStore.DualProofV2(nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	sourceTx := NewTx(1, 1)
	sourceTx.header.ID = 2
	targetTx := NewTx(1, 1)
	targetTx.header.ID = 1

	_, err = immuStore.DualProof(sourceTx.Header(), targetTx.Header())
	require.ErrorIs(t, err, ErrSourceTxNewerThanTargetTx)

	_, err = immuStore.DualProofV2(sourceTx.Header(), targetTx.Header())
	require.ErrorIs(t, err, ErrSourceTxNewerThanTargetTx)

	_, err = immuStore.LinearProof(2, 1)
	require.ErrorIs(t, err, ErrSourceTxNewerThanTargetTx)

	_, err = sourceTx.EntryOf([]byte{1, 2, 3})
	require.ErrorIs(t, err, ErrKeyNotFound)

	t.Run("validateEntries", func(t *testing.T) {
		err = immuStore.validateEntries(make([]*EntrySpec, immuStore.maxTxEntries+1))
		require.ErrorIs(t, err, ErrMaxTxEntriesLimitExceeded)

		entry := &EntrySpec{Key: nil, Value: nil}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrNullKey)

		entry = &EntrySpec{Key: make([]byte, immuStore.maxKeyLen+1), Value: make([]byte, 1)}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrMaxKeyLenExceeded)

		entry = &EntrySpec{Key: make([]byte, 1), Value: make([]byte, immuStore.maxValueLen+1)}
		err = immuStore.validateEntries([]*EntrySpec{entry})
		require.ErrorIs(t, err, ErrMaxValueLenExceeded)
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

func TestImmudbTxOffsetAndSize(t *testing.T) {
	opts := DefaultOptions().WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	immuStore.mutex.Lock()
	defer immuStore.mutex.Unlock()

	_, _, err = immuStore.txOffsetAndSize(0)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestImmudbStoreIndexing(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			defer wg.Done()
			for {
				txID, _ := immuStore.CommittedAlh()

				snap, err := immuStore.SnapshotMustIncludeTxID(context.Background(), nil, txID)
				require.NoError(t, err)

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						v := make([]byte, 8)
						binary.BigEndian.PutUint64(v, snap.Ts()-1)

						valRef, err := snap.Get(context.Background(), k)
						if err != nil {
							require.ErrorIs(t, err, tbtree.ErrKeyNotFound)
							continue
						}

						val, err := valRef.Resolve()
						require.NoError(t, err)
						require.Equal(t, v, val)
					}
				}

				if snap.Ts() == uint64(txCount) {
					k := make([]byte, 8)
					binary.BigEndian.PutUint64(k, uint64(eCount-1))

					_, valRef1, err := immuStore.GetWithPrefix(context.Background(), k, nil)
					require.NoError(t, err)

					v1, err := valRef1.Resolve()
					require.NoError(t, err)

					valRef2, err := snap.Get(context.Background(), k)
					require.NoError(t, err)

					v2, err := valRef2.Resolve()
					require.NoError(t, err)
					require.Equal(t, v1, v2)
					require.Equal(t, valRef1.Tx(), valRef2.Tx())

					txs, hCount, err := immuStore.History(k, 0, false, txCount)
					require.NoError(t, err)
					require.Equal(t, txCount, len(txs))
					require.Equal(t, txCount, int(hCount))

					snap.Close()
					break
				}

				snap.Close()
				time.Sleep(time.Duration(100) * time.Millisecond)
			}
		}()
	}

	wg.Wait()

	if t.Failed() {
		return
	}

	err = immuStore.FlushIndexes(-10, true)
	require.ErrorIs(t, err, tbtree.ErrIllegalArguments)

	err = immuStore.FlushIndexes(100, true)
	require.NoError(t, err)

	t.Run("latest set value should be committed", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		err = tx.Set([]byte("key"), nil, []byte("value1"))
		require.NoError(t, err)

		err = tx.Set([]byte("key"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		val, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value2"), val)

		key, _, err := immuStore.GetWithPrefix(context.Background(), []byte("k"), []byte("k"))
		require.NoError(t, err)
		require.Equal(t, []byte("key"), key)

		_, err = immuStore.GetBetween(context.Background(), []byte("key"), 1, valRef.Tx())
		require.NoError(t, err)
	})
}

func TestImmudbStoreRWTransactions(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	t.Run("after closing write-only tx edge cases", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		require.Nil(t, tx.Metadata())

		err = tx.Set(nil, nil, []byte{3, 2, 1})
		require.ErrorIs(t, err, ErrNullKey)

		err = tx.Set(make([]byte, immuStore.maxKeyLen+1), nil, []byte{3, 2, 1})
		require.ErrorIs(t, err, ErrMaxKeyLenExceeded)

		err = tx.Set([]byte{1, 2, 3}, nil, make([]byte, immuStore.maxValueLen+1))
		require.ErrorIs(t, err, ErrMaxValueLenExceeded)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1})
		require.NoError(t, err)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1, 0})
		require.NoError(t, err)

		_, err = tx.Get(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, _, err = tx.GetWithPrefix(context.Background(), []byte{1}, []byte{1})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		err = tx.Delete(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, err = tx.NewKeyReader(KeyReaderSpec{})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1, 0})
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.NewKeyReader(KeyReaderSpec{})
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.Commit(context.Background())
		require.ErrorIs(t, err, ErrAlreadyClosed)

		err = tx.Cancel()
		require.ErrorIs(t, err, ErrAlreadyClosed)
	})

	t.Run("cancelled transaction should not produce effects", func(t *testing.T) {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		_, err = tx.Get(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrWriteOnlyTx)

		err = tx.Cancel()
		require.NoError(t, err)

		err = tx.Cancel()
		require.ErrorIs(t, err, ErrAlreadyClosed)

		_, err = tx.Commit(context.Background())
		require.ErrorIs(t, err, ErrAlreadyClosed)

		valRef, err := immuStore.Get(context.Background(), []byte{1, 2, 3})
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
		_, err := immuStore.Get(context.Background(), []byte("key1"))
		require.ErrorIs(t, err, embedded.ErrKeyNotFound)

		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		_, err = tx.Get(context.Background(), []byte("key1"))
		require.ErrorIs(t, err, embedded.ErrKeyNotFound)

		err = tx.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx.GetWithFilters(context.Background(), []byte("key1"), nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, _, err = tx.GetWithPrefixAndFilters(context.Background(), []byte("key1"), nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		key, valRef, err := tx.GetWithPrefix(context.Background(), []byte("key1"), []byte("key"))
		require.NoError(t, err)
		require.NotNil(t, key)
		require.NotNil(t, valRef)

		_, _, err = tx.GetWithPrefix(context.Background(), []byte("key1"), []byte("key1"))
		require.ErrorIs(t, err, embedded.ErrKeyNotFound)

		r, err := tx.NewKeyReader(KeyReaderSpec{Prefix: []byte("key")})
		require.NoError(t, err)
		require.NotNil(t, r)

		k, _, err := r.Read(context.Background())
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), k)

		_, err = tx.Commit(context.Background())
		require.ErrorIs(t, err, tbtree.ErrReadersNotClosed)

		err = r.Close()
		require.NoError(t, err)

		valRef, err = tx.Get(context.Background(), []byte("key1"))
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

		_, err = immuStore.Get(context.Background(), []byte("key1"))
		require.ErrorIs(t, err, embedded.ErrKeyNotFound)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		_, _, err = tx.GetWithPrefix(context.Background(), []byte("key1"), []byte("key1"))
		require.ErrorIs(t, err, ErrAlreadyClosed)

		valRef, err = immuStore.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(2), valRef.Tx())

		v, err = valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), v)
	})

	t.Run("second ongoing tx after the first commit should succeed", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(4), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx2"), v)
	})

	t.Run("second ongoing tx with multiple entries after the first commit should succeed", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key2"), nil, []byte("value2_tx2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(6), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx2"), v)
	})

	t.Run("second ongoing tx after the first cancellation should succeed", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1_tx1"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		err = tx1.Cancel()
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.Equal(t, uint64(7), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx2"), v)
	})

	t.Run("deleted keys should not be reachable", func(t *testing.T) {
		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx.Delete(context.Background(), []byte{1, 2, 3})
		require.NoError(t, err)

		err = tx.Delete(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)

		r, err := tx.NewKeyReader(KeyReaderSpec{
			Prefix:  []byte{1, 2, 3},
			Filters: []FilterFn{IgnoreDeleted},
		})
		require.NoError(t, err)
		require.NotNil(t, r)

		_, _, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrNoMoreEntries)

		err = r.Close()
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		_, err = immuStore.Get(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = immuStore.GetWithFilters(context.Background(), []byte{1, 2, 3}, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		_, _, err = immuStore.GetWithPrefixAndFilters(context.Background(), []byte{1, 2, 3}, nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)

		valRef, err := immuStore.GetWithFilters(context.Background(), []byte{1, 2, 3})
		require.NoError(t, err)
		require.NotNil(t, valRef)
		require.True(t, valRef.KVMetadata().Deleted())
		require.NotNil(t, valRef.KVMetadata())
		require.False(t, valRef.KVMetadata().IsExpirable())

		tx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()

		r, err = tx.NewKeyReader(KeyReaderSpec{
			Prefix:  []byte{1, 2, 3},
			Filters: []FilterFn{IgnoreDeleted},
		})
		require.NoError(t, err)
		require.NotNil(t, r)

		_, _, err = r.ReadBetween(context.Background(), 1, immuStore.TxCount())
		require.ErrorIs(t, err, ErrNoMoreEntries)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("non-expired keys should be reachable", func(t *testing.T) {
		nearFuture := time.Now().Add(2 * time.Second)

		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		md := NewKVMetadata()
		err = md.ExpiresAt(nearFuture)
		require.NoError(t, err)

		err = tx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte("expirableKey"))
		require.NoError(t, err)
		require.NotNil(t, valRef)

		val, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("expirableValue"), val)

		time.Sleep(2 * time.Second)

		// already expired
		_, err = immuStore.Get(context.Background(), []byte("expirableKey"))
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)

		// expired entries can not be resolved
		valRef, err = immuStore.GetWithFilters(context.Background(), []byte("expirableKey"))
		require.NoError(t, err)
		_, err = valRef.Resolve()
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)

		// expired entries are not returned
		_, err = immuStore.GetWithFilters(context.Background(), []byte("expirableKey"), IgnoreExpired)
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)
	})

	t.Run("expired keys should not be reachable", func(t *testing.T) {
		now := time.Now()

		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		md := NewKVMetadata()
		err = md.ExpiresAt(now)
		require.NoError(t, err)

		err = tx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		// already expired
		_, err = immuStore.Get(context.Background(), []byte("expirableKey"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		// expired entries can not be resolved
		valRef, err := immuStore.GetWithFilters(context.Background(), []byte("expirableKey"))
		require.NoError(t, err)
		_, err = valRef.Resolve()
		require.ErrorIs(t, err, ErrKeyNotFound)
		require.ErrorIs(t, err, ErrExpiredEntry)
	})

	t.Run("transactions should not read data from anothers committed or ongoing transactions since it was created", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		err = tx1.Delete(context.Background(), []byte("key2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value1_tx2"))
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		_, err = tx3.Get(context.Background(), []byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		// ongoing tranactions should not read committed entries since their creation
		tx11, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx11.Set([]byte("key1"), nil, []byte("value1_tx11"))
		require.NoError(t, err)

		_, err = tx11.Commit(context.Background())
		require.NoError(t, err)
		//

		_, err = tx3.Get(context.Background(), []byte("key1"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		err = tx3.Set([]byte("key1"), nil, []byte("value1_tx3"))
		require.NoError(t, err)

		hdr2, err := tx2.Commit(context.Background())
		require.NoError(t, err)

		valRef2, err := immuStore.Get(context.Background(), []byte("key1"))
		require.NoError(t, err)
		require.NotNil(t, valRef2)
		require.Equal(t, hdr2.ID, valRef2.Tx())

		v2, err := valRef2.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte("value1_tx2"), v2)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})
}

func TestImmudbStoreKVMetadata(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
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

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	_, err = immuStore.Get(context.Background(), []byte{1, 2, 3})
	require.ErrorIs(t, err, ErrKeyNotFound)

	valRef, err := immuStore.GetWithFilters(context.Background(), []byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), valRef.Tx())
	require.True(t, valRef.KVMetadata().Deleted())
	require.Equal(t, uint64(1), valRef.HC())
	require.Equal(t, uint32(3), valRef.Len())
	require.Equal(t, sha256.Sum256([]byte{3, 2, 1}), valRef.HVal())
	require.Nil(t, valRef.TxMetadata())

	v, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte{3, 2, 1}, v)

	t.Run("read deleted key from snapshot should return key not found", func(t *testing.T) {
		snap, err := immuStore.Snapshot(nil)
		require.NoError(t, err)
		require.NotNil(t, snap)
		defer snap.Close()

		_, err = snap.Get(context.Background(), []byte{1, 2, 3})
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	tx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, err = tx.Get(context.Background(), []byte{1, 2, 3})
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tx.Set([]byte{1, 2, 3}, nil, []byte{1, 1, 1})
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)
	valRef, err = immuStore.Get(context.Background(), []byte{1, 2, 3})
	require.NoError(t, err)
	require.Equal(t, uint64(2), valRef.Tx())

	v, err = valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1, 1}, v)
}

func TestImmudbStoreNonIndexableEntries(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)
	require.NotNil(t, tx)

	md := NewKVMetadata()
	err = md.AsNonIndexable(true)
	require.NoError(t, err)

	err = tx.Set([]byte("nonIndexedKey"), md, []byte("nonIndexedValue"))
	require.NoError(t, err)

	err = tx.Set([]byte("indexedKey"), nil, []byte("indexedValue"))
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	_, err = immuStore.Get(context.Background(), []byte("nonIndexedKey"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	valRef, err := immuStore.Get(context.Background(), []byte("indexedKey"))
	require.NoError(t, err)
	require.NotNil(t, valRef)

	val, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("indexedValue"), val)

	// commit tx with all non-indexable entries
	tx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Set([]byte("nonIndexedKey1"), md, []byte("nonIndexedValue1"))
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	_, err = immuStore.Get(context.Background(), []byte("nonIndexedKey1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	// commit simple tx with an indexable entry
	tx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Set([]byte("indexedKey1"), nil, []byte("indexedValue1"))
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	valRef, err = immuStore.Get(context.Background(), []byte("indexedKey1"))
	require.NoError(t, err)
	require.NotNil(t, valRef)

	val, err = valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("indexedValue1"), val)
}

func TestImmudbStoreCommitWith(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	_, err = immuStore.CommitWith(context.Background(), nil, false)
	require.ErrorIs(t, err, ErrIllegalArguments)

	callback := func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return nil, nil, nil
	}
	_, err = immuStore.CommitWith(context.Background(), callback, false)
	require.ErrorIs(t, err, ErrNoEntriesProvided)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return nil, nil, errors.New("error")
	}
	_, err = immuStore.CommitWith(context.Background(), callback, false)
	require.Error(t, err)

	callback = func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: []byte("value")},
		}, nil, nil
	}

	hdr, err := immuStore.CommitWith(context.Background(), callback, true)
	require.NoError(t, err)

	_, err = immuStore.ReadValue(nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	tx, err := immuStore.fetchAllocTx()
	require.NoError(t, err)
	defer immuStore.releaseAllocTx(tx)

	immuStore.ReadTx(hdr.ID, false, tx)

	entry, err := tx.EntryOf([]byte(fmt.Sprintf("keyInsertedAtTx%d", hdr.ID)))
	require.NoError(t, err)

	val, err := immuStore.ReadValue(entry)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
}

func TestImmudbStoreHistoricalValues(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	opts.WithIndexOptions(opts.IndexOpts.WithFlushThld(10))

	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	defer immustoreClose(t, immuStore)

	txCount := 10
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.Commit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	err = immuStore.CompactIndexes()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			for {
				snap, err := immuStore.Snapshot(nil)
				require.NoError(t, err)

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						valRefs, hCount, err := snap.History(k, 0, false, txCount)
						require.NoError(t, err)
						require.EqualValues(t, snap.Ts(), len(valRefs))
						require.EqualValues(t, snap.Ts(), hCount)

						for _, valRef := range valRefs {
							v := make([]byte, 8)
							binary.BigEndian.PutUint64(v, valRef.Tx()-1)

							val, err := valRef.Resolve()
							require.NoError(t, err)
							require.Equal(t, v, val)
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
}

func TestImmudbStoreCompapactionDisabled(t *testing.T) {
	opts := DefaultOptions().WithCompactionDisabled(true)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	err = immuStore.CompactIndexes()
	require.ErrorIs(t, err, ErrCompactionDisabled)
}

func TestImmudbStoreInclusionProof(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	txCount := 100
	eCount := 100

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
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

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, err = immuStore.CommitWith(context.Background(), func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
		return []*EntrySpec{
			{Key: []byte(fmt.Sprintf("keyInsertedAtTx%d", txID)), Value: nil},
		}, nil, nil
	}, false)
	require.ErrorIs(t, err, ErrAlreadyClosed)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	tx := tempTxHolder(t, immuStore)

	r, err := immuStore.NewTxReader(1, false, tx)
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
			_, err = immuStore.readValueAt(value, txEntries[j].VOff(), txEntries[j].HVal(), false)
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
}

func TestLeavesMatchesAHTSync(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)

		err = immuStore.WaitForTx(context.Background(), txhdr.ID, false)
		require.NoError(t, err)

		err = immuStore.WaitForIndexingUpto(context.Background(), txhdr.ID)
		require.NoError(t, err)

		var k0 [8]byte
		_, _, err = immuStore.GetWithPrefix(context.Background(), k0[:], nil)
		require.NoError(t, err)
	}

	tx := tempTxHolder(t, immuStore)

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), false, tx)
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
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	tx := tempTxHolder(t, immuStore)

	for i := 0; i < txCount; i++ {
		err := immuStore.ReadTx(uint64(i+1), false, tx)
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
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.NotNil(t, immuStore)

	txCount := 16
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
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

		txhdr, err := tx.Commit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	sourceTx := tempTxHolder(t, immuStore)
	targetTx := tempTxHolder(t, immuStore)

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, false, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		for j := i; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, false, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.header.ID)

			dproof, err := immuStore.DualProof(sourceTx.Header(), targetTx.Header())
			require.NoError(t, err)

			verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.True(t, verifies)

			dproofV2, err := immuStore.DualProofV2(sourceTx.Header(), targetTx.Header())
			require.NoError(t, err)

			verifiesV2 := VerifyDualProofV2(dproofV2, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.NoError(t, verifiesV2)
		}
	}
}

func TestImmudbStoreConsistencyProofAgainstLatest(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 10

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)
	}

	sourceTx := tempTxHolder(t, immuStore)
	targetTx := tempTxHolder(t, immuStore)

	targetTxID := uint64(txCount)
	err = immuStore.ReadTx(targetTxID, false, targetTx)
	require.NoError(t, err)
	require.Equal(t, uint64(txCount), targetTx.header.ID)

	for i := 0; i < txCount-1; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, false, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		dproof, err := immuStore.DualProof(sourceTx.Header(), targetTx.Header())
		require.NoError(t, err)

		verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
		require.True(t, verifies)

		dproofV2, err := immuStore.DualProofV2(sourceTx.Header(), targetTx.Header())
		require.NoError(t, err)

		verifiesV2 := VerifyDualProofV2(dproofV2, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
		require.NoError(t, verifiesV2)
	}
}

func TestImmudbStoreConsistencyProofReopened(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)
	require.NotNil(t, immuStore)

	txCount := 16
	eCount := 100

	tx, err := immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.ErrorIs(t, err, ErrNoEntriesProvided)

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.AsyncCommit(context.Background())
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txhdr.ID)

		currentID, currentAlh := immuStore.CommittedAlh()
		require.Equal(t, txhdr.ID, currentID)
		require.Equal(t, txhdr.Alh(), currentAlh)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	os.RemoveAll(filepath.Join(dir, "aht"))

	immuStore, err = Open(dir, opts.WithMaxValueLen(opts.MaxValueLen-1))
	require.NoError(t, err)

	txholder := tempTxHolder(t, immuStore)

	for i := 0; i < txCount; i++ {
		txID := uint64(i + 1)

		ri, err := immuStore.NewTxReader(txID, false, txholder)
		require.NoError(t, err)

		txi, err := ri.Read()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txi.header.ID)
	}

	sourceTx := tempTxHolder(t, immuStore)
	targetTx := tempTxHolder(t, immuStore)

	for i := 0; i < txCount; i++ {
		sourceTxID := uint64(i + 1)

		err := immuStore.ReadTx(sourceTxID, false, sourceTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), sourceTx.header.ID)

		for j := i + 1; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, false, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.header.ID)

			lproof, err := immuStore.LinearProof(sourceTxID, targetTxID)
			require.NoError(t, err)

			verifies := VerifyLinearProof(lproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.True(t, verifies)

			dproof, err := immuStore.DualProof(sourceTx.Header(), targetTx.Header())
			require.NoError(t, err)

			verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.True(t, verifies)

			dproofV2, err := immuStore.DualProofV2(sourceTx.Header(), targetTx.Header())
			require.NoError(t, err)

			verifiesV2 := VerifyDualProofV2(dproofV2, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
			require.NoError(t, verifiesV2)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestReOpeningImmudbStore(t *testing.T) {
	dir := t.TempDir()

	itCount := 3
	txCount := 100
	eCount := 10

	for it := 0; it < itCount; it++ {
		opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
		immuStore, err := Open(dir, opts)
		require.NoError(t, err)

		for i := 0; i < txCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			txhdr, err := tx.AsyncCommit(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txhdr.ID)
		}

		err = immuStore.Close()
		require.NoError(t, err)
	}
}

func TestReOpeningWithCompressionEnabledImmudbStore(t *testing.T) {
	dir := t.TempDir()

	itCount := 3
	txCount := 100
	eCount := 10

	for it := 0; it < itCount; it++ {
		opts := DefaultOptions().
			WithSynced(false).
			WithCompressionFormat(appendable.GZipCompression).
			WithCompresionLevel(appendable.DefaultCompression).
			WithMaxConcurrency(1)

		immuStore, err := Open(dir, opts)
		require.NoError(t, err)

		for i := 0; i < txCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			txhdr, err := tx.AsyncCommit(context.Background())
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), txhdr.ID)
		}

		err = immuStore.Close()
		require.NoError(t, err)
	}
}

func TestUncommittedTxOverwriting(t *testing.T) {
	path := t.TempDir()

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithMaxConcurrency(3)

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaVersion, Version)
	metadata.PutBool(metaEmbeddedValues, false)
	metadata.PutBool(metaPreallocFiles, false)
	metadata.PutInt(metaFileSize, opts.FileSize)
	metadata.PutInt(metaMaxTxEntries, opts.MaxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.MaxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.MaxValueLen)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithRetryableSync(opts.Synced).
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

	txHolder := tempTxHolder(t, immuStore)

	txReader, err := immuStore.NewTxReader(1, false, txHolder)
	require.NoError(t, err)

	_, err = txReader.Read()
	require.ErrorIs(t, err, ErrNoMoreEntries)

	txCount := 100
	eCount := 64

	emulatedFailures := 0

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(j+1))

			err = tx.Set(k, nil, v)
			require.NoError(t, err)
		}

		txhdr, err := tx.Commit(context.Background())
		if err != nil {
			require.ErrorIs(t, err, errEmulatedAppendableError)
			emulatedFailures++
		} else {
			require.Equal(t, uint64(i+1-emulatedFailures), txhdr.ID)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(path, opts)
	require.NoError(t, err)

	txHolder = tempTxHolder(t, immuStore)

	r, err := immuStore.NewTxReader(1, false, txHolder)
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
			_, err = immuStore.readValueAt(value, txe.vOff, txe.hVal, false)
			require.NoError(t, err)

			e := &EntrySpec{Key: txe.key(), Value: value}

			verifies := htree.VerifyInclusion(proof, entrySpecDigest(e), tx.header.Eh)
			require.True(t, verifies)
		}
	}

	_, err = r.Read()
	require.ErrorIs(t, err, ErrNoMoreEntries)

	require.Equal(t, uint64(txCount-emulatedFailures), immuStore.TxCount())

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestExportAndReplicateTx(t *testing.T) {
	primaryDir := t.TempDir()

	primaryStore, err := Open(primaryDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, primaryStore)

	replicaDir := t.TempDir()

	replicaStore, err := Open(replicaDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, replicaStore)

	tx, err := primaryStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	tx.WithMetadata(NewTxMetadata())

	err = tx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr, err := tx.Commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, hdr)

	txholder := tempTxHolder(t, primaryStore)

	etx, err := primaryStore.ExportTx(1, false, false, txholder)
	require.NoError(t, err)

	rhdr, err := replicaStore.ReplicateTx(context.Background(), etx, false, false)
	require.NoError(t, err)
	require.NotNil(t, rhdr)

	require.Equal(t, hdr.ID, rhdr.ID)
	require.Equal(t, hdr.Alh(), rhdr.Alh())

	_, err = replicaStore.ReplicateTx(context.Background(), nil, false, false)
	require.ErrorIs(t, err, ErrIllegalArguments)
}

func TestExportAndReplicateTxCornerCases(t *testing.T) {
	primaryDir := t.TempDir()

	primaryStore, err := Open(primaryDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, primaryStore)

	replicaDir := t.TempDir()

	replicaStore, err := Open(replicaDir, DefaultOptions().WithMaxActiveTransactions(1))
	require.NoError(t, err)
	defer immustoreClose(t, replicaStore)

	tx, err := primaryStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	tx.WithMetadata(NewTxMetadata())

	err = tx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr, err := tx.Commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, hdr)

	txholder := tempTxHolder(t, primaryStore)

	t.Run("prevent replicating broken data", func(t *testing.T) {
		etx, err := primaryStore.ExportTx(1, false, false, txholder)
		require.NoError(t, err)

		for i := range etx {
			if i >= 44 && i < 52 {
				// Timestamp - this field is part of innerHash thus is not validated through EH
				continue
			}

			t.Run(fmt.Sprintf("broken byte at position %d", i), func(t *testing.T) {

				// Break etx by modifying a single byte of the packet
				brokenEtx := make([]byte, len(etx))
				copy(brokenEtx, etx)
				brokenEtx[i]++

				_, err = replicaStore.ReplicateTx(context.Background(), brokenEtx, false, false)
				require.Error(t, err)

				if !errors.Is(err, ErrIllegalArguments) &&
					!errors.Is(err, ErrMaxActiveTransactionsLimitExceeded) &&
					!errors.Is(err, ErrCorruptedData) &&
					!errors.Is(err, ErrNewerVersionOrCorruptedData) {
					require.Failf(t, "Incorrect error", "Incorrect error received from validation: %v", err)
				}
			})
		}
	})
}

func TestExportAndReplicateTxSimultaneousWriters(t *testing.T) {
	primaryDir := t.TempDir()

	primaryStore, err := Open(primaryDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, primaryStore)

	replicaDir := t.TempDir()

	replicaOpts := DefaultOptions().WithMaxConcurrency(100)
	replicaStore, err := Open(replicaDir, replicaOpts)
	require.NoError(t, err)
	defer immustoreClose(t, replicaStore)

	const txCount = 3

	for i := 0; i < txCount; i++ {
		t.Run(fmt.Sprintf("tx: %d", i), func(t *testing.T) {
			tx, err := primaryStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			tx.WithMetadata(NewTxMetadata())

			err = tx.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)

			hdr, err := tx.Commit(context.Background())
			require.NoError(t, err)
			require.NotNil(t, hdr)

			txholder := tempTxHolder(t, replicaStore)
			etx, err := primaryStore.ExportTx(hdr.ID, false, false, txholder)
			require.NoError(t, err)

			// Replicate the same transactions concurrently, only one must succeed
			errors := make([]error, replicaStore.maxConcurrency)
			wg := sync.WaitGroup{}
			for j := 0; j < replicaStore.maxConcurrency; j++ {
				wg.Add(1)
				go func(j int) {
					defer wg.Done()
					_, errors[j] = replicaStore.ReplicateTx(context.Background(), etx, false, false)
				}(j)
			}
			wg.Wait()

			winnersCnt := 0
			for _, err := range errors {
				if err == nil {
					winnersCnt++
				} else {
					require.ErrorIs(t, err, ErrTxAlreadyCommitted)
				}
			}
			require.Equal(t, 1, winnersCnt)
			require.EqualValues(t, i+1, replicaStore.TxCount())
		})
	}
}

func TestExportAndReplicateTxDisorderedReplication(t *testing.T) {
	primaryDir := t.TempDir()

	primaryStore, err := Open(primaryDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, primaryStore)

	replicaDir := t.TempDir()

	replicaOpts := DefaultOptions().WithMaxConcurrency(100)
	replicaStore, err := Open(replicaDir, replicaOpts)
	require.NoError(t, err)
	defer immustoreClose(t, replicaStore)

	const txCount = 15

	etxs := make(chan []byte, txCount)

	txholder := tempTxHolder(t, replicaStore)

	for i := 0; i < txCount; i++ {
		tx, err := primaryStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		tx.WithMetadata(NewTxMetadata())

		err = tx.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
		require.NoError(t, err)

		hdr, err := tx.Commit(context.Background())
		require.NoError(t, err)
		require.NotNil(t, hdr)

		etx, err := primaryStore.ExportTx(hdr.ID, false, false, txholder)
		require.NoError(t, err)

		etxs <- etx
	}

	close(etxs)

	const replicatorsCount = 3

	var wg sync.WaitGroup
	wg.Add(replicatorsCount)

	rand.Seed(time.Now().UnixNano())

	for r := 0; r < replicatorsCount; r++ {
		go func(replicatorID int) {
			defer wg.Done()
			for etx := range etxs {
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

				_, err = replicaStore.ReplicateTx(context.Background(), etx, false, false)
				require.NoError(t, err)
			}
		}(r)
	}

	// it's needed to avoid getting 'already closed' error if the store is closed fast enough
	wg.Wait()

	if t.Failed() {
		return
	}

	err = replicaStore.WaitForTx(context.Background(), uint64(txCount), false)
	require.NoError(t, err)
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
	immuStore, err := Open(t.TempDir(), DefaultOptions().WithMaxConcurrency(1))
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	// set initial value
	otx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = otx.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	hdr1, err := otx.Commit(context.Background())
	require.NoError(t, err)

	// delete entry
	otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = otx.Delete(context.Background(), []byte("key1"))
	require.NoError(t, err)

	_, err = otx.Commit(context.Background())
	require.NoError(t, err)

	t.Run("must not exist constraint should pass when evaluated over a deleted key", func(t *testing.T) {
		otx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustNotExist{[]byte("key1")})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("must exist constraint should pass when evaluated over an existent key", func(t *testing.T) {
		otx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key3"), nil, []byte("value3"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustExist{[]byte("key2")})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("must not be modified after constraint should not pass when key is deleted after specified tx", func(t *testing.T) {
		otx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("key1"), TxID: hdr1.ID})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})

	t.Run("must not be modified after constraint should pass when if key does not exist", func(t *testing.T) {
		otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("nonExistentKey"), TxID: 1})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.NoError(t, err)
	})

	// insert an expirable entry
	otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	md := NewKVMetadata()
	err = md.ExpiresAt(time.Now().Add(1 * time.Second))
	require.NoError(t, err)

	err = otx.Set([]byte("expirableKey"), md, []byte("expirableValue"))
	require.NoError(t, err)

	hdr, err := otx.Commit(context.Background())
	require.NoError(t, err)

	// wait for entry to be expired
	for i := 0; ; i++ {
		require.Less(t, i, 20, "entry expiration failed")

		time.Sleep(100 * time.Millisecond)

		_, err = immuStore.Get(context.Background(), []byte("expirableKey"))
		if err != nil && errors.Is(err, ErrKeyNotFound) {
			break
		}

		require.NoError(t, err)
	}

	t.Run("must not be modified after constraint should not pass when if expired and expiration was set after specified tx", func(t *testing.T) {
		otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyNotModifiedAfterTx{Key: []byte("expirableKey"), TxID: hdr.ID - 1})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})

	t.Run("must not exist constraint should pass when if expired", func(t *testing.T) {
		otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustNotExist{Key: []byte("expirableKey")})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("must exist constraint should not pass when if expired", func(t *testing.T) {
		otx, err = immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = otx.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		err = otx.AddPrecondition(&PreconditionKeyMustExist{Key: []byte("expirableKey")})
		require.NoError(t, err)

		_, err = otx.Commit(context.Background())
		require.ErrorIs(t, err, ErrPreconditionFailed)
	})
}

func BenchmarkSyncedAppend(b *testing.B) {
	opts := DefaultOptions().
		WithMaxConcurrency(100).
		WithSynced(true).
		WithAHTOptions(DefaultAHTOptions().WithSyncThld(1_000)).
		WithSyncFrequency(20 * time.Millisecond).
		WithMaxActiveTransactions(100)

	immuStore, _ := Open(b.TempDir(), opts)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		workerCount := 100

		var wg sync.WaitGroup
		wg.Add(workerCount)

		for w := 0; w < workerCount; w++ {
			go func() {
				txCount := 100
				eCount := 1

				committed := 0

				for committed < txCount {
					tx, err := immuStore.NewWriteOnlyTx(context.Background())
					require.NoError(b, err)

					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(i<<4+j))

						v := make([]byte, 8)
						binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

						err = tx.Set(k, nil, v)
						require.NoError(b, err)
					}

					_, err = tx.AsyncCommit(context.Background())
					if err == ErrMaxConcurrencyLimitExceeded || err == ErrMaxActiveTransactionsLimitExceeded {
						time.Sleep(1 * time.Nanosecond)
						continue
					}
					require.NoError(b, err)

					committed++
				}

				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkAsyncAppend(b *testing.B) {
	opts := DefaultOptions().
		WithSynced(false).
		WithMaxConcurrency(1).
		WithMaxActiveTransactions(100)

	immuStore, _ := Open(b.TempDir(), opts)

	for i := 0; i < b.N; i++ {
		txCount := 1000
		eCount := 1000

		for i := 0; i < txCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(b, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(b, err)
			}

			_, err = tx.Commit(context.Background())
			require.NoError(b, err)
		}
	}
}

func BenchmarkSyncedAppendWithExtCommitAllowance(b *testing.B) {
	opts := DefaultOptions().
		WithMaxConcurrency(100).
		WithSynced(true).
		WithAHTOptions(DefaultAHTOptions().WithSyncThld(1_000)).
		WithSyncFrequency(20 * time.Millisecond).
		WithMaxActiveTransactions(1000).
		WithExternalCommitAllowance(true)

	immuStore, _ := Open(b.TempDir(), opts)

	go func() {
		for {
			err := immuStore.AllowCommitUpto(immuStore.LastPrecommittedTxID())
			if err == ErrAlreadyClosed {
				return
			}
			require.NoError(b, err)

			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		workerCount := 100

		var wg sync.WaitGroup
		wg.Add(workerCount)

		for w := 0; w < workerCount; w++ {
			go func() {
				txCount := 10
				eCount := 1

				committed := 0

				for committed < txCount {
					tx, err := immuStore.NewWriteOnlyTx(context.Background())
					require.NoError(b, err)

					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(i<<4+j))

						v := make([]byte, 8)
						binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

						err = tx.Set(k, nil, v)
						require.NoError(b, err)
					}

					_, err = tx.AsyncCommit(context.Background())
					if err == ErrMaxConcurrencyLimitExceeded || err == ErrMaxActiveTransactionsLimitExceeded {
						time.Sleep(1 * time.Nanosecond)
						continue
					}
					require.NoError(b, err)

					committed++
				}

				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkAsyncAppendWithExtCommitAllowance(b *testing.B) {
	opts := DefaultOptions().
		WithSynced(false).
		WithMaxConcurrency(1).
		WithMaxActiveTransactions(1000).
		WithExternalCommitAllowance(true)

	immuStore, _ := Open(b.TempDir(), opts)

	go func() {
		for {
			err := immuStore.AllowCommitUpto(immuStore.LastPrecommittedTxID())
			if err == ErrAlreadyClosed {
				return
			}
			require.NoError(b, err)
			time.Sleep(time.Duration(5) * time.Millisecond)
		}
	}()

	for i := 0; i < b.N; i++ {
		txCount := 1000
		eCount := 1000

		for i := 0; i < txCount; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(b, err)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				err = tx.Set(k, nil, v)
				require.NoError(b, err)
			}

			_, err = tx.Commit(context.Background())
			require.NoError(b, err)
		}
	}
}

func BenchmarkExportTx(b *testing.B) {
	opts := DefaultOptions().WithSynced(false)

	immuStore, _ := Open(b.TempDir(), opts)

	txCount := 1_000
	eCount := 1_000
	keyLen := 40
	valLen := 256

	for i := 0; i < txCount; i++ {
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(b, err)

		for j := 0; j < eCount; j++ {
			k := make([]byte, keyLen)
			binary.BigEndian.PutUint64(k, uint64(i*eCount+j))

			v := make([]byte, valLen)
			binary.BigEndian.PutUint64(v, uint64(j))

			err = tx.Set(k, nil, v)
			require.NoError(b, err)
		}

		_, err = tx.Commit(context.Background())
		require.NoError(b, err)
	}

	tx, err := immuStore.fetchAllocTx()
	require.NoError(b, err)
	defer immuStore.releaseAllocTx(tx)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < txCount; i++ {
			_, err := immuStore.ExportTx(uint64(i+1), false, false, tx)
			require.NoError(b, err)
		}
	}
}

func TestImmudbStoreIncompleteCommitWrite(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithPreallocFiles(false)

	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	tx, err := immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val1"))
	require.NoError(t, err)

	hdr, err := tx.Commit(context.Background())
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

		err = fl.Sync()
		require.NoError(t, err)
	}

	append("commit/00000000.txi", 11) // Commit log entry is 12 bytes, must add less than that
	append("tx/00000000.tx", 100)
	append("val_0/00000000.val", 100)

	// Force reindexing and rebuilding the aht tree
	err = os.RemoveAll(filepath.Join(dir, "aht"))
	require.NoError(t, err)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	valRef, err := immuStore.Get(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr.ID, valRef.Tx())

	value, err := valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val1"), value)

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreTruncatedCommitLog(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithPreallocFiles(false)

	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	tx, err := immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val1"))
	require.NoError(t, err)

	hdr1, err := tx.Commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, hdr1)

	tx, err = immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val2"))
	require.NoError(t, err)

	hdr2, err := tx.Commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, hdr2)
	require.NotEqual(t, hdr1.ID, hdr2.ID)

	err = immuStore.Close()
	require.NoError(t, err)

	// Truncate the commit and tx logs - it must discard the last transaction but other than
	// that the immudb should work correctly
	// Note: This may change once the truthly appendable interface is implemented
	//       (https://github.com/codenotary/immudb/issues/858)

	cLogFile := filepath.Join(dir, "commit/00000000.txi")
	stat, err := os.Stat(cLogFile)
	require.NoError(t, err)

	err = os.Truncate(cLogFile, stat.Size()-1)
	require.NoError(t, err)

	txLogFile := filepath.Join(dir, "tx/00000000.tx")
	stat, err = os.Stat(txLogFile)
	require.NoError(t, err)

	err = os.Truncate(txLogFile, stat.Size()-1)
	require.NoError(t, err)

	// Remove the index, it does not support truncation of commits now
	err = os.RemoveAll(filepath.Join(dir, "index"))
	require.NoError(t, err)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	err = immuStore.WaitForIndexingUpto(context.Background(), hdr1.ID)
	require.NoError(t, err)

	valRef, err := immuStore.Get(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr1.ID, valRef.Tx())

	value, err := valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val1"), value)

	// ensure we can correctly write more data into the store
	tx, err = immuStore.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte("key1"), nil, []byte("val2"))
	require.NoError(t, err)

	_, err = tx.Commit(context.Background())
	require.NoError(t, err)

	valRef, err = immuStore.Get(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr2.ID, valRef.Tx())

	value, err = valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val2"), value)

	// test after reopening the store
	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	valRef, err = immuStore.Get(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, hdr2.ID, valRef.Tx())

	value, err = valRef.Resolve()
	require.NoError(t, err)
	require.EqualValues(t, []byte("val2"), value)

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbPreconditionIndexing(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	t.Run("commit", func(t *testing.T) {
		indexer, err := immuStore.getIndexerFor(nil)
		require.NoError(t, err)

		// First add some entries that are not indexed
		indexer.Pause()

		for i := 1; i < 100; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("key_%d", i)), nil, []byte(fmt.Sprintf("value_%d", i)))
			require.NoError(t, err)

			_, err = tx.AsyncCommit(context.Background())
			require.NoError(t, err)
		}

		// Next prepare transaction with preconditions - this must wait for the indexer
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
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
			indexer.Resume()
		}()

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("commitWith", func(t *testing.T) {
		indexer, err := immuStore.getIndexerFor(nil)
		require.NoError(t, err)

		// First add some entries that are not indexed
		indexer.Pause()

		for i := 1; i < 100; i++ {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			err = tx.Set([]byte(fmt.Sprintf("key2_%d", i)), nil, []byte(fmt.Sprintf("value2_%d", i)))
			require.NoError(t, err)

			_, err = tx.AsyncCommit(context.Background())
			require.NoError(t, err)
		}

		go func() {
			time.Sleep(100 * time.Millisecond)
			indexer.Resume()
		}()

		// Next prepare transaction with preconditions - this must wait for the indexer
		_, err = immuStore.CommitWith(context.Background(), func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error) {
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
	immuStore, err := Open(t.TempDir(), DefaultOptions())
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
		tx, err := immuStore.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		err = tx.Set([]byte("key1"), nil, []byte("val1"))
		require.NoError(t, err)

		hdr, err := tx.Commit(context.Background())
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
		hdr, err := immuStore.LastTxUntil(time.Now().Add(1 * time.Second))
		require.NoError(t, err)
		require.Equal(t, uint64(txCount), hdr.ID)
	})

	t.Run("the first tx should be returned when requesting from a past time", func(t *testing.T) {
		hdr, err := immuStore.FirstTxSince(start)
		require.NoError(t, err)
		require.Equal(t, uint64(1), hdr.ID)
	})

	t.Run("no tx should be returned when requesting a tx until a past time", func(t *testing.T) {
		_, err = immuStore.LastTxUntil(start)
		require.ErrorIs(t, err, ErrTxNotFound)
	})

	for i, ts := range txts {
		hdr, err := immuStore.FirstTxSince(time.Unix(ts, 0))
		require.NoError(t, err)
		require.LessOrEqual(t, ts, hdr.Ts)
		require.GreaterOrEqual(t, uint64(i+1), hdr.ID)

		if hdr.ID > 1 {
			require.Less(t, txts[hdr.ID-2], ts)
		}

		_, err = immuStore.LastTxUntil(time.Unix(ts, 0))
		require.NoError(t, err)
		require.GreaterOrEqual(t, ts, hdr.Ts)

		if int(hdr.ID) < len(txts) {
			require.GreaterOrEqual(t, txts[hdr.ID], ts)
		}
	}
}

func TestBlTXOrdering(t *testing.T) {
	opts := DefaultOptions().WithMaxConcurrency(200)

	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	t.Run("run multiple simultaneous writes", func(t *testing.T) {
		wg := sync.WaitGroup{}
		done := make(chan struct{})
		for i := 0; i < opts.MaxConcurrency; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					tx, err := immuStore.NewWriteOnlyTx(context.Background())
					require.NoError(t, err)

					tx.Set([]byte(fmt.Sprintf("key:%d", i)), nil, []byte("value"))

					_, err = tx.Commit(context.Background())
					require.NoError(t, err)
				}
			}(i)
		}
		// Perform writes for larger time so that transactions will have different
		// timestamps
		time.Sleep(2 * time.Second)
		close(done)
		wg.Wait()
		if t.Failed() {
			t.FailNow()
		}
	})

	t.Run("verify dual proofs for sequences of transactions", func(t *testing.T) {
		maxTxID, _ := immuStore.CommittedAlh()

		for i := uint64(1); i < maxTxID; i++ {

			srcTxHeader, err := immuStore.ReadTxHeader(i, false, false)
			require.NoError(t, err)

			dstTxHeader, err := immuStore.ReadTxHeader(i+1, false, false)
			require.NoError(t, err)

			require.LessOrEqual(t, srcTxHeader.BlTxID, dstTxHeader.BlTxID)
			require.LessOrEqual(t, srcTxHeader.Ts, dstTxHeader.Ts)

			proof, err := immuStore.DualProof(srcTxHeader, dstTxHeader)
			require.NoError(t, err)

			verifies := VerifyDualProof(proof, i, i+1, srcTxHeader.Alh(), dstTxHeader.Alh())
			require.True(t, verifies)

			dproofV2, err := immuStore.DualProofV2(srcTxHeader, dstTxHeader)
			require.NoError(t, err)

			verifiesV2 := VerifyDualProofV2(dproofV2, i, i+1, srcTxHeader.Alh(), dstTxHeader.Alh())
			require.NoError(t, verifiesV2)
		}

	})
}

func TestImmudbStoreExternalCommitAllowance(t *testing.T) {
	opts := DefaultOptions().
		WithSynced(false).
		WithExternalCommitAllowance(true)

	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	txCount := 10
	eCount := 10

	var wg sync.WaitGroup
	wg.Add(txCount)

	for i := 0; i < txCount; i++ {
		go func() {
			defer wg.Done()

			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for i := 0; i < eCount; i++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			_, err = tx.Commit(context.Background())
			require.NoError(t, err)
		}()
	}

	err = immuStore.WaitForTx(context.Background(), uint64(txCount), true)
	require.NoError(t, err)

	go func() {
		for i := 0; i < txCount; i++ {
			require.Less(t, immuStore.LastCommittedTxID(), uint64(i+1))

			err = immuStore.AllowCommitUpto(uint64(i + 1))
			require.NoError(t, err)

			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}()

	wg.Wait()

	require.Equal(t, uint64(txCount), immuStore.LastCommittedTxID())
}

func TestImmudbStorePrecommittedTxLoading(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().
		WithSynced(false).
		WithEmbeddedValues(false).
		WithExternalCommitAllowance(true)

	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	txCount := 10
	eCount := 10

	var wg sync.WaitGroup
	wg.Add(txCount)

	for i := 0; i < txCount; i++ {
		go func() {
			defer wg.Done()

			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for i := 0; i < eCount; i++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			_, err = tx.Commit(context.Background())
			require.ErrorIs(t, err, ErrAlreadyClosed)
		}()
	}

	err = immuStore.WaitForTx(context.Background(), uint64(txCount), true)
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	err = immuStore.AllowCommitUpto(uint64(txCount))
	require.NoError(t, err)

	err = immuStore.WaitForTx(context.Background(), uint64(txCount), false)
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	wg.Wait()
}

func TestImmudbStorePrecommittedTxDiscarding(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions().
		WithSynced(false).
		WithEmbeddedValues(false).
		WithExternalCommitAllowance(true)

	immuStore, err := Open(dir, opts)
	require.NoError(t, err)

	txCount := 10
	eCount := 10

	var wg sync.WaitGroup
	wg.Add(txCount)

	for i := 0; i < txCount; i++ {
		go func() {
			tx, err := immuStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			for i := 0; i < eCount; i++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i))

				err = tx.Set(k, nil, v)
				require.NoError(t, err)
			}

			wg.Done()

			_, err = tx.Commit(context.Background())
			require.ErrorIs(t, err, ErrAlreadyClosed)
		}()
	}

	err = immuStore.WaitForTx(context.Background(), uint64(txCount), true)
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(dir, opts)
	require.NoError(t, err)

	n, err := immuStore.DiscardPrecommittedTxsSince(0)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Zero(t, n)

	err = immuStore.AllowCommitUpto(uint64(txCount / 2))
	require.NoError(t, err)

	n, err = immuStore.DiscardPrecommittedTxsSince(1)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Zero(t, n)

	err = immuStore.WaitForTx(context.Background(), uint64(txCount/2), false)
	require.NoError(t, err)

	// discard all expect one precommitted tx
	n, err = immuStore.DiscardPrecommittedTxsSince(uint64(txCount/2 + 2))
	require.NoError(t, err)
	require.Equal(t, txCount/2-1, n)

	require.Equal(t, uint64(txCount/2+1), immuStore.LastPrecommittedTxID())

	// discard latest precommitted one
	n, err = immuStore.DiscardPrecommittedTxsSince(uint64(txCount/2 + 1))
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.Equal(t, uint64(txCount/2), immuStore.LastPrecommittedTxID())

	err = immuStore.Close()
	require.NoError(t, err)

	wg.Wait()
}

func TestImmudbStoreMVCC(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	t.Run("no read conflict should be detected when read keys are not updated by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx2.Get(context.Background(), []byte("key2"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected even when the key was updated by another transaction if its value was not read", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		err = tx2.Set([]byte("key1"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when read key was updated by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key3"), nil, []byte("value"))
		require.NoError(t, err)

		_, err = tx2.Get(context.Background(), []byte("key3"))
		require.ErrorIs(t, err, ErrKeyNotFound)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		err = tx2.Set([]byte("key3"), nil, []byte("value"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read key was deleted by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key4"), nil, []byte("value"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Delete(context.Background(), []byte("key4"))
		require.NoError(t, err)

		_, err = tx3.Get(context.Background(), []byte("key4"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		err = tx3.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when read keys are not updated by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		key, _, err := tx2.GetWithPrefix(context.Background(), []byte("key2"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("key2"), key)

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when read key was updated by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		key, _, err := tx2.GetWithPrefix(context.Background(), []byte("key"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), key)

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read key was deleted by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		_, _, err = tx2.GetWithPrefix(context.Background(), []byte("key"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("key1"), []byte("key1"))

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read keys have been updated by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key3"), nil, []byte("value3"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key4"), nil, []byte("value4"))
		require.NoError(t, err)

		err = tx3.Set([]byte("key2"), nil, []byte("value2_2"))
		require.NoError(t, err)

		err = tx3.Set([]byte("key3"), nil, []byte("value3_2"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix: []byte("key"),
		})
		require.NoError(t, err)

		for i := 1; i <= 4; i++ {
			for j := 1; j <= i; j++ {
				_, _, err = r.Read(context.Background())
				if errors.Is(err, ErrNoMoreEntries) {
					break
				}
			}

			err = r.Reset()
			require.NoError(t, err)
		}

		err = r.Close()
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("no read conflict should be detected when read keys have been updated by the ongoing transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		err = tx2.Set([]byte("key3"), nil, []byte("value3"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		err = tx3.Set([]byte("key2"), nil, []byte("value2_2"))
		require.NoError(t, err)

		err = tx3.Set([]byte("key3"), nil, []byte("value3_2"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix: []byte("key"),
		})
		require.NoError(t, err)

		for i := 1; i <= 3; i++ {
			for j := 1; j <= i; j++ {
				_, _, err = r.Read(context.Background())
				if errors.Is(err, ErrNoMoreEntries) {
					break
				}
			}

			err = r.Reset()
			require.NoError(t, err)
		}

		err = r.Close()
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.NoError(t, err)
	})

	t.Run("read conflict should be detected when reading more entries than expected", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Set([]byte("key5"), nil, []byte("value5"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix: []byte("key"),
		})
		require.NoError(t, err)

		for {
			_, _, err = r.Read(context.Background())
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
		}

		err = r.Close()
		require.NoError(t, err)

		err = tx3.Set([]byte("key6"), nil, []byte("value6"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read keys are deleted by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix:  []byte("key"),
			Filters: []FilterFn{IgnoreDeleted},
		})
		require.NoError(t, err)

		for {
			_, _, err = r.Read(context.Background())
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
		}

		err = r.Close()
		require.NoError(t, err)

		err = tx3.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read keys are deleted by the ongoing transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		err = tx3.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix:  []byte("key"),
			Filters: []FilterFn{IgnoreDeleted},
		})
		require.NoError(t, err)

		for {
			_, _, err = r.Read(context.Background())
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
		}

		err = r.Close()
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})

	t.Run("read conflict should be detected when read keys are deleted by another transaction", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx1.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		err = tx1.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		tx3, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		err = tx2.Delete(context.Background(), []byte("key1"))
		require.NoError(t, err)

		r, err := tx3.NewKeyReader(KeyReaderSpec{
			Prefix:  []byte("key"),
			Filters: []FilterFn{IgnoreDeleted},
			Offset:  1,
		})
		require.NoError(t, err)

		for {
			_, _, err = r.Read(context.Background())
			if errors.Is(err, ErrNoMoreEntries) {
				break
			}
		}

		err = r.Close()
		require.NoError(t, err)

		err = tx3.Set([]byte("key2"), nil, []byte("value2"))
		require.NoError(t, err)

		_, err = tx2.Commit(context.Background())
		require.NoError(t, err)

		_, err = tx3.Commit(context.Background())
		require.ErrorIs(t, err, ErrTxReadConflict)
	})
}

func TestImmudbStoreMVCCBoundaries(t *testing.T) {
	mvccReadsetLimit := 3

	immuStore, err := Open(t.TempDir(), DefaultOptions().WithMVCCReadSetLimit(mvccReadsetLimit))
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	t.Run("MVCC read-set limit should be reached when randomly reading keys", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i < mvccReadsetLimit; i++ {
			_, err = tx1.Get(context.Background(), []byte(fmt.Sprintf("key%d", i)))
			require.ErrorIs(t, err, ErrKeyNotFound)
		}

		for i := 0; i < mvccReadsetLimit; i++ {
			_, err = tx1.Get(context.Background(), []byte(fmt.Sprintf("key%d", i)))
			require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)
		}

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should not be reached when reading an updated key", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i <= mvccReadsetLimit; i++ {
			err = tx1.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)
		}

		for i := 0; i <= mvccReadsetLimit; i++ {
			_, err = tx1.Get(context.Background(), []byte(fmt.Sprintf("key%d", i)))
			require.NoError(t, err)
		}

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should be reached when reading keys by prefix", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i < mvccReadsetLimit; i++ {
			_, _, err = tx1.GetWithPrefix(context.Background(), []byte(fmt.Sprintf("key%d", i)), nil)
			require.ErrorIs(t, err, ErrKeyNotFound)
		}

		for i := 0; i < mvccReadsetLimit; i++ {
			_, _, err = tx1.GetWithPrefix(context.Background(), []byte(fmt.Sprintf("key%d", i)), nil)
			require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)
		}

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should not be reached when reading an updated entries", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i <= mvccReadsetLimit; i++ {
			err = tx1.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)
		}

		for i := 0; i <= mvccReadsetLimit; i++ {
			_, _, err = tx1.GetWithPrefix(context.Background(), []byte(fmt.Sprintf("key%d", i)), nil)
			require.NoError(t, err)
		}

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should be reached when scanning out of read-set boundaries", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i < mvccReadsetLimit; i++ {
			err = tx1.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)
		}

		r, err := tx1.NewKeyReader(KeyReaderSpec{Prefix: []byte("key")})
		require.NoError(t, err)

		// Note: creating the reader already consumes one read-set slot
		for i := 0; i < mvccReadsetLimit-1; i++ {
			_, _, err = r.Read(context.Background())
			require.NoError(t, err)
		}

		_, _, err = r.Read(context.Background())
		require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)

		err = r.Close()
		require.NoError(t, err)

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should be reached when reseting a reader out of read-set boundaries", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i < mvccReadsetLimit; i++ {
			err = tx1.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)
		}

		r, err := tx1.NewKeyReader(KeyReaderSpec{Prefix: []byte("key")})
		require.NoError(t, err)

		// Note: creating the reader already consumes one read-set slot
		for i := 0; i < mvccReadsetLimit-1; i++ {
			_, _, err = r.Read(context.Background())
			require.NoError(t, err)
		}

		err = r.Reset()
		require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)

		err = r.Close()
		require.NoError(t, err)

		err = tx1.Cancel()
		require.NoError(t, err)
	})

	t.Run("MVCC read-set limit should be reached when reading non-updated keys", func(t *testing.T) {
		tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i <= mvccReadsetLimit; i++ {
			err = tx1.Set([]byte(fmt.Sprintf("key%d", i)), nil, []byte(fmt.Sprintf("value%d", i)))
			require.NoError(t, err)
		}

		_, err = tx1.Commit(context.Background())
		require.NoError(t, err)

		tx2, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)

		for i := 0; i < mvccReadsetLimit; i++ {
			_, err = tx2.Get(context.Background(), []byte(fmt.Sprintf("key%d", i)))
			require.NoError(t, err)
		}

		_, err = tx2.Get(context.Background(), []byte("key"))
		require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)

		_, _, err = tx2.GetWithPrefix(context.Background(), []byte("key"), nil)
		require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)

		_, err = tx2.NewKeyReader(KeyReaderSpec{Prefix: []byte("key")})
		require.ErrorIs(t, err, ErrMVCCReadSetLimitExceeded)

		err = tx2.Cancel()
		require.NoError(t, err)
	})
}

func TestImmudbStoreWithClosedContext(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	t.Run("transaction creation should fail with a cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := immuStore.NewTx(ctx, DefaultTxOptions())
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("transaction commit should fail with a cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		tx, err := immuStore.NewTx(ctx, DefaultTxOptions())
		require.NoError(t, err)

		err = tx.Set([]byte("key1"), nil, []byte("value1"))
		require.NoError(t, err)

		cancel()

		_, err = tx.Commit(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestImmudbStoreWithoutVLogCache(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions().WithVLogCacheSize(0))
	require.NoError(t, err)

	defer immuStore.Close()

	tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = tx1.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	_, err = tx1.Commit(context.Background())
	require.NoError(t, err)

	valRef, err := immuStore.Get(context.Background(), []byte("key1"))
	require.NoError(t, err)

	val, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
}

func TestImmudbStoreWithVLogCache(t *testing.T) {
	immuStore, err := Open(t.TempDir(), DefaultOptions().WithVLogCacheSize(10))
	require.NoError(t, err)

	defer immuStore.Close()

	tx1, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	err = tx1.Set([]byte("key1"), nil, []byte("value1"))
	require.NoError(t, err)

	_, err = tx1.Commit(context.Background())
	require.NoError(t, err)

	_, valRef, err := immuStore.GetWithPrefix(context.Background(), []byte("key1"), nil)
	require.NoError(t, err)

	val, err := valRef.Resolve()
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
}

func TestImmudbStoreTruncateUptoTx_WithMultipleIOConcurrency(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxConcurrency(100).
		WithMaxIOConcurrency(5)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	for i := 1; i <= 20; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		hdr, err := tx.Commit(context.Background())
		require.NoError(t, err)

		readTx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(hdr.ID, false, readTx)
		require.NoError(t, err)

		for _, e := range readTx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}

	deletePointTx := uint64(15)

	hdr, err := st.ReadTxHeader(deletePointTx, false, false)
	require.NoError(t, err)

	require.NoError(t, st.TruncateUptoTx(hdr.ID))

	for i := deletePointTx; i <= 20; i++ {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}
}

func TestImmudbStoreTruncateUptoTx_WithSingleIOConcurrency(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxIOConcurrency(1)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	for i := 1; i <= 10; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)
	}

	deletePointTx := uint64(5)

	hdr, err := st.ReadTxHeader(deletePointTx, false, false)
	require.NoError(t, err)

	require.NoError(t, st.TruncateUptoTx(hdr.ID))

	for i := deletePointTx; i <= 10; i++ {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}

	for i := deletePointTx - 1; i > 0; i-- {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.Error(t, err)
		}
	}
}

func TestImmudbStoreTruncateUptoTx_ForIdempotency(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxIOConcurrency(1)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)
	defer immustoreClose(t, st)

	for i := 1; i <= 10; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)
	}

	deletePointTx := uint64(5)

	hdr, err := st.ReadTxHeader(deletePointTx, false, false)
	require.NoError(t, err)

	// TruncateUptoTx should be idempotent
	require.NoError(t, st.TruncateUptoTx(hdr.ID))
	require.NoError(t, st.TruncateUptoTx(hdr.ID))
	require.NoError(t, st.TruncateUptoTx(hdr.ID))

	for i := deletePointTx; i <= 10; i++ {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}

	for i := deletePointTx - 1; i > 0; i-- {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.Error(t, err)
		}
	}

}

func TestImmudbStore_WithConcurrentWritersOnMultipleIO(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxConcurrency(100).
		WithMaxIOConcurrency(3)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	wg := sync.WaitGroup{}

	for i := 1; i <= 3; i++ {
		wg.Add(1)

		go func(j int) {
			defer wg.Done()

			for k := 1*(j-1)*10 + 1; k < (j*10)+1; k++ {
				tx, err := st.NewWriteOnlyTx(context.Background())
				require.NoError(t, err)

				key := []byte(fmt.Sprintf("key_%d", k))
				value := make([]byte, fileSize)

				err = tx.Set(key, nil, value)
				require.NoError(t, err)

				_, err = tx.Commit(context.Background())
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	deletePointTx := uint64(15)

	hdr, err := st.ReadTxHeader(deletePointTx, false, false)
	require.NoError(t, err)
	require.NoError(t, st.TruncateUptoTx(hdr.ID))

	for i := deletePointTx; i <= 30; i++ {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}
}

func TestImmudbStore_WithConcurrentTruncate(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxIOConcurrency(1)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	waitCh := make(chan struct{})
	doneCh := make(chan struct{})

	for i := 1; i <= 20; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		if i == 10 {
			close(waitCh)
		}
	}

	deletePointTx := uint64(5)

	go func() {
		<-waitCh

		hdr, err := st.ReadTxHeader(deletePointTx, false, false)
		require.NoError(t, err)
		require.NoError(t, st.TruncateUptoTx(hdr.ID))

		close(doneCh)
	}()

	<-doneCh

	for i := deletePointTx; i <= 20; i++ {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}

	for i := deletePointTx - 1; i > 0; i-- {
		tx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(i, false, tx)
		require.NoError(t, err)

		for _, e := range tx.Entries() {
			_, err := st.ReadValue(e)
			require.Error(t, err)
		}
	}
}

func TestExportTxWithTruncation(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithEmbeddedValues(false).
		WithFileSize(fileSize).
		WithMaxIOConcurrency(1)

	// Create a master store
	masterDir := t.TempDir()
	masterStore, err := Open(masterDir, opts)
	require.NoError(t, err)
	defer immustoreClose(t, masterStore)

	// Create a replica store
	replicaDir := t.TempDir()
	replicaStore, err := Open(replicaDir, DefaultOptions())
	require.NoError(t, err)
	defer immustoreClose(t, replicaStore)

	t.Run("validate replication post truncation on master", func(t *testing.T) {
		hdrs := make([]*TxHeader, 0, 5)

		// Add 10 transactions on master store
		for i := 1; i <= 10; i++ {
			tx, err := masterStore.NewWriteOnlyTx(context.Background())
			require.NoError(t, err)

			key := []byte(fmt.Sprintf("key_%d", i))
			value := make([]byte, fileSize)

			err = tx.Set(key, nil, value)
			require.NoError(t, err)

			hdr, err := tx.Commit(context.Background())
			require.NoError(t, err)
			require.NotNil(t, hdr)

			hdrs = append(hdrs, hdr)
		}

		// Truncate upto 5th transaction on master store
		deletePointTx := uint64(5)

		hdr, err := masterStore.ReadTxHeader(deletePointTx, false, false)
		require.NoError(t, err)

		require.NoError(t, masterStore.TruncateUptoTx(hdr.ID))

		// Validate that the values are not accessible for transactions that are truncated
		for i := deletePointTx - 1; i > 0; i-- {
			tx := NewTx(masterStore.MaxTxEntries(), masterStore.MaxKeyLen())

			err = masterStore.ReadTx(i, false, tx)
			require.NoError(t, err)

			for _, e := range tx.Entries() {
				_, err := masterStore.ReadValue(e)
				require.Error(t, err)
			}
		}

		// Replicate all the transactions to replica store
		for i := uint64(1); i <= 10; i++ {
			txholder := tempTxHolder(t, masterStore)

			etx, err := masterStore.ExportTx(i, false, false, txholder)
			require.NoError(t, err)

			rhdr, err := replicaStore.ReplicateTx(context.Background(), etx, false, false)
			require.NoError(t, err)
			require.NotNil(t, rhdr)
		}

		// Validate that the alh is matching with master when data is exported to replica
		for i := uint64(1); i <= 10; i++ {
			tx := NewTx(replicaStore.MaxTxEntries(), replicaStore.MaxKeyLen())

			err = replicaStore.ReadTx(i, false, tx)
			require.NoError(t, err)

			hdr := hdrs[i-1]
			require.Equal(t, hdr.ID, tx.header.ID)
			require.Equal(t, hdr.Alh(), tx.header.Alh())
		}

		// Validate that the values are not copied on replica for truncated transaction on master
		for i := deletePointTx - 1; i > 0; i-- {
			tx := NewTx(replicaStore.MaxTxEntries(), replicaStore.MaxKeyLen())

			err = replicaStore.ReadTx(i, false, tx)
			require.NoError(t, err)

			for _, e := range tx.Entries() {
				val, err := replicaStore.ReadValue(e)
				require.NoError(t, err)
				require.Nil(t, val)
			}
		}

		// Validate that the values are copied on replica for non truncated transaction on master
		for i := deletePointTx; i <= 10; i++ {
			tx := NewTx(replicaStore.MaxTxEntries(), replicaStore.MaxKeyLen())

			err = replicaStore.ReadTx(i, false, tx)
			require.NoError(t, err)

			for _, e := range tx.Entries() {
				val, err := replicaStore.ReadValue(e)
				require.NoError(t, err)
				require.NotNil(t, val)
			}
		}
	})
}

func TestImmudbStoreTxMetadata(t *testing.T) {
	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)

	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	t.Run("test tx metadata with truncation header", func(t *testing.T) {
		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		require.NotNil(t, tx)

		tx.WithMetadata(NewTxMetadata().WithTruncatedTxID(10))

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{3, 2, 1})
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, uint64(1), valRef.Tx())
		require.Equal(t, uint64(1), valRef.HC())
		require.Equal(t, uint32(3), valRef.Len())
		require.Equal(t, sha256.Sum256([]byte{3, 2, 1}), valRef.HVal())

		require.True(t, valRef.TxMetadata().HasTruncatedTxID())
		trid, err := valRef.TxMetadata().GetTruncatedTxID()
		require.NoError(t, err)
		require.Equal(t, uint64(10), trid)
	})

	t.Run("test tx metadata with no truncation header", func(t *testing.T) {
		tx, err := immuStore.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		require.NotNil(t, tx)

		err = tx.Set([]byte{1, 2, 3}, nil, []byte{1, 1, 1})
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)

		valRef, err := immuStore.Get(context.Background(), []byte{1, 2, 3})
		require.NoError(t, err)
		require.Equal(t, uint64(2), valRef.Tx())

		v, err := valRef.Resolve()
		require.NoError(t, err)
		require.Equal(t, []byte{1, 1, 1}, v)
		require.NoError(t, err)
		require.Equal(t, uint64(2), valRef.Tx())
		require.Nil(t, valRef.TxMetadata())
		require.Equal(t, uint64(2), valRef.HC())
		require.Equal(t, uint32(3), valRef.Len())
		require.Equal(t, sha256.Sum256([]byte{1, 1, 1}), valRef.HVal())
	})

}

func TestImmudbStoreTruncateUptoTx_WithDataPostTruncationPoint(t *testing.T) {
	fileSize := 1024

	opts := DefaultOptions().
		WithFileSize(fileSize).
		WithMaxIOConcurrency(1)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	for i := 1; i <= 10; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		_, err = tx.Commit(context.Background())
		require.NoError(t, err)
	}

	deletePointTx := uint64(1)

	hdr, err := st.ReadTxHeader(deletePointTx, false, false)
	require.NoError(t, err)

	require.NoError(t, st.TruncateUptoTx(hdr.ID))

	for i := 11; i <= 20; i++ {
		tx, err := st.NewWriteOnlyTx(context.Background())
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("key_%d", i))
		value := make([]byte, fileSize)

		err = tx.Set(key, nil, value)
		require.NoError(t, err)

		hdr, err = tx.Commit(context.Background())
		require.NoError(t, err)

		rtx := NewTx(st.MaxTxEntries(), st.MaxKeyLen())

		err = st.ReadTx(hdr.ID, false, rtx)
		require.NoError(t, err)

		for _, e := range rtx.Entries() {
			_, err := st.ReadValue(e)
			require.NoError(t, err)
		}
	}
}

func TestCommitOfEmptyTxWithMetadata(t *testing.T) {
	st, err := Open(t.TempDir(), DefaultOptions())
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	tx, err := st.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	tx.WithMetadata(NewTxMetadata().WithTruncatedTxID(1))

	hdr, err := tx.Commit(context.Background())
	require.NoError(t, err)

	txholder, err := st.fetchAllocTx()
	require.NoError(t, err)

	defer st.releaseAllocTx(txholder)

	err = st.readTx(hdr.ID, false, true, txholder)
	require.NoError(t, err)
	require.Empty(t, txholder.Entries())
}

func TestImmudbStore_ExportTxWithEmptyValues(t *testing.T) {
	opts := DefaultOptions().WithEmbeddedValues(false)

	st, err := Open(t.TempDir(), opts)
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	tx, err := st.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx.Set([]byte("my-key"), nil, nil)
	require.NoError(t, err)

	hdr, err := tx.Commit(context.Background())
	require.NoError(t, err)

	txholder, err := st.fetchAllocTx()
	require.NoError(t, err)

	defer st.releaseAllocTx(txholder)

	_, err = st.ExportTx(hdr.ID, false, false, txholder)
	require.NoError(t, err)
}

func TestIndexingChanges(t *testing.T) {
	st, err := Open(t.TempDir(), DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	require.NotNil(t, st)

	defer immustoreClose(t, st)

	err = st.InitIndexing(&IndexSpec{
		SourcePrefix: []byte("j"),
		TargetPrefix: []byte("j"),
	})
	require.NoError(t, err)

	err = st.InitIndexing(&IndexSpec{
		SourcePrefix: []byte("k"),
		TargetPrefix: []byte("k"),
	})
	require.NoError(t, err)

	tx1, err := st.NewWriteOnlyTx(context.Background())
	require.NoError(t, err)

	err = tx1.Set([]byte("j1"), nil, []byte("val_j1"))
	require.NoError(t, err)

	err = tx1.Set([]byte("k1"), nil, []byte("val_k1"))
	require.NoError(t, err)

	_, err = tx1.Commit(context.Background())
	require.NoError(t, err)

	tx2, err := st.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, err = tx2.Get(context.Background(), []byte("j1"))
	require.NoError(t, err)

	_, err = tx2.Get(context.Background(), []byte("k1"))
	require.NoError(t, err)

	_, err = tx2.Get(context.Background(), []byte("k2"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, err = tx2.GetWithPrefixAndFilters(context.Background(), []byte("k2"), []byte("k2"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tx2.Cancel()
	require.NoError(t, err)

	err = st.DeleteIndex([]byte("j"))
	require.NoError(t, err)

	err = st.DeleteIndex([]byte("j"))
	require.ErrorIs(t, err, ErrIndexNotFound)

	tx3, err := st.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, err = tx3.Get(context.Background(), []byte("j1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, err = tx3.Get(context.Background(), []byte("k1"))
	require.NoError(t, err)

	err = tx3.Cancel()
	require.NoError(t, err)

	err = st.InitIndexing(&IndexSpec{
		SourcePrefix: []byte("j"),
		TargetPrefix: []byte("j"),
	})
	require.NoError(t, err)

	tx4, err := st.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, err = tx4.Get(context.Background(), []byte("j1"))
	require.NoError(t, err)

	_, err = tx4.Get(context.Background(), []byte("k1"))
	require.NoError(t, err)

	err = tx4.Cancel()
	require.NoError(t, err)

	err = st.CloseIndexing([]byte("k"))
	require.NoError(t, err)

	tx5, err := st.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, err = tx5.Get(context.Background(), []byte("j1"))
	require.NoError(t, err)

	_, err = tx5.Get(context.Background(), []byte("k1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, err = st.GetBetween(context.Background(), []byte("k1"), 1, 1)
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, err = tx5.GetWithPrefixAndFilters(context.Background(), []byte("k1"), []byte("k1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = tx5.Cancel()
	require.NoError(t, err)

	_, err = st.Get(context.Background(), []byte("m1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, err = st.GetBetween(context.Background(), []byte("m1"), 1, 2)
	require.ErrorIs(t, err, ErrKeyNotFound)

	_, _, err = st.GetWithPrefixAndFilters(context.Background(), []byte("k1"), []byte("k1"))
	require.ErrorIs(t, err, ErrKeyNotFound)

}
