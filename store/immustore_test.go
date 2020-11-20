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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/mocked"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/tbtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImmudbStoreConcurrency(t *testing.T) {
	opts := DefaultOptions().WithSynced(false)
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

			id, _, _, _, err := immuStore.Commit(kvs)
			if err != nil {
				panic(err)
			}

			if uint64(i+1) != id {
				panic(fmt.Errorf("expected %v but actual %v", uint64(i+1), id))
			}
		}

		wg.Done()
	}()

	go func() {

		txID := uint64(1)

		for {
			time.Sleep(time.Duration(100) * time.Millisecond)

			txReader, err := immuStore.NewTxReader(txID, 4096)
			if err != nil {
				panic(err)
			}

			for {
				time.Sleep(time.Duration(10) * time.Millisecond)

				tx, err := txReader.Read()
				if err == io.EOF {
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

func TestImmudbStoreOpenWithInvalidPath(t *testing.T) {
	_, err := Open("immustore_test.go", DefaultOptions())
	require.Error(t, ErrorPathIsNotADirectory, err)
}

func TestImmudbStoreOnClosedStore(t *testing.T) {
	immuStore, err := Open("closed_store", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("closed_store")

	err = immuStore.Close()
	require.NoError(t, err)

	err = immuStore.Close()
	require.Error(t, ErrAlreadyClosed, err)

	err = immuStore.Sync()
	require.Error(t, ErrAlreadyClosed, err)

	_, _, _, _, err = immuStore.Commit(nil)
	require.Error(t, ErrAlreadyClosed, err)

	err = immuStore.ReadTx(1, nil)
	require.Error(t, ErrAlreadyClosed, err)

	_, err = immuStore.NewTxReader(1, 1024)
	require.Error(t, ErrAlreadyClosed, err)
}

func TestImmudbStoreSettings(t *testing.T) {
	immuStore, err := Open("store_settings", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("store_settings")

	require.Equal(t, DefaultOptions().readOnly, immuStore.ReadOnly())
	require.Equal(t, DefaultOptions().synced, immuStore.Synced())
	require.Equal(t, DefaultOptions().maxConcurrency, immuStore.MaxConcurrency())
	require.Equal(t, DefaultOptions().maxIOConcurrency, immuStore.MaxIOConcurrency())
	require.Equal(t, DefaultOptions().maxTxEntries, immuStore.MaxTxEntries())
	require.Equal(t, DefaultOptions().maxKeyLen, immuStore.MaxKeyLen())
	require.Equal(t, DefaultOptions().maxValueLen, immuStore.MaxValueLen())
	require.Equal(t, DefaultOptions().maxLinearProofLen, immuStore.MaxLinearProofLen())
}

func TestImmudbStoreEdgeCases(t *testing.T) {
	defer os.RemoveAll("edge_cases")

	_, err := Open("edge_cases", nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = OpenWith("edge_cases", nil, nil, nil, nil)
	require.Error(t, ErrIllegalArguments, err)

	_, err = OpenWith("edge_cases", nil, nil, nil, DefaultOptions())
	require.Error(t, ErrIllegalArguments, err)

	vLog := &mocked.MockedAppendable{}
	vLogs := []appendable.Appendable{vLog}
	txLog := &mocked.MockedAppendable{}
	cLog := &mocked.MockedAppendable{}

	// Should fail reading fileSize from metadata
	cLog.MetadataFn = func() []byte {
		return nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail reading maxTxEntries from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail reading maxKeyLen from metadata
	cLog.MetadataFn = func() []byte {
		md := appendable.NewMetadata(nil)
		md.PutInt(metaFileSize, 1)
		md.PutInt(metaMaxTxEntries, 4)
		md.PutInt(metaMaxKeyLen, 8)
		return md.Bytes()
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
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
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail validating cLogSize
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize + 1, nil
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail reading cLog
	cLog.SizeFn = func() (int64, error) {
		return cLogEntrySize, nil
	}
	cLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, errors.New("error")
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	// Should fail reading txLogSize
	cLog.SizeFn = func() (int64, error) {
		return 0, nil
	}
	txLog.SizeFn = func() (int64, error) {
		return 0, errors.New("error")
	}
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
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
	_, err = OpenWith("edge_cases", vLogs, txLog, cLog, DefaultOptions())
	require.Error(t, err)

	immuStore, err := Open("edge_cases", DefaultOptions())
	require.NoError(t, err)

	_, err = immuStore.DualProof(nil, nil)
	require.Error(t, ErrIllegalArguments, err)

	sourceTx := newTx(1, 1)
	sourceTx.ID = 2
	targetTx := newTx(1, 1)
	targetTx.ID = 1
	_, err = immuStore.DualProof(sourceTx, targetTx)
	require.Error(t, ErrIllegalArguments, err)

	_, err = immuStore.LinearProof(2, 1)
	require.Error(t, ErrIllegalArguments, err)

	_, err = immuStore.LinearProof(1, uint64(1+immuStore.maxLinearProofLen))
	require.Error(t, ErrLinearProofMaxLenExceeded, err)

	_, err = immuStore.ReadValue(sourceTx, []byte{1, 2, 3})
	require.Error(t, ErrKeyNotFound, err)

	err = immuStore.validateEntries(nil)
	require.Error(t, ErrorNoEntriesProvided, err)

	err = immuStore.validateEntries(make([]*KV, immuStore.maxTxEntries+1))
	require.Error(t, ErrorMaxTxEntriesLimitExceeded, err)

	entry := &KV{Key: nil, Value: nil}
	err = immuStore.validateEntries([]*KV{entry})
	require.Error(t, ErrNullKeyOrValue, err)

	entry = &KV{Key: make([]byte, immuStore.maxKeyLen+1), Value: make([]byte, 1)}
	err = immuStore.validateEntries([]*KV{entry})
	require.Error(t, ErrorMaxKeyLenExceeded, err)

	entry = &KV{Key: make([]byte, 1), Value: make([]byte, immuStore.maxValueLen+1)}
	err = immuStore.validateEntries([]*KV{entry})
	require.Error(t, ErrorMaxValueLenExceeded, err)
}

func TestImmudbSetBlErr(t *testing.T) {
	immuStore, err := Open("data_bl_err", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data_bl_err")

	immuStore.SetBlErr(errors.New("error"))

	_, err = immuStore.BlInfo()
	require.Error(t, err)
}

func TestImmudbTxOffsetAndSize(t *testing.T) {
	immuStore, err := Open("data_tx_off_sz", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data_tx_off_sz")

	_, _, err = immuStore.txOffsetAndSize(0)
	require.Error(t, ErrIllegalArguments, err)

	mockedCLog := &mocked.MockedAppendable{}
	mockedCLog.ReadAtFn = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs, uint64(immuStore.committedTxLogSize+1))
		binary.BigEndian.PutUint32(bs[8:], uint32(immuStore.maxTxSize+1))
		return 12, nil
	}
	immuStore.cLog = mockedCLog

	_, _, err = immuStore.txOffsetAndSize(1)
	require.Error(t, ErrorCorruptedTxData, err)
}

func TestImmudbStoreIndexing(t *testing.T) {
	opts := DefaultOptions().WithSynced(false)
	immuStore, err := Open("data_indexing", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_indexing")

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	_, _, _, _, err = immuStore.Commit(nil)
	require.Equal(t, ErrorNoEntriesProvided, err)

	_, _, _, _, err = immuStore.Commit([]*KV{
		{Key: []byte("key"), Value: []byte("value")},
		{Key: []byte("key"), Value: []byte("value")},
	})
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			for {
				_, err := immuStore.IndexInfo()
				if err != nil {
					panic(err)
				}

				snap, err := immuStore.Snapshot()
				if err != nil {
					panic(err)
				}

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						v := make([]byte, 8)
						binary.BigEndian.PutUint64(v, snap.Ts()-1)

						wv, _, err := snap.Get(k)

						if err != nil {
							if err != tbtree.ErrKeyNotFound {
								panic(err)
							}
						}

						if err == nil {
							if wv == nil {
								panic("expected not nil")
							}

							valLen := binary.BigEndian.Uint32(wv)
							vOff := binary.BigEndian.Uint64(wv[4:])

							var hVal [sha256.Size]byte
							copy(hVal[:], wv[4+8:])

							val := make([]byte, valLen)
							_, err := immuStore.ReadValueAt(val, int64(vOff), hVal)

							if err != nil {
								panic(err)
							}

							if !bytes.Equal(v, val) {
								panic(fmt.Errorf("expected %v actual %v", v, val))
							}

							_, err = immuStore.ReadValueAt(val, int64(vOff), sha256.Sum256(hVal[:]))
							if err != ErrCorruptedData {
								panic(err)
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

	_, err = immuStore.IndexInfo()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreHistoricalValues(t *testing.T) {
	opts := DefaultOptions().WithSynced(false)
	immuStore, err := Open("data_historical", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_historical")

	require.NotNil(t, immuStore)

	txCount := 10
	eCount := 100

	_, _, _, _, err = immuStore.Commit(nil)
	require.Equal(t, ErrorNoEntriesProvided, err)

	_, _, _, _, err = immuStore.Commit([]*KV{
		{Key: []byte("key"), Value: []byte("value")},
		{Key: []byte("key"), Value: []byte("value")},
	})
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)

	var wg sync.WaitGroup
	wg.Add(1)

	for f := 0; f < 1; f++ {
		go func() {
			for {
				snap, err := immuStore.Snapshot()
				if err != nil {
					panic(err)
				}

				for i := 0; i < int(snap.Ts()); i++ {
					for j := 0; j < eCount; j++ {
						k := make([]byte, 8)
						binary.BigEndian.PutUint64(k, uint64(j))

						txIDs, err := snap.GetTs(k, int64(txCount))
						if err != nil {
							panic(err)
						}
						if int(snap.Ts()) != len(txIDs) {
							panic(fmt.Errorf("expected %v actual %v", int(snap.Ts()), len(txIDs)))
						}

						tx := immuStore.NewTx()

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

	_, err = immuStore.IndexInfo()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreInclusionProof(t *testing.T) {
	immuStore, err := Open("data_inclusion_proof", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("data_inclusion_proof")

	require.NotNil(t, immuStore)

	txCount := 100
	eCount := 100

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, _, _, _, err = immuStore.Commit([]*KV{{Key: []byte{}, Value: []byte{}}})
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data_inclusion_proof", DefaultOptions())
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, 1024)
	require.NoError(t, err)

	for i := 0; i < txCount; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for j := 0; j < eCount; j++ {
			path := tx.Proof(j)

			key := txEntries[j].Key()

			value := make([]byte, txEntries[j].ValueLen)
			_, err := immuStore.ReadValueAt(value, txEntries[j].VOff, txEntries[j].HValue)
			require.NoError(t, err)

			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			require.Equal(t, k, key)
			require.Equal(t, v, value)

			kv := &KV{Key: key, Value: value}

			verifies := path.VerifyInclusion(uint64(tx.nentries-1), uint64(j), tx.Eh, kv.Digest())
			require.True(t, verifies)

			v, err = immuStore.ReadValue(tx, key)
			require.Equal(t, value, v)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestLeavesMatchesAHTSync(t *testing.T) {
	immuStore, err := Open("data_leaves_alh", DefaultOptions().WithSynced(false).WithMaxLinearProofLen(0))
	require.NoError(t, err)
	defer os.RemoveAll("data_leaves_alh")

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
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

		alh := tx.Alh()
		require.Equal(t, alh[:], p)
	}
}

func TestLeavesMatchesAHTASync(t *testing.T) {
	immuStore, err := Open("data_leaves_alh_async", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("data_leaves_alh_async")

	require.NotNil(t, immuStore)

	txCount := 1000
	eCount := 10

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
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

		alh := tx.Alh()
		require.Equal(t, alh[:], p)
	}
}

func TestImmudbStoreConsistencyProof(t *testing.T) {
	immuStore, err := Open("data_consistency_proof", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof")

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 10

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
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

			verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
			require.True(t, verifies)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreConsistencyProofAgainstLatest(t *testing.T) {
	immuStore, err := Open("data_consistency_proof_latest", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof_latest")

	require.NotNil(t, immuStore)

	txCount := 66
	eCount := 10

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
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

		verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
		require.True(t, verifies)
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func TestImmudbStoreConsistencyProofReopened(t *testing.T) {
	immuStore, err := Open("data_consistency_proof_reopen", DefaultOptions().WithSynced(false))
	require.NoError(t, err)
	defer os.RemoveAll("data_consistency_proof_reopen")

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 100

	_, _, _, _, err = immuStore.Commit(nil)
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

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
	}

	err = immuStore.Sync()
	require.NoError(t, err)

	err = immuStore.Close()
	require.NoError(t, err)

	_, _, _, _, err = immuStore.Commit([]*KV{{Key: []byte{}, Value: []byte{}}})
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data_consistency_proof_reopen", DefaultOptions())
	require.NoError(t, err)

	for i := 0; i < txCount; i++ {
		txID := uint64(i + 1)

		ri, err := immuStore.NewTxReader(txID, 1024)
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

			verifies := VerifyLinearProof(lproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
			require.True(t, verifies)

			dproof, err := immuStore.DualProof(sourceTx, targetTx)
			require.NoError(t, err)

			verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
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
		opts := DefaultOptions().WithSynced(false)
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

			id, _, _, _, err := immuStore.Commit(kvs)
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), id)
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
			WithCompresionLevel(appendable.DefaultCompression)

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

			id, _, _, _, err := immuStore.Commit(kvs)
			require.NoError(t, err)
			require.Equal(t, uint64(it*txCount+i+1), id)
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

	opts := DefaultOptions()

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaFileSize, opts.fileSize)
	metadata.PutInt(metaMaxTxEntries, opts.maxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.maxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.maxValueLen)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithSynced(opts.synced).
		WithFileMode(opts.fileMode).
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

		id, _, _, _, err := immuStore.Commit(kvs)
		if err != nil {
			require.Equal(t, errEmulatedAppendableError, err)
			emulatedFailures++
		} else {
			require.Equal(t, uint64(i+1-emulatedFailures), id)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)

	immuStore, err = Open(path, opts)
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(1, 1024)
	require.NoError(t, err)

	for i := 0; i < txCount-emulatedFailures; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		txEntries := tx.Entries()
		assert.Equal(t, eCount, len(txEntries))

		for j := 0; j < eCount; j++ {
			path := tx.Proof(j)

			key := txEntries[j].Key()

			value := make([]byte, txEntries[j].ValueLen)
			_, err := immuStore.ReadValueAt(value, txEntries[j].VOff, txEntries[j].HValue)
			require.NoError(t, err)

			kv := &KV{Key: key, Value: value}

			verifies := path.VerifyInclusion(uint64(tx.nentries-1), uint64(j), tx.Eh, kv.Digest())
			require.True(t, verifies)
		}
	}

	_, err = r.Read()
	require.Equal(t, io.EOF, err)

	require.Equal(t, uint64(txCount-emulatedFailures), immuStore.TxCount())

	err = immuStore.Close()
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

func BenchmarkSyncedAppend(b *testing.B) {
	immuStore, _ := Open("data_synced_bench", DefaultOptions())
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

			_, _, _, _, err := immuStore.Commit(kvs)
			if err != nil {
				panic(err)
			}
		}
	}
}

func BenchmarkAppend(b *testing.B) {
	opts := DefaultOptions().WithSynced(false)
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

			_, _, _, _, err := immuStore.Commit(kvs)
			if err != nil {
				panic(err)
			}
		}
	}
}
