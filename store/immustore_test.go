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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImmudbStore(t *testing.T) {
	immuStore, err := Open("data", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data")

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 64

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

	err = immuStore.Close()
	require.NoError(t, err)

	_, _, _, _, err = immuStore.Commit([]*KV{{Key: nil, Value: nil}})
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data", DefaultOptions())
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(0, 1024)
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
			_, err := immuStore.ReadValueAt(value, txEntries[j].VOff)
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
		}
	}

	for i := 0; i < txCount; i++ {
		offi, _, err := immuStore.TxOffsetAndSize(uint64(i + 1))
		require.NoError(t, err)

		ri, err := immuStore.NewTxReader(offi, 1024)
		require.NoError(t, err)

		txi, err := ri.Read()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txi.ID)
	}

	trustedTx := NewTx(immuStore.maxTxEntries, immuStore.maxKeyLen)
	targetTx := NewTx(immuStore.maxTxEntries, immuStore.maxKeyLen)

	for i := 0; i < txCount; i++ {
		trustedTxID := uint64(i + 1)

		err := immuStore.ReadTx(trustedTxID, trustedTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), trustedTx.ID)

		for j := i + 1; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.ID)

			p, err := immuStore.LinearProof(trustedTxID, targetTxID)
			require.NoError(t, err)

			calculatedAlh := evalLinearProof(p)

			require.Equal(t, trustedTx.Alh(), p[0])
			require.Equal(t, targetTx.PrevAlh, calculatedAlh)
		}
	}

	err = immuStore.Close()
	require.NoError(t, err)
}

func evalLinearProof(proof [][sha256.Size]byte) (r [sha256.Size]byte) {
	bs := make([]byte, 2*sha256.Size)

	r = proof[0]

	for i := 1; i < len(proof); i += 2 {
		copy(bs, proof[i][:])
		copy(bs[sha256.Size:], proof[i+1][:])
		r = sha256.Sum256(bs)
	}

	return
}

func TestReOpenningImmudbStore(t *testing.T) {
	defer os.RemoveAll("data")

	itCount := 10
	txCount := 32
	eCount := 10

	for it := 0; it < itCount; it++ {
		immuStore, err := Open("data", DefaultOptions())
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
	path := "data"
	err := os.Mkdir(path, 0700)
	require.NoError(t, err)
	defer os.RemoveAll("data")

	opts := DefaultOptions()

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetadataKeyFileSize, opts.fileSize)
	metadata.PutInt(MetadataKeyMaxTxEntries, opts.maxTxEntries)
	metadata.PutInt(MetadataKeyMaxKeyLen, opts.maxKeyLen)
	metadata.PutInt(MetadataKeyMaxValueLen, opts.maxValueLen)

	appendableOpts := multiapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileMode(opts.fileMode).
		SetMetadata(metadata.Bytes())

	vLogPath := filepath.Join(path, "val_0")
	appendableOpts.SetFileExt("val")
	vLog, err := multiapp.Open(vLogPath, appendableOpts)
	require.NoError(t, err)

	txLogPath := filepath.Join(path, "tx")
	appendableOpts.SetFileExt("tx")
	txLog, err := multiapp.Open(txLogPath, appendableOpts)
	require.NoError(t, err)

	cLogPath := filepath.Join(path, "commit")
	appendableOpts.SetFileExt("idb")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	require.NoError(t, err)

	failingVLog := &FailingAppendable{vLog, 2}
	failingTxLog := &FailingAppendable{txLog, 5}
	failingCLog := &FailingAppendable{cLog, 5}

	immuStore, err := OpenWith([]appendable.Appendable{failingVLog}, failingTxLog, failingCLog, opts)
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

	r, err := immuStore.NewTxReader(0, 1024)
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
			_, err := immuStore.ReadValueAt(value, txEntries[j].VOff)
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
	immuStore, _ := Open("data", DefaultOptions())
	defer os.RemoveAll("data")

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
	immuStore, _ := Open("data", DefaultOptions().SetSynced(false))
	defer os.RemoveAll("data")

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
