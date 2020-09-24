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
	"github.com/codenotary/merkletree"
	"github.com/stretchr/testify/require"
)

func TestImmudbStore(t *testing.T) {
	immuStore, err := Open("data", DefaultOptions())
	require.NoError(t, err)
	defer os.RemoveAll("data")

	require.NotNil(t, immuStore)

	txCount := 32
	eCount := 100

	_, _, _, _, err = immuStore.Commit(nil)
	require.Equal(t, ErrorNoEntriesProvided, err)

	for i := 0; i < txCount; i++ {
		kvs := make([]*kv, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kvs[j] = &kv{key: k, value: v}
		}

		id, _, _, _, err := immuStore.Commit(kvs)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), id)
	}

	err = immuStore.Close()
	require.NoError(t, err)

	_, _, _, _, err = immuStore.Commit([]*kv{{key: nil, value: nil}})
	require.Equal(t, ErrAlreadyClosed, err)

	immuStore, err = Open("data", DefaultOptions())
	require.NoError(t, err)

	r, err := immuStore.NewTxReader(0, 1024)
	require.NoError(t, err)

	for i := 0; i < txCount; i++ {
		tx, err := r.Read()
		require.NoError(t, err)
		require.NotNil(t, tx)

		for j := 0; j < eCount; j++ {
			path := tx.Proof(j)

			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(i<<4+j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

			kv := &kv{key: k, value: v}

			verifies := path.VerifyInclusion(uint64(tx.htree.width-1), uint64(j), merkletree.Root(tx.htree), kv.digest())
			require.True(t, verifies)

			value := make([]byte, tx.es[j].valueLen)
			n, err := immuStore.ReadValueAt(value, tx.es[j].voff)
			require.NoError(t, err)
			require.Equal(t, tx.es[j].valueLen, n)
		}
	}

	for i := 0; i < txCount; i++ {
		offi, _, err := immuStore.TxOffsetAndSize(uint64(i + 1))
		require.NoError(t, err)

		ri, err := immuStore.NewTxReader(offi, 1024)
		require.NoError(t, err)

		txi, err := ri.Read()
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), txi.id)
	}

	trustedTx := mallocTx(immuStore.maxTxEntries, immuStore.maxKeyLen)
	targetTx := mallocTx(immuStore.maxTxEntries, immuStore.maxKeyLen)

	for i := 0; i < txCount; i++ {
		trustedTxID := uint64(i + 1)

		err := immuStore.ReadTx(trustedTxID, trustedTx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), trustedTx.id)

		for j := i + 1; j < txCount; j++ {
			targetTxID := uint64(j + 1)

			err := immuStore.ReadTx(targetTxID, targetTx)
			require.NoError(t, err)
			require.Equal(t, uint64(j+1), targetTx.id)

			p, err := immuStore.LinearProof(trustedTxID, targetTxID)
			require.NoError(t, err)

			calculatedAlh := evalLinearProof(p)

			require.Equal(t, trustedTx.Alh(), p[0])
			require.Equal(t, targetTx.alh, calculatedAlh)
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
	txCount := 50
	eCount := 10

	for it := 0; it < itCount; it++ {
		immuStore, err := Open("data", DefaultOptions())
		require.NoError(t, err)

		for i := 0; i < txCount; i++ {
			kvs := make([]*kv, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &kv{key: k, value: v}
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
	appendableOpts := appendable.DefaultOptions().SetReadOnly(opts.readOnly).SetFileMode(opts.fileMode)

	txLogFilename := filepath.Join(path, "immudb.itx")
	txLog, err := appendable.Open(txLogFilename, appendableOpts)
	require.NoError(t, err)

	vLogFilename := filepath.Join(path, "immudb.val")
	vLog, err := appendable.Open(vLogFilename, appendableOpts)
	require.NoError(t, err)

	cLogFilename := filepath.Join(path, "immudb.ctx")
	cLog, err := appendable.Open(cLogFilename, appendableOpts)
	require.NoError(t, err)

	failingTxLog := &FailingAppendable{txLog, 5}
	failingVLog := &FailingAppendable{vLog, 2}
	failingCLog := &FailingAppendable{cLog, 5}

	immuStore, err := open(failingTxLog, failingVLog, failingCLog, opts)
	require.NoError(t, err)

	txCount := 1000
	eCount := 100

	emulatedFailures := 0

	for i := 0; i < txCount; i++ {
		kvs := make([]*kv, eCount)

		for j := 0; j < eCount; j++ {
			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(j+1))

			kvs[j] = &kv{key: k, value: v}
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

		for j := 0; j < eCount; j++ {
			path := tx.Proof(j)

			k := make([]byte, 4)
			binary.BigEndian.PutUint32(k, uint32(j))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(j+1))

			kv := &kv{key: k, value: v}

			verifies := path.VerifyInclusion(uint64(tx.htree.width-1), uint64(j), merkletree.Root(tx.htree), kv.digest())
			require.True(t, verifies)

			value := make([]byte, tx.es[j].valueLen)
			n, err := immuStore.ReadValueAt(value, tx.es[j].voff)
			require.NoError(t, err)
			require.Equal(t, tx.es[j].valueLen, n)
		}
	}

	_, err = r.Read()
	require.Equal(t, io.EOF, err)

	err = immuStore.Close()
	require.NoError(t, err)
}

var errEmulatedAppendableError = errors.New("emulated appendable error")

type FailingAppendable struct {
	*appendable.AppendableFile
	errorRate int
}

func (la *FailingAppendable) Append(bs []byte) (off int64, n int, err error) {
	if rand.Intn(100) < la.errorRate {
		return 0, 0, errEmulatedAppendableError
	}

	return la.AppendableFile.Append(bs)
}

func BenchmarkAppend(b *testing.B) {
	immuStore, _ := Open("data", DefaultOptions())
	defer os.RemoveAll("data")

	for i := 0; i < b.N; i++ {
		kCount := 100
		eCount := 1000

		for i := 0; i < kCount; i++ {
			kvs := make([]*kv, eCount)

			for j := 0; j < eCount; j++ {
				k := make([]byte, 8)
				binary.BigEndian.PutUint32(k, uint32(i<<4+j))

				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, uint64(i<<4+(eCount-j)))

				kvs[j] = &kv{key: k, value: v}
			}

			_, _, _, _, err := immuStore.Commit(kvs)
			if err != nil {
				panic(err)
			}
		}
	}
}
