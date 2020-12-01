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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyLinearProofEdgeCases(t *testing.T) {
	require.False(t, VerifyLinearProof(nil, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyLinearProof(&LinearProof{}, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))

	require.True(t,
		VerifyLinearProof(
			&LinearProof{Proof: [][sha256.Size]byte{sha256.Sum256(nil)}, SourceTxID: 1, TargetTxID: 1},
			1,
			1,
			sha256.Sum256(nil),
			sha256.Sum256(nil),
		),
	)
}

func TestVerifyDualProofEdgeCases(t *testing.T) {
	require.False(t, VerifyDualProof(nil, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyDualProof(&DualProof{}, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))

	opts := DefaultOptions().WithSynced(false).WithMaxLinearProofLen(0).WithMaxConcurrency(1)
	immuStore, err := Open("data_dualproof_edge_cases", opts)
	require.NoError(t, err)
	defer os.RemoveAll("data_dualproof_edge_cases")

	require.NotNil(t, immuStore)

	txCount := 10
	eCount := 4

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

		// Alter proof
		dproof.SourceTxMetadata.BlTxID++
		verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
		require.False(t, verifies)

		// Restore proof
		dproof.SourceTxMetadata.BlTxID--

		// Alter proof
		dproof.TargetTxMetadata.BlTxID++
		verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.Alh(), targetTx.Alh())
		require.False(t, verifies)
	}

	err = immuStore.Close()
	require.NoError(t, err)
}
