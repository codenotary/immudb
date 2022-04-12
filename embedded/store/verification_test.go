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
			&LinearProof{Terms: [][sha256.Size]byte{sha256.Sum256(nil)}, SourceTxID: 1, TargetTxID: 1},
			1,
			1,
			sha256.Sum256(nil),
			sha256.Sum256(nil),
		),
	)

	require.False(t,
		VerifyLinearProof(
			&LinearProof{Terms: [][sha256.Size]byte{sha256.Sum256(nil)}, SourceTxID: 1, TargetTxID: 2},
			1,
			2,
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

		// Alter proof
		dproof.SourceTxHeader.BlTxID++
		verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
		require.False(t, verifies)

		// Restore proof
		dproof.SourceTxHeader.BlTxID--

		// Alter proof
		dproof.TargetTxHeader.BlTxID++
		verifies = VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
		require.False(t, verifies)

		// Restore proof
		dproof.TargetTxHeader.BlTxID--
	}

	err = immuStore.Close()
	require.NoError(t, err)
}
