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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/fs"
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

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(t.TempDir(), opts)
	require.NoError(t, err)

	defer immustoreClose(t, immuStore)

	require.NotNil(t, immuStore)

	txCount := 10
	eCount := 4

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

}

func TestVerifyDualProofWithAdditionalLinearInclusionProof(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	copier := fs.NewStandardCopier()
	require.NoError(t, copier.CopyDir("../../test/data_long_linear_proof", dir))

	opts := DefaultOptions().WithSynced(false).WithMaxConcurrency(1)
	immuStore, err := Open(dir, opts)
	require.NoError(t, err)
	defer immustoreClose(t, immuStore)

	maxTxID := immuStore.TxCount()

	t.Run("data check", func(t *testing.T) {
		require.EqualValues(t, 30, maxTxID, "Invalid dataset - expected 30 transactions")

		t.Run("transactions 1-10 do not use linear proof longer than 1", func(t *testing.T) {
			for txID := uint64(1); txID <= 10; txID++ {
				hdr, err := immuStore.ReadTxHeader(txID, true, false)
				require.NoError(t, err)
				require.Equal(t, txID-1, hdr.BlTxID)
			}
		})

		t.Run("transactions 11-20 use long linear proof", func(t *testing.T) {
			for txID := uint64(11); txID <= 20; txID++ {
				hdr, err := immuStore.ReadTxHeader(txID, true, false)
				require.NoError(t, err)
				require.EqualValues(t, 10, hdr.BlTxID)
			}
		})

		t.Run("transactions 21-30 do not use linear proof longer than 1", func(t *testing.T) {
			for txID := uint64(21); txID <= 30; txID++ {
				hdr, err := immuStore.ReadTxHeader(txID, true, false)
				require.NoError(t, err)
				require.Equal(t, txID-1, hdr.BlTxID)
			}
		})

	})

	t.Run("exhaustive consistency proof check", func(t *testing.T) {
		for sourceTxID := uint64(1); sourceTxID < maxTxID; sourceTxID++ {
			for targetTxID := sourceTxID; targetTxID < maxTxID; targetTxID++ {

				sourceTx := tempTxHolder(t, immuStore)
				targetTx := tempTxHolder(t, immuStore)

				err := immuStore.ReadTx(sourceTxID, false, sourceTx)
				require.NoError(t, err)

				err = immuStore.ReadTx(targetTxID, false, targetTx)
				require.NoError(t, err)

				dproof, err := immuStore.DualProof(sourceTx.Header(), targetTx.Header())
				require.NoError(t, err)

				verifies := VerifyDualProof(dproof, sourceTxID, targetTxID, sourceTx.header.Alh(), targetTx.header.Alh())
				require.True(t, verifies)
			}
		}
	})

}
