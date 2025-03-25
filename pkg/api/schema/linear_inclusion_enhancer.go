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

package schema

import (
	"context"
	"crypto/sha256"

	"github.com/codenotary/immudb/embedded/store"
)

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func FillMissingLinearAdvanceProof(
	ctx context.Context,
	proof *store.DualProof,
	sourceTxID uint64,
	targetTxID uint64,
	imc ImmuServiceClient,
) error {
	if proof.LinearAdvanceProof != nil {
		// The proof is already present, no need to fill it in
		return nil
	}

	// Early preconditions that indicate a broken proof anyway
	if proof == nil ||
		proof.SourceTxHeader == nil ||
		proof.TargetTxHeader == nil ||
		proof.SourceTxHeader.ID != sourceTxID ||
		proof.TargetTxHeader.ID != targetTxID {
		return nil
	}

	// Find the range startTxID / endTxID to fill with linear inclusion proof
	startTxID := proof.SourceTxHeader.BlTxID
	endTxID := minUint64(sourceTxID, proof.TargetTxHeader.BlTxID)

	if endTxID <= startTxID+1 {
		// Linear Advance Proof is not needed
		return nil
	}

	lAdvProof := &store.LinearAdvanceProof{
		InclusionProofs: make([][][sha256.Size]byte, endTxID-startTxID-1),
	}

	// Fill in inclusion proofs for subsequent transactions
	for txID := startTxID + 1; txID < endTxID; txID++ {
		partialProof, err := imc.VerifiableTxById(ctx, &VerifiableTxRequest{
			Tx:           targetTxID,
			ProveSinceTx: txID,
			// Add entries spec to exclude any entries
			EntriesSpec: &EntriesSpec{KvEntriesSpec: &EntryTypeSpec{Action: EntryTypeAction_EXCLUDE}},
		})
		if err != nil {
			return err
		}
		lAdvProof.InclusionProofs[txID-startTxID-1] = DigestsFromProto(partialProof.DualProof.InclusionProof)
	}

	// Get the linear proof for the whole chain
	partialProof, err := imc.VerifiableTxById(ctx, &VerifiableTxRequest{
		Tx:           endTxID,
		ProveSinceTx: startTxID + 1,
		// Add entries spec to exclude any entries
		EntriesSpec: &EntriesSpec{KvEntriesSpec: &EntryTypeSpec{Action: EntryTypeAction_EXCLUDE}},
	})
	if err != nil {
		// Note: We don't check whether the proof returned from the server is correct here.
		// If there's any inconsistency, the proof validation will fail detecting incorrect
		// response from the server.
		return err
	}
	lAdvProof.LinearProofTerms = DigestsFromProto(partialProof.DualProof.LinearProof.Terms)

	proof.LinearAdvanceProof = lAdvProof
	return nil
}
