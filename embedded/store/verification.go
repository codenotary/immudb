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

	"github.com/codenotary/immudb/embedded/ahtree"
)

func VerifyLinearProof(proof *LinearProof, sourceTxID, targetTxID uint64, sourceAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil || proof.SourceTxID != sourceTxID || proof.TargetTxID != targetTxID {
		return false
	}

	if proof.SourceTxID == 0 || proof.SourceTxID > proof.TargetTxID ||
		len(proof.Proof) == 0 || sourceAlh != proof.Proof[0] {
		return false
	}

	calculatedAlh := proof.Proof[0]

	for i := 1; i < len(proof.Proof); i++ {
		var bs [txIDSize + 2*sha256.Size]byte
		binary.BigEndian.PutUint64(bs[:], proof.SourceTxID+uint64(i))
		copy(bs[txIDSize:], calculatedAlh[:])
		copy(bs[txIDSize+sha256.Size:], proof.Proof[i][:]) // innerHash = hash(blTxID + blRoot + txH)
		calculatedAlh = sha256.Sum256(bs[:])               // hash(txID + prevAlh + innerHash)
	}

	return targetAlh == calculatedAlh
}

func VerifyDualProof(proof *DualProof, sourceTxID, targetTxID uint64, sourceAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil || proof.SourceTxMetadata.ID != sourceTxID || proof.TargetTxMetadata.ID != targetTxID {
		return false
	}

	if proof.SourceTxMetadata.ID == 0 || proof.SourceTxMetadata.ID > proof.TargetTxMetadata.ID {
		return false
	}

	cSourceAlh := alh(
		proof.SourceTxMetadata.ID,
		proof.SourceTxMetadata.PrevAlh,
		proof.SourceTxMetadata.BlTxID,
		proof.SourceTxMetadata.BlRoot,
		proof.SourceTxMetadata.TxH,
	)
	if sourceAlh != cSourceAlh {
		return false
	}

	cTargetAlh := alh(
		proof.TargetTxMetadata.ID,
		proof.TargetTxMetadata.PrevAlh,
		proof.TargetTxMetadata.BlTxID,
		proof.TargetTxMetadata.BlRoot,
		proof.TargetTxMetadata.TxH,
	)
	if targetAlh != cTargetAlh {
		return false
	}

	if sourceTxID < proof.TargetTxMetadata.BlTxID {
		verifies := ahtree.VerifyInclusion(
			proof.BinaryInclusionProof,
			sourceTxID,
			proof.TargetTxMetadata.BlTxID,
			leafFor(sourceAlh),
			proof.TargetTxMetadata.BlRoot,
		)

		if !verifies {
			return false
		}
	}

	if proof.SourceTxMetadata.BlTxID > 0 {
		verfifies := ahtree.VerifyConsistency(
			proof.BinaryConsistencyProof,
			proof.SourceTxMetadata.BlTxID,
			proof.TargetTxMetadata.BlTxID,
			proof.SourceTxMetadata.BlRoot,
			proof.TargetTxMetadata.BlRoot,
		)

		if !verfifies {
			return false
		}
	}

	if proof.TargetTxMetadata.BlTxID > 0 {
		verifies := ahtree.VerifyLastInclusion(
			proof.BinaryLastInclusionProof,
			proof.TargetTxMetadata.BlTxID,
			leafFor(proof.TargetBlTxAlh),
			proof.TargetTxMetadata.BlRoot,
		)

		if !verifies {
			return false
		}
	}

	if sourceTxID < proof.TargetTxMetadata.BlTxID {
		return VerifyLinearProof(proof.LinearProof, proof.TargetTxMetadata.BlTxID, targetTxID, proof.TargetBlTxAlh, targetAlh)
	}

	return VerifyLinearProof(proof.LinearProof, sourceTxID, targetTxID, sourceAlh, targetAlh)
}

func alh(txID uint64, prevAlh [sha256.Size]byte, blTxID uint64, blRoot, txH [sha256.Size]byte) [sha256.Size]byte {
	var bi [txIDSize + 2*sha256.Size]byte

	binary.BigEndian.PutUint64(bi[:], txID)
	copy(bi[txIDSize:], prevAlh[:])

	var bj [txIDSize + 2*sha256.Size]byte
	binary.BigEndian.PutUint64(bj[:], blTxID)
	copy(bj[txIDSize:], blRoot[:])
	copy(bj[txIDSize+sha256.Size:], txH[:])
	innerHash := sha256.Sum256(bj[:]) // hash(blTxID + blRoot + txH)

	copy(bi[txIDSize+sha256.Size:], innerHash[:]) // hash(txID + prevAlh + innerHash)

	return sha256.Sum256(bi[:])
}

func leafFor(d [sha256.Size]byte) [sha256.Size]byte {
	var b [1 + sha256.Size]byte
	b[0] = ahtree.LeafPrefix
	copy(b[1:], d[:])
	return sha256.Sum256(b[:])
}
