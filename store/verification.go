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

	"codenotary.io/immudb-v2/ahtree"
)

func VerifyLinearProof(proof *LinearProof, trustedTxID, targetTxID uint64, trustedAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil || proof.TrustedTxID != trustedTxID || proof.TargetTxID != targetTxID {
		return false
	}

	if proof.TrustedTxID == 0 || proof.TrustedTxID > proof.TargetTxID ||
		len(proof.Proof) == 0 || trustedAlh != proof.Proof[0] {
		return false
	}

	calculatedAlh := proof.Proof[0]

	bs := make([]byte, txIDSize+2*sha256.Size)

	for i := 1; i < len(proof.Proof); i++ {
		binary.BigEndian.PutUint64(bs, proof.TrustedTxID+uint64(i))
		copy(bs[txIDSize:], calculatedAlh[:])
		copy(bs[txIDSize+sha256.Size:], proof.Proof[i][:])
		calculatedAlh = sha256.Sum256(bs)
	}

	return targetAlh == calculatedAlh
}

func VerifyDualProof(proof *DualProof, trustedTxID, targetTxID uint64, trustedAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil || proof.TrustedTxID != trustedTxID || proof.TargetTxID != targetTxID {
		return false
	}

	if proof.TrustedTxID == 0 || proof.TrustedTxID > proof.TargetTxID {
		return false
	}

	cTrustedAlh := alh(proof.TrustedTxID, proof.TrustedPrevAlh, proof.TrustedBlTxID, proof.TrustedBlRoot, proof.TrustedTxH)
	if trustedAlh != cTrustedAlh {
		return false
	}

	cTargetAlh := alh(proof.TargetTxID, proof.TargetPrevAlh, proof.TargetBlTxID, proof.TargetBlRoot, proof.TargetTxH)
	if targetAlh != cTargetAlh {
		return false
	}

	if proof.TrustedTxID < proof.TargetBlTxID {
		cTargetBlRoot := ahtree.EvalInclusion(proof.BinaryInclusionProof, trustedTxID, proof.TargetBlTxID, sha256.Sum256(trustedAlh[:]))
		if proof.TargetBlRoot != cTargetBlRoot {
			return false
		}
	}

	if proof.TrustedBlTxID > 0 {
		cTrustedBlRoot, c2TargetBlRoot := ahtree.EvalConsistency(proof.BinaryConsistencyProof, proof.TrustedBlTxID, proof.TargetBlTxID)
		if proof.TrustedBlRoot != cTrustedBlRoot || proof.TargetBlRoot != c2TargetBlRoot {
			return false
		}
	}

	if proof.TargetBlTxID > 0 {
		c2TargetBlRoot := ahtree.EvalLastInclusion(proof.BinaryLastInclusionProof, proof.TargetBlTxID, sha256.Sum256(proof.TargetBlTxAlh[:]))
		if proof.TargetBlRoot != c2TargetBlRoot {
			return false
		}
	}

	if proof.TrustedTxID < proof.TargetBlTxID {
		return VerifyLinearProof(proof.LinearProof, proof.TargetBlTxID, targetTxID, proof.TargetBlTxAlh, targetAlh)
	}

	return VerifyLinearProof(proof.LinearProof, trustedTxID, targetTxID, trustedAlh, targetAlh)
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
