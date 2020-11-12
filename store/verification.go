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

	if proof.JointTxID == 0 {
		return VerifyLinearProof(proof.LinearProof, proof.TrustedTxID, proof.TargetTxID, trustedAlh, targetAlh)
	}

	trustedLeaf := sha256.Sum256(trustedAlh[:])
	jointBlRoot := ahtree.EvalInclusion(proof.BinaryInclusionProof, proof.TrustedTxID, proof.JointTxID-1, trustedLeaf)

	if trustedTxID > 1 {
		cTrustedBlRoot, cJointBlRoot := ahtree.EvalConsistency(proof.BinaryConsistencyProof, proof.TrustedTxID-1, proof.JointTxID-1)

		if jointBlRoot != cJointBlRoot {
			return false
		}

		cTrustedAlh := alh(trustedTxID, proof.TrustedPrevAlh, cTrustedBlRoot, proof.TrustedTxH)

		if trustedAlh != cTrustedAlh {
			return false
		}
	}

	jointAlh := alh(proof.JointTxID, proof.JointPrevAlh, jointBlRoot, proof.JointTxH)

	if proof.JointTxID == targetTxID {
		return targetAlh == jointAlh
	}

	return VerifyLinearProof(proof.LinearProof, proof.JointTxID, proof.TargetTxID, jointAlh, targetAlh)
}

func alh(txID uint64, prevAlh [sha256.Size]byte, blRoot, txH [sha256.Size]byte) [sha256.Size]byte {
	var bi [txIDSize + 2*sha256.Size]byte
	i := 0

	binary.BigEndian.PutUint64(bi[:], txID)
	i += txIDSize
	copy(bi[i:], prevAlh[:])
	i += sha256.Size

	var bj [2 * sha256.Size]byte
	j := 0

	copy(bj[:], blRoot[:])
	j += sha256.Size
	copy(bj[j:], txH[:])

	bhash := sha256.Sum256(bj[:])

	copy(bi[i:], bhash[:])

	return sha256.Sum256(bi[:])
}
