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
)

func VerifyLinearProof(lproof *LinearProof, trustedTxID, targetTxID uint64, trustedAlh, targetAlh [sha256.Size]byte) bool {
	if lproof.TrustedTxID != trustedTxID || lproof.TargetTxID != targetTxID {
		return false
	}

	if lproof.TrustedTxID == 0 || lproof.TrustedTxID > lproof.TargetTxID ||
		len(lproof.Proof) == 0 || trustedAlh != lproof.Proof[0] {
		return false
	}

	calculatedAlh := lproof.Proof[0]

	bs := make([]byte, txIDSize+2*sha256.Size)

	for i := 1; i < len(lproof.Proof); i++ {
		binary.BigEndian.PutUint64(bs, lproof.TrustedTxID+uint64(i))
		copy(bs[txIDSize:], calculatedAlh[:])
		copy(bs[txIDSize+sha256.Size:], lproof.Proof[i][:])
		calculatedAlh = sha256.Sum256(bs)
	}

	return targetAlh == calculatedAlh
}

func VerifyDualProof(proof *DualProof, trustedTxID, targetTxID uint64, trustedAlh, targetAlh [sha256.Size]byte) bool {
	return true
}
