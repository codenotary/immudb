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
)

func VerifyLinearProof(lProof [][sha256.Size]byte, trustedTxID, targetTxID uint64, trustedAlh, targetPrevAlh [sha256.Size]byte) bool {
	if trustedTxID > targetTxID || trustedTxID == 0 {
		return false
	}

	if trustedTxID < targetTxID && len(lProof) == 0 {
		return false
	}

	if trustedAlh != lProof[0] {
		return false
	}

	bs := make([]byte, 2*sha256.Size)

	calculatedAlh := lProof[0]

	for i := 1; i < len(lProof); i += 2 {
		copy(bs, lProof[i][:])
		copy(bs[sha256.Size:], lProof[i+1][:])
		calculatedAlh = sha256.Sum256(bs)
	}

	return targetPrevAlh == calculatedAlh
}
