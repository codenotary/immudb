/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"fmt"

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/htree"
)

func VerifyInclusion(proof *htree.InclusionProof, entryDigest, root [sha256.Size]byte) bool {
	return htree.VerifyInclusion(proof, entryDigest, root)
}

func advanceLinearHash(alh [sha256.Size]byte, txID uint64, term [sha256.Size]byte) [sha256.Size]byte {
	var bs [txIDSize + 2*sha256.Size]byte
	binary.BigEndian.PutUint64(bs[:], txID)
	copy(bs[txIDSize:], alh[:])
	copy(bs[txIDSize+sha256.Size:], term[:]) // innerHash = hash(ts + mdLen + md + nentries + eH + blTxID + blRoot)
	return sha256.Sum256(bs[:])              // hash(txID + prevAlh + innerHash)
}

func VerifyLinearProof(proof *LinearProof, sourceTxID, targetTxID uint64, sourceAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil || proof.SourceTxID != sourceTxID || proof.TargetTxID != targetTxID {
		return false
	}

	if proof.SourceTxID == 0 || proof.SourceTxID > proof.TargetTxID ||
		len(proof.Terms) == 0 || sourceAlh != proof.Terms[0] {
		return false
	}

	if uint64(len(proof.Terms)) != targetTxID-sourceTxID+1 {
		return false
	}

	calculatedAlh := proof.Terms[0]

	for i := 1; i < len(proof.Terms); i++ {
		calculatedAlh = advanceLinearHash(calculatedAlh, proof.SourceTxID+uint64(i), proof.Terms[i])
	}

	return targetAlh == calculatedAlh
}

func VerifyLinearAdvanceProof(
	proof *LinearAdvanceProof,
	startTxID uint64,
	endTxID uint64,
	endAlh [sha256.Size]byte,
	treeRoot [sha256.Size]byte,
	treeSize uint64,
) bool {
	//
	//       Old
	//  \ Merkle Tree
	//   \
	//    \
	//     \              Additional Inclusion proof
	//      \                  for those nodes
	//       \                in new Merkle Tree
	//        \            ......................
	//         \          /                      \
	//          \
	//           \+--+     +--+     +--+     +--+     +--+
	// -----------|  |-----|  |-----|  |-----|  |-----|  |
	//            +--+     +--+     +--+     +--+     +--+
	//
	//         startTxID                            endTxID
	//

	// This must not happen - that's an invalid proof
	if endTxID < startTxID {
		return false
	}

	// Linear Advance Proof is not needed
	if endTxID <= startTxID+1 {
		return true
	}

	// Check more preconditions that would indicate broken proof
	if proof == nil ||
		len(proof.LinearProofTerms) != int(endTxID-startTxID) ||
		len(proof.InclusionProofs) != int(endTxID-startTxID)-1 {
		return false
	}

	calculatedAlh := proof.LinearProofTerms[0] // alh at startTx+1
	for txID := startTxID + 1; txID < endTxID; txID++ {
		// Ensure the node in the chain is included in the target Merkle Tree
		if !ahtree.VerifyInclusion(
			proof.InclusionProofs[txID-startTxID-1],
			txID,
			treeSize,
			leafFor(calculatedAlh),
			treeRoot,
		) {
			return false
		}

		// Get the Alh for the next transaction
		calculatedAlh = advanceLinearHash(calculatedAlh, txID+1, proof.LinearProofTerms[txID-startTxID])
	}

	// We must end up with the final Alh - that one is also checked for inclusion but in different part of the proof
	return calculatedAlh == endAlh
}

func VerifyDualProof(proof *DualProof, sourceTxID, targetTxID uint64, sourceAlh, targetAlh [sha256.Size]byte) bool {
	if proof == nil ||
		proof.SourceTxHeader == nil ||
		proof.TargetTxHeader == nil ||
		proof.SourceTxHeader.ID != sourceTxID ||
		proof.TargetTxHeader.ID != targetTxID {
		return false
	}

	if proof.SourceTxHeader.ID == 0 || proof.SourceTxHeader.ID > proof.TargetTxHeader.ID {
		return false
	}

	cSourceAlh := proof.SourceTxHeader.Alh()
	if sourceAlh != cSourceAlh {
		return false
	}

	cTargetAlh := proof.TargetTxHeader.Alh()
	if targetAlh != cTargetAlh {
		return false
	}

	if sourceTxID < proof.TargetTxHeader.BlTxID {
		verifies := ahtree.VerifyInclusion(
			proof.InclusionProof,
			sourceTxID,
			proof.TargetTxHeader.BlTxID,
			leafFor(sourceAlh),
			proof.TargetTxHeader.BlRoot,
		)

		if !verifies {
			return false
		}
	}

	if proof.SourceTxHeader.BlTxID > 0 {
		verifies := ahtree.VerifyConsistency(
			proof.ConsistencyProof,
			proof.SourceTxHeader.BlTxID,
			proof.TargetTxHeader.BlTxID,
			proof.SourceTxHeader.BlRoot,
			proof.TargetTxHeader.BlRoot,
		)

		if !verifies {
			return false
		}
	}

	if proof.TargetTxHeader.BlTxID > 0 {
		verifies := ahtree.VerifyLastInclusion(
			proof.LastInclusionProof,
			proof.TargetTxHeader.BlTxID,
			leafFor(proof.TargetBlTxAlh),
			proof.TargetTxHeader.BlRoot,
		)

		if !verifies {
			return false
		}
	}

	if sourceTxID < proof.TargetTxHeader.BlTxID {
		verifies := VerifyLinearProof(proof.LinearProof, proof.TargetTxHeader.BlTxID, targetTxID, proof.TargetBlTxAlh, targetAlh)
		if !verifies {
			return false
		}

		// Verify that the part of the linear proof consumed by the new merkle tree is consistent with that Merkle Tree
		// In this case, this is the whole chain to the SourceTxID from the previous Merkle Tree.
		// The sourceTxID consistency is already proven using proof.InclusionProof
		if !VerifyLinearAdvanceProof(
			proof.LinearAdvanceProof,
			proof.SourceTxHeader.BlTxID,
			sourceTxID,
			sourceAlh,
			proof.TargetTxHeader.BlRoot,
			proof.TargetTxHeader.BlTxID,
		) {
			return false
		}

	} else {

		verifies := VerifyLinearProof(proof.LinearProof, sourceTxID, targetTxID, sourceAlh, targetAlh)
		if !verifies {
			return false
		}

		// Verify that the part of the linear proof consumed by the new merkle tree is consistent with that Merkle Tree
		// In this case, this is the whole linear chain between the old Merkle Tree and the new Merkle Tree. The last entry
		// in the new Merkle Tree is already proven through the LastInclusionProof, the remaining part of the liner proof
		// that goes outside of the target Merkle Tree will be validated in future DualProof validations
		if !VerifyLinearAdvanceProof(
			proof.LinearAdvanceProof,
			proof.SourceTxHeader.BlTxID,
			proof.TargetTxHeader.BlTxID,
			proof.TargetBlTxAlh,
			proof.TargetTxHeader.BlRoot,
			proof.TargetTxHeader.BlTxID,
		) {
			return false
		}
	}

	return true
}

func leafFor(d [sha256.Size]byte) [sha256.Size]byte {
	var b [1 + sha256.Size]byte
	b[0] = ahtree.LeafPrefix
	copy(b[1:], d[:])
	return sha256.Sum256(b[:])
}

type EntrySpecDigest func(kv *EntrySpec) [sha256.Size]byte

func EntrySpecDigestFor(version int) (EntrySpecDigest, error) {
	switch version {
	case 0:
		return EntrySpecDigest_v0, nil
	case 1:
		return EntrySpecDigest_v1, nil
	}

	return nil, ErrUnsupportedTxVersion
}

func EntrySpecDigest_v0(kv *EntrySpec) [sha256.Size]byte {
	b := make([]byte, len(kv.Key)+sha256.Size)
	copy(b[:], kv.Key)
	hvalue := sha256.Sum256(kv.Value)
	copy(b[len(kv.Key):], hvalue[:])
	return sha256.Sum256(b)
}

func EntrySpecDigest_v1(kv *EntrySpec) [sha256.Size]byte {
	var mdbs []byte

	if kv.Metadata != nil {
		mdbs = kv.Metadata.Bytes()
	}

	mdLen := len(mdbs)
	kLen := len(kv.Key)

	b := make([]byte, sszSize+mdLen+sszSize+kLen+sha256.Size)
	i := 0

	binary.BigEndian.PutUint16(b[i:], uint16(mdLen))
	i += sszSize

	copy(b[i:], mdbs)
	i += mdLen

	binary.BigEndian.PutUint16(b[i:], uint16(kLen))
	i += sszSize

	copy(b[i:], kv.Key)
	i += len(kv.Key)

	var hvalue [sha256.Size]byte

	if kv.IsValueTruncated {
		hvalue = kv.HashValue
	} else {
		hvalue = sha256.Sum256(kv.Value)
	}

	copy(b[i:], hvalue[:])
	i += sha256.Size

	return sha256.Sum256(b[:i])
}

func VerifyDualProofV2(proof *DualProofV2, sourceTxID, targetTxID uint64, sourceAlh, targetAlh [sha256.Size]byte) error {
	if proof == nil ||
		proof.SourceTxHeader == nil ||
		proof.TargetTxHeader == nil ||
		proof.SourceTxHeader.ID == 0 ||
		proof.SourceTxHeader.ID != sourceTxID ||
		proof.TargetTxHeader.ID != targetTxID {
		return ErrIllegalArguments
	}

	if sourceTxID > targetTxID {
		return ErrSourceTxNewerThanTargetTx
	}

	cSourceAlh := proof.SourceTxHeader.Alh()
	if sourceAlh != cSourceAlh {
		return ErrIllegalArguments
	}

	cTargetAlh := proof.TargetTxHeader.Alh()
	if targetAlh != cTargetAlh {
		return ErrIllegalArguments
	}

	if proof.SourceTxHeader.ID-1 != proof.SourceTxHeader.BlTxID || proof.TargetTxHeader.ID-1 != proof.TargetTxHeader.BlTxID {
		return ErrUnexpectedLinkingError
	}

	if sourceTxID == targetTxID {
		return nil
	}

	verifies := ahtree.VerifyInclusion(
		proof.InclusionProof,
		sourceTxID,
		proof.TargetTxHeader.BlTxID,
		leafFor(sourceAlh),
		proof.TargetTxHeader.BlRoot,
	)

	if !verifies {
		return fmt.Errorf("inclusion proof does NOT validate")
	}

	if sourceTxID == 1 {
		// corner case to validate the first transaction
		// alh is empty when the digest of the first transaction is calculated
		verifies = ahtree.VerifyConsistency(
			proof.ConsistencyProof,
			sourceTxID,
			proof.TargetTxHeader.BlTxID,
			leafFor(sourceAlh),
			proof.TargetTxHeader.BlRoot,
		)
	} else {
		verifies = ahtree.VerifyConsistency(
			proof.ConsistencyProof,
			proof.SourceTxHeader.BlTxID,
			proof.TargetTxHeader.BlTxID,
			proof.SourceTxHeader.BlRoot,
			proof.TargetTxHeader.BlRoot,
		)
	}
	if !verifies {
		return fmt.Errorf("consistency proof does NOT validate")
	}

	return nil
}
