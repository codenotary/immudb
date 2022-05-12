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

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/htree"
)

func VerifyInclusion(proof *htree.InclusionProof, entryDigest, root [sha256.Size]byte) bool {
	return htree.VerifyInclusion(proof, entryDigest, root)
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
		var bs [txIDSize + 2*sha256.Size]byte
		binary.BigEndian.PutUint64(bs[:], proof.SourceTxID+uint64(i))
		copy(bs[txIDSize:], calculatedAlh[:])
		copy(bs[txIDSize+sha256.Size:], proof.Terms[i][:]) // innerHash = hash(ts + mdLen + md + nentries + eH + blTxID + blRoot)
		calculatedAlh = sha256.Sum256(bs[:])               // hash(txID + prevAlh + innerHash)
	}

	return targetAlh == calculatedAlh
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
		verfifies := ahtree.VerifyConsistency(
			proof.ConsistencyProof,
			proof.SourceTxHeader.BlTxID,
			proof.TargetTxHeader.BlTxID,
			proof.SourceTxHeader.BlRoot,
			proof.TargetTxHeader.BlRoot,
		)

		if !verfifies {
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
		return VerifyLinearProof(proof.LinearProof, proof.TargetTxHeader.BlTxID, targetTxID, proof.TargetBlTxAlh, targetAlh)
	}

	return VerifyLinearProof(proof.LinearProof, sourceTxID, targetTxID, sourceAlh, targetAlh)
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

	hvalue := sha256.Sum256(kv.Value)
	copy(b[i:], hvalue[:])
	i += sha256.Size

	return sha256.Sum256(b[:i])
}
