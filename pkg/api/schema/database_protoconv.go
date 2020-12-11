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

package schema

import (
	"crypto/sha256"

	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/store"
)

func TxTo(tx *store.Tx) *Tx {
	entries := make([]*TxEntry, len(tx.Entries()))

	for i, e := range tx.Entries() {
		entries[i] = &TxEntry{
			Key:    e.Key(),
			HValue: e.HValue[:],
			VOff:   e.VOff,
		}
	}

	return &Tx{
		Metadata: TxMetatadaTo(tx.Metadata()),
		Entries:  entries,
	}
}

func InclusionProofTo(iproof *htree.InclusionProof) *InclusionProof {
	return &InclusionProof{
		Leaf:  int32(iproof.Leaf),
		Width: int32(iproof.Width),
		Terms: digestsTo(iproof.Terms),
	}
}

func InclusionProofFrom(iproof *InclusionProof) *htree.InclusionProof {
	return &htree.InclusionProof{
		Leaf:  int(iproof.Leaf),
		Width: int(iproof.Width),
		Terms: digestsFrom(iproof.Terms),
	}
}

func DualProofTo(dualProof *store.DualProof) *DualProof {
	return &DualProof{
		SourceTxMetadata:   TxMetatadaTo(dualProof.SourceTxMetadata),
		TargetTxMetadata:   TxMetatadaTo(dualProof.TargetTxMetadata),
		InclusionProof:     digestsTo(dualProof.InclusionProof),
		ConsistencyProof:   digestsTo(dualProof.ConsistencyProof),
		TargetBlTxAlh:      dualProof.TargetBlTxAlh[:],
		LastInclusionProof: digestsTo(dualProof.LastInclusionProof),
		LinearProof:        LinearProofTo(dualProof.LinearProof),
	}
}

func TxMetatadaTo(txMetadata *store.TxMetadata) *TxMetadata {
	return &TxMetadata{
		Id:       txMetadata.ID,
		PrevAlh:  txMetadata.PrevAlh[:],
		Ts:       txMetadata.Ts,
		Nentries: int32(txMetadata.NEntries),
		EH:       txMetadata.Eh[:],
		BlTxId:   txMetadata.BlTxID,
		BlRoot:   txMetadata.BlRoot[:],
	}
}

func LinearProofTo(linearProof *store.LinearProof) *LinearProof {
	return &LinearProof{
		SourceTxId: linearProof.SourceTxID,
		TargetTxId: linearProof.TargetTxID,
		Terms:      digestsTo(linearProof.Terms),
	}
}

func DualProofFrom(dproof *DualProof) *store.DualProof {
	return &store.DualProof{
		SourceTxMetadata:   TxMetadataFrom(dproof.SourceTxMetadata),
		TargetTxMetadata:   TxMetadataFrom(dproof.TargetTxMetadata),
		InclusionProof:     digestsFrom(dproof.InclusionProof),
		ConsistencyProof:   digestsFrom(dproof.ConsistencyProof),
		TargetBlTxAlh:      digestFrom(dproof.TargetBlTxAlh),
		LastInclusionProof: digestsFrom(dproof.LastInclusionProof),
		LinearProof:        LinearProofFrom(dproof.LinearProof),
	}
}

func TxMetadataFrom(txMetadata *TxMetadata) *store.TxMetadata {
	return &store.TxMetadata{
		ID:       txMetadata.Id,
		PrevAlh:  digestFrom(txMetadata.PrevAlh),
		Ts:       txMetadata.Ts,
		NEntries: int(txMetadata.Nentries),
		Eh:       digestFrom(txMetadata.EH),
		BlTxID:   txMetadata.BlTxId,
		BlRoot:   digestFrom(txMetadata.BlRoot),
	}
}

func LinearProofFrom(lproof *LinearProof) *store.LinearProof {
	return &store.LinearProof{
		SourceTxID: lproof.SourceTxId,
		TargetTxID: lproof.TargetTxId,
		Terms:      digestsFrom(lproof.Terms),
	}
}

func digestsTo(terms [][sha256.Size]byte) [][]byte {
	slicedTerms := make([][]byte, len(terms))

	for i, t := range terms {
		slicedTerms[i] = make([]byte, sha256.Size)
		copy(slicedTerms[i], t[:])
	}

	return slicedTerms
}

func digestFrom(slicedDigest []byte) [sha256.Size]byte {
	var d [sha256.Size]byte
	copy(d[:], slicedDigest)
	return d
}

func digestsFrom(slicedTerms [][]byte) [][sha256.Size]byte {
	terms := make([][sha256.Size]byte, len(slicedTerms))

	for i, t := range slicedTerms {
		copy(terms[i][:], t)
	}

	return terms
}
