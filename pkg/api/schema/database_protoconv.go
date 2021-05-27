/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
		hValue := e.HVal()

		entries[i] = &TxEntry{
			Key:    e.Key(),
			HValue: hValue[:],
			VLen:   int32(e.VLen()),
		}
	}

	return &Tx{
		Metadata: TxMetatadaTo(tx.Metadata()),
		Entries:  entries,
	}
}

func TxFrom(stx *Tx) *store.Tx {
	entries := make([]*store.TxEntry, len(stx.Entries))

	for i, e := range stx.Entries {
		entries[i] = store.NewTxEntry(e.Key, int(e.VLen), DigestFrom(e.HValue), 0)
	}

	tx := store.NewTxWithEntries(entries)

	tx.ID = stx.Metadata.Id
	tx.PrevAlh = DigestFrom(stx.Metadata.PrevAlh)
	tx.Ts = stx.Metadata.Ts
	tx.BlTxID = stx.Metadata.BlTxId
	tx.BlRoot = DigestFrom(stx.Metadata.BlRoot)

	tx.BuildHashTree()
	tx.CalcAlh()

	return tx
}

func InclusionProofTo(iproof *htree.InclusionProof) *InclusionProof {
	return &InclusionProof{
		Leaf:  int32(iproof.Leaf),
		Width: int32(iproof.Width),
		Terms: DigestsTo(iproof.Terms),
	}
}

func InclusionProofFrom(iproof *InclusionProof) *htree.InclusionProof {
	return &htree.InclusionProof{
		Leaf:  int(iproof.Leaf),
		Width: int(iproof.Width),
		Terms: DigestsFrom(iproof.Terms),
	}
}

func DualProofTo(dualProof *store.DualProof) *DualProof {
	return &DualProof{
		SourceTxMetadata:   TxMetatadaTo(dualProof.SourceTxMetadata),
		TargetTxMetadata:   TxMetatadaTo(dualProof.TargetTxMetadata),
		InclusionProof:     DigestsTo(dualProof.InclusionProof),
		ConsistencyProof:   DigestsTo(dualProof.ConsistencyProof),
		TargetBlTxAlh:      dualProof.TargetBlTxAlh[:],
		LastInclusionProof: DigestsTo(dualProof.LastInclusionProof),
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
		Terms:      DigestsTo(linearProof.Terms),
	}
}

func DualProofFrom(dproof *DualProof) *store.DualProof {
	return &store.DualProof{
		SourceTxMetadata:   TxMetadataFrom(dproof.SourceTxMetadata),
		TargetTxMetadata:   TxMetadataFrom(dproof.TargetTxMetadata),
		InclusionProof:     DigestsFrom(dproof.InclusionProof),
		ConsistencyProof:   DigestsFrom(dproof.ConsistencyProof),
		TargetBlTxAlh:      DigestFrom(dproof.TargetBlTxAlh),
		LastInclusionProof: DigestsFrom(dproof.LastInclusionProof),
		LinearProof:        LinearProofFrom(dproof.LinearProof),
	}
}

func TxMetadataFrom(txMetadata *TxMetadata) *store.TxMetadata {
	return &store.TxMetadata{
		ID:       txMetadata.Id,
		PrevAlh:  DigestFrom(txMetadata.PrevAlh),
		Ts:       txMetadata.Ts,
		NEntries: int(txMetadata.Nentries),
		Eh:       DigestFrom(txMetadata.EH),
		BlTxID:   txMetadata.BlTxId,
		BlRoot:   DigestFrom(txMetadata.BlRoot),
	}
}

func LinearProofFrom(lproof *LinearProof) *store.LinearProof {
	return &store.LinearProof{
		SourceTxID: lproof.SourceTxId,
		TargetTxID: lproof.TargetTxId,
		Terms:      DigestsFrom(lproof.Terms),
	}
}

func DigestsTo(terms [][sha256.Size]byte) [][]byte {
	slicedTerms := make([][]byte, len(terms))

	for i, t := range terms {
		slicedTerms[i] = make([]byte, sha256.Size)
		copy(slicedTerms[i], t[:])
	}

	return slicedTerms
}

func DigestFrom(slicedDigest []byte) [sha256.Size]byte {
	var d [sha256.Size]byte
	copy(d[:], slicedDigest)
	return d
}

func DigestsFrom(slicedTerms [][]byte) [][sha256.Size]byte {
	terms := make([][sha256.Size]byte, len(slicedTerms))

	for i, t := range slicedTerms {
		copy(terms[i][:], t)
	}

	return terms
}
