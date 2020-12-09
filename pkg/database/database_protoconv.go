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

package database

import (
	"crypto/sha256"

	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

func txTo(tx *store.Tx) *schema.Tx {
	eh := tx.Eh()

	entries := make([]*schema.TxEntry, len(tx.Entries()))

	for i, e := range tx.Entries() {
		entries[i] = &schema.TxEntry{
			Key:    e.Key(),
			HValue: e.HValue[:],
			VOff:   e.VOff,
		}
	}

	return &schema.Tx{
		Metadata: &schema.TxMetadata{
			Id:       tx.ID,
			PrevAlh:  tx.PrevAlh[:],
			Ts:       tx.Ts,
			Nentries: int32(len(entries)),
			EH:       eh[:],
			BlTxId:   tx.BlTxID,
			BlRoot:   tx.BlRoot[:],
		},
		Entries: entries,
	}
}

func inclusionProofTo(iproof *htree.InclusionProof) *schema.InclusionProof {
	return &schema.InclusionProof{
		Index: int32(iproof.Index),
		Width: int32(iproof.Width),
		Terms: digestsTo(iproof.Terms),
	}
}

func inclusionProofFrom(iproof *schema.InclusionProof) *htree.InclusionProof {
	return &htree.InclusionProof{
		Index: int(iproof.Index),
		Width: int(iproof.Width),
		Terms: digestsFrom(iproof.Terms),
	}
}

func dualProofTo(dualProof *store.DualProof) *schema.DualProof {
	return &schema.DualProof{
		SourceTxMetadata:   txMetatadaTo(dualProof.SourceTxMetadata),
		TargetTxMetadata:   txMetatadaTo(dualProof.TargetTxMetadata),
		InclusionProof:     digestsTo(dualProof.InclusionProof),
		ConsistencyProof:   digestsTo(dualProof.ConsistencyProof),
		TargetBlTxAlh:      dualProof.TargetBlTxAlh[:],
		LastInclusionProof: digestsTo(dualProof.LastInclusionProof),
		LinearProof:        linearProofTo(dualProof.LinearProof),
	}
}

func txMetatadaTo(txMetadata *store.TxMetadata) *schema.TxMetadata {
	return &schema.TxMetadata{
		Id:       txMetadata.ID,
		PrevAlh:  txMetadata.PrevAlh[:],
		Ts:       txMetadata.Ts,
		Nentries: int32(txMetadata.NEntries),
		EH:       txMetadata.Eh[:],
		BlTxId:   txMetadata.BlTxID,
		BlRoot:   txMetadata.BlRoot[:],
	}
}

func linearProofTo(linearProof *store.LinearProof) *schema.LinearProof {
	return &schema.LinearProof{
		SourceTxId: linearProof.SourceTxID,
		TargetTxId: linearProof.TargetTxID,
		Terms:      digestsTo(linearProof.Terms),
	}
}

func dualProofFrom(dproof *schema.DualProof) *store.DualProof {
	return &store.DualProof{
		SourceTxMetadata:   txMetatadaFrom(dproof.SourceTxMetadata),
		TargetTxMetadata:   txMetatadaFrom(dproof.TargetTxMetadata),
		InclusionProof:     digestsFrom(dproof.InclusionProof),
		ConsistencyProof:   digestsFrom(dproof.ConsistencyProof),
		TargetBlTxAlh:      digestFrom(dproof.TargetBlTxAlh),
		LastInclusionProof: digestsFrom(dproof.LastInclusionProof),
		LinearProof:        linearProofFrom(dproof.LinearProof),
	}
}

func txMetatadaFrom(txMetadata *schema.TxMetadata) *store.TxMetadata {
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

func linearProofFrom(lproof *schema.LinearProof) *store.LinearProof {
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
