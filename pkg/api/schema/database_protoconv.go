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

package schema

import (
	"crypto/sha256"
	"time"

	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/store"
)

func TxToProto(tx *store.Tx) *Tx {
	entries := make([]*TxEntry, len(tx.Entries()))

	for i, e := range tx.Entries() {
		entries[i] = TxEntryToProto(e)
	}

	return &Tx{
		Header:  TxHeaderToProto(tx.Header()),
		Entries: entries,
	}
}

func TxEntryToProto(e *store.TxEntry) *TxEntry {
	hValue := e.HVal()

	return &TxEntry{
		Key:      e.Key(),
		Metadata: KVMetadataToProto(e.Metadata()),
		HValue:   hValue[:],
		VLen:     int32(e.VLen()),
	}
}

func KVMetadataToProto(md *store.KVMetadata) *KVMetadata {
	if md == nil {
		return nil
	}

	kvmd := &KVMetadata{
		Deleted:      md.Deleted(),
		NonIndexable: md.NonIndexable(),
	}

	if md.IsExpirable() {
		expTime, _ := md.ExpirationTime()
		kvmd.Expiration = &Expiration{ExpiresAt: expTime.Unix()}
	}

	return kvmd
}

func TxFromProto(stx *Tx) *store.Tx {
	header := &store.TxHeader{}
	header.ID = stx.Header.Id
	header.Ts = stx.Header.Ts
	header.BlTxID = stx.Header.BlTxId
	header.BlRoot = DigestFromProto(stx.Header.BlRoot)
	header.PrevAlh = DigestFromProto(stx.Header.PrevAlh)

	header.Version = int(stx.Header.Version)

	header.Metadata = TxMetadataFromProto(stx.Header.Metadata)

	entries := make([]*store.TxEntry, len(stx.Entries))

	header.NEntries = int(stx.Header.Nentries)
	header.Eh = DigestFromProto(stx.Header.EH)

	for i, e := range stx.Entries {
		entries[i] = store.NewTxEntry(e.Key, KVMetadataFromProto(e.Metadata), int(e.VLen), DigestFromProto(e.HValue), 0)
	}

	tx := store.NewTxWithEntries(header, entries)

	tx.BuildHashTree()

	return tx
}

func KVMetadataFromProto(md *KVMetadata) *store.KVMetadata {
	if md == nil {
		return nil
	}

	kvmd := store.NewKVMetadata()

	kvmd.AsDeleted(md.Deleted)

	if md.Expiration != nil {
		kvmd.ExpiresAt(time.Unix(md.Expiration.ExpiresAt, 0))
	}

	kvmd.AsNonIndexable(md.NonIndexable)

	return kvmd
}

func InclusionProofToProto(iproof *htree.InclusionProof) *InclusionProof {
	return &InclusionProof{
		Leaf:  int32(iproof.Leaf),
		Width: int32(iproof.Width),
		Terms: DigestsToProto(iproof.Terms),
	}
}

func InclusionProofFromProto(iproof *InclusionProof) *htree.InclusionProof {
	return &htree.InclusionProof{
		Leaf:  int(iproof.Leaf),
		Width: int(iproof.Width),
		Terms: DigestsFromProto(iproof.Terms),
	}
}

func DualProofToProto(dualProof *store.DualProof) *DualProof {
	return &DualProof{
		SourceTxHeader:     TxHeaderToProto(dualProof.SourceTxHeader),
		TargetTxHeader:     TxHeaderToProto(dualProof.TargetTxHeader),
		InclusionProof:     DigestsToProto(dualProof.InclusionProof),
		ConsistencyProof:   DigestsToProto(dualProof.ConsistencyProof),
		TargetBlTxAlh:      dualProof.TargetBlTxAlh[:],
		LastInclusionProof: DigestsToProto(dualProof.LastInclusionProof),
		LinearProof:        LinearProofToProto(dualProof.LinearProof),
		LinearAdvanceProof: LinearAdvanceProofToProto(dualProof.LinearAdvanceProof),
	}
}

func DualProofV2ToProto(dualProof *store.DualProofV2) *DualProofV2 {
	return &DualProofV2{
		SourceTxHeader:   TxHeaderToProto(dualProof.SourceTxHeader),
		TargetTxHeader:   TxHeaderToProto(dualProof.TargetTxHeader),
		InclusionProof:   DigestsToProto(dualProof.InclusionProof),
		ConsistencyProof: DigestsToProto(dualProof.ConsistencyProof),
	}
}

func TxHeaderToProto(hdr *store.TxHeader) *TxHeader {
	if hdr == nil {
		return nil
	}

	return &TxHeader{
		Id:       hdr.ID,
		PrevAlh:  hdr.PrevAlh[:],
		Ts:       hdr.Ts,
		Version:  int32(hdr.Version),
		Metadata: TxMetadataToProto(hdr.Metadata),
		Nentries: int32(hdr.NEntries),
		EH:       hdr.Eh[:],
		BlTxId:   hdr.BlTxID,
		BlRoot:   hdr.BlRoot[:],
	}
}

func TxMetadataToProto(md *store.TxMetadata) *TxMetadata {
	if md == nil {
		return nil
	}

	txmd := &TxMetadata{}
	if md.HasTruncatedTxID() {
		txID, _ := md.GetTruncatedTxID()
		txmd.TruncatedTxID = txID
	}

	txmd.Extra = md.Extra()

	return txmd
}

func LinearProofToProto(linearProof *store.LinearProof) *LinearProof {
	return &LinearProof{
		SourceTxId: linearProof.SourceTxID,
		TargetTxId: linearProof.TargetTxID,
		Terms:      DigestsToProto(linearProof.Terms),
	}
}

func LinearAdvanceProofToProto(proof *store.LinearAdvanceProof) *LinearAdvanceProof {
	if proof == nil {
		return nil
	}

	inclusionProofs := make([]*InclusionProof, len(proof.InclusionProofs))
	for i, p := range proof.InclusionProofs {
		inclusionProofs[i] = &InclusionProof{
			Terms: DigestsToProto(p),
		}
	}

	return &LinearAdvanceProof{
		LinearProofTerms: DigestsToProto(proof.LinearProofTerms),
		InclusionProofs:  inclusionProofs,
	}
}

func DualProofFromProto(dproof *DualProof) *store.DualProof {
	return &store.DualProof{
		SourceTxHeader:     TxHeaderFromProto(dproof.SourceTxHeader),
		TargetTxHeader:     TxHeaderFromProto(dproof.TargetTxHeader),
		InclusionProof:     DigestsFromProto(dproof.InclusionProof),
		ConsistencyProof:   DigestsFromProto(dproof.ConsistencyProof),
		TargetBlTxAlh:      DigestFromProto(dproof.TargetBlTxAlh),
		LastInclusionProof: DigestsFromProto(dproof.LastInclusionProof),
		LinearProof:        LinearProofFromProto(dproof.LinearProof),
		LinearAdvanceProof: LinearAdvanceProofFromProto(dproof.LinearAdvanceProof),
	}
}

func DualProofV2FromProto(dproof *DualProofV2) *store.DualProofV2 {
	return &store.DualProofV2{
		SourceTxHeader:   TxHeaderFromProto(dproof.SourceTxHeader),
		TargetTxHeader:   TxHeaderFromProto(dproof.TargetTxHeader),
		InclusionProof:   DigestsFromProto(dproof.InclusionProof),
		ConsistencyProof: DigestsFromProto(dproof.ConsistencyProof),
	}
}

func TxHeaderFromProto(hdr *TxHeader) *store.TxHeader {
	return &store.TxHeader{
		ID:       hdr.Id,
		PrevAlh:  DigestFromProto(hdr.PrevAlh),
		Ts:       hdr.Ts,
		Version:  int(hdr.Version),
		Metadata: TxMetadataFromProto(hdr.Metadata),
		NEntries: int(hdr.Nentries),
		Eh:       DigestFromProto(hdr.EH),
		BlTxID:   hdr.BlTxId,
		BlRoot:   DigestFromProto(hdr.BlRoot),
	}
}

func TxMetadataFromProto(md *TxMetadata) *store.TxMetadata {
	if md == nil {
		return nil
	}

	txmd := store.NewTxMetadata()
	if md.TruncatedTxID > 0 {
		txmd.WithTruncatedTxID(md.TruncatedTxID)
	}

	txmd.WithExtra(md.Extra)

	return txmd
}

func LinearProofFromProto(lproof *LinearProof) *store.LinearProof {
	return &store.LinearProof{
		SourceTxID: lproof.SourceTxId,
		TargetTxID: lproof.TargetTxId,
		Terms:      DigestsFromProto(lproof.Terms),
	}
}

func LinearAdvanceProofFromProto(laproof *LinearAdvanceProof) *store.LinearAdvanceProof {
	if laproof == nil {
		return nil
	}

	inclusionProofs := make([][][sha256.Size]byte, len(laproof.InclusionProofs))
	for i, proof := range laproof.InclusionProofs {
		inclusionProofs[i] = DigestsFromProto(proof.Terms)
	}

	return &store.LinearAdvanceProof{
		LinearProofTerms: DigestsFromProto(laproof.LinearProofTerms),
		InclusionProofs:  inclusionProofs,
	}
}

func DigestsToProto(terms [][sha256.Size]byte) [][]byte {
	slicedTerms := make([][]byte, len(terms))

	for i, t := range terms {
		slicedTerms[i] = make([]byte, sha256.Size)
		copy(slicedTerms[i], t[:])
	}

	return slicedTerms
}

func DigestFromProto(slicedDigest []byte) [sha256.Size]byte {
	var d [sha256.Size]byte
	copy(d[:], slicedDigest)
	return d
}

func DigestsFromProto(slicedTerms [][]byte) [][sha256.Size]byte {
	terms := make([][sha256.Size]byte, len(slicedTerms))

	for i, t := range slicedTerms {
		copy(terms[i][:], t)
	}

	return terms
}
