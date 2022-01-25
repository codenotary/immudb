package proof

import (
	"crypto/sha256"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

type Inclusion struct {
	Key, Val []byte
}

func Verify(verifiableTx *schema.VerifiableTx, state *schema.ImmutableState, i *Inclusion) error {
	if verifiableTx.Tx.Header.Nentries != 1 || len(verifiableTx.Tx.Entries) != 1 {
		return store.ErrCorruptedData
	}

	tx := schema.TxFromProto(verifiableTx.Tx)

	entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
	if err != nil {
		return err
	}

	if i != nil {
		inclusionProof, err := tx.Proof(database.EncodeKey(i.Key))
		if err != nil {
			return err
		}

		md := tx.Entries()[0].Metadata()

		if md != nil && md.Deleted() {
			return store.ErrCorruptedData
		}

		e := database.EncodeEntrySpec(i.Key, md, i.Val)

		if !store.VerifyInclusion(inclusionProof, entrySpecDigest(e), tx.Header().Eh) {
			return store.ErrCorruptedData
		}
	}

	if tx.Header().Eh != schema.DigestFromProto(verifiableTx.DualProof.TargetTxHeader.EH) {
		return store.ErrCorruptedData
	}

	var sourceID, targetID uint64
	var sourceAlh, targetAlh [sha256.Size]byte

	sourceID = state.TxId
	sourceAlh = schema.DigestFromProto(state.TxHash)
	targetID = tx.Header().ID
	targetAlh = tx.Header().Alh()

	if state.TxId > 0 {
		if !store.VerifyDualProof(
			schema.DualProofFromProto(verifiableTx.DualProof),
			sourceID,
			targetID,
			sourceAlh,
			targetAlh,
		) {
			return store.ErrCorruptedData
		}
	}
	return nil
}
