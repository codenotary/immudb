package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

// VerifyResult is the outcome of an in-process cryptographic verification of a
// single decision record.
type VerifyResult struct {
	Verified   bool   `json:"verified"`
	DecisionID int    `json:"decision_id"`
	Key        string `json:"key"`
	TxID       uint64 `json:"tx_id"`
	RootTxID   uint64 `json:"root_tx_id"`
	RootHash   string `json:"root_hash"`
	Detail     string `json:"detail"`
}

// TrustNote documents the scope of the in-process proof. The trust anchor is the
// local store's own current root, so verification proves internal consistency
// (a record cannot be altered in place without detection). It does NOT by itself
// detect wholesale replacement of the data directory with a different, internally
// consistent store — for that, pin an external expected root via the expectedRoot
// argument to VerifyDecision (or store one out of band and compare).
const TrustNote = "Proof anchor is this store's current root; pass an expected root to detect directory replacement."

// VerifyDecision performs a genuine client-side verified read of a decision,
// fully in-process (no immudb server). It mirrors the verification pkg/client
// does for VerifiedGet: it checks the entry's inclusion proof against the
// transaction's entry-hash root, then a dual proof linking that transaction to
// the ledger's current committed state. Because the embedded store builds a
// complete DualProof (including the linear-advance proof), no gRPC round trip is
// required.
// VerifyDecision verifies decision id. If expectedRoot is non-empty, it must
// equal the ledger's current root hash (hex); this lets a caller pin an
// externally recorded anchor so a replaced data directory is detected, not just
// in-place tampering.
func (l *Ledger) VerifyDecision(ctx context.Context, project string, id int, expectedRoot string) (VerifyResult, error) {
	project = sanitizeProject(project)
	res := VerifyResult{DecisionID: id, Key: decisionKey(project, id)}

	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		// Trust anchor: the ledger's current committed root.
		state, err := db.CurrentState()
		if err != nil {
			return err
		}

		if expectedRoot != "" && !strings.EqualFold(expectedRoot, hex.EncodeToString(state.TxHash)) {
			res.Verified = false
			res.RootTxID = state.TxId
			res.RootHash = hex.EncodeToString(state.TxHash)
			res.Detail = "current ledger root does not match the expected root: the data directory may have been replaced"
			return nil
		}

		key := []byte(decisionKey(project, id))
		vEntry, err := db.VerifiableGet(ctx, &schema.VerifiableGetRequest{
			KeyRequest:   &schema.KeyRequest{Key: key},
			ProveSinceTx: state.TxId,
		})
		if err != nil {
			return err
		}

		entrySpecDigest, err := store.EntrySpecDigestFor(int(vEntry.VerifiableTx.Tx.Header.Version))
		if err != nil {
			return err
		}

		inclusionProof := schema.InclusionProofFromProto(vEntry.InclusionProof)
		dualProof := schema.DualProofFromProto(vEntry.VerifiableTx.DualProof)

		// Direct KV entry (immuledger never stores references).
		vTx := vEntry.Entry.Tx
		e := database.EncodeEntrySpec(
			key,
			schema.KVMetadataFromProto(vEntry.Entry.Metadata),
			vEntry.Entry.Value,
		)

		var eh [sha256.Size]byte
		var sourceID, targetID uint64
		var sourceAlh, targetAlh [sha256.Size]byte

		if state.TxId <= vTx {
			eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.TargetTxHeader.EH)
			sourceID = state.TxId
			sourceAlh = schema.DigestFromProto(state.TxHash)
			targetID = vTx
			targetAlh = dualProof.TargetTxHeader.Alh()
		} else {
			eh = schema.DigestFromProto(vEntry.VerifiableTx.DualProof.SourceTxHeader.EH)
			sourceID = vTx
			sourceAlh = dualProof.SourceTxHeader.Alh()
			targetID = state.TxId
			targetAlh = schema.DigestFromProto(state.TxHash)
		}

		res.TxID = vTx
		res.RootTxID = targetID
		res.RootHash = hex.EncodeToString(targetAlh[:])

		if !store.VerifyInclusion(inclusionProof, entrySpecDigest(e), eh) {
			res.Verified = false
			res.Detail = "inclusion proof failed: the stored value does not match the committed transaction"
			return nil
		}

		if state.TxId > 0 {
			if !store.VerifyDualProof(dualProof, sourceID, targetID, sourceAlh, targetAlh) {
				res.Verified = false
				res.Detail = "dual proof failed: the transaction is not consistent with the current ledger state"
				return nil
			}
		}

		res.Verified = true
		res.Detail = fmt.Sprintf(
			"decision #%d is cryptographically included in tx %d and consistent with ledger root tx %d",
			id, vTx, targetID,
		)
		return nil
	})

	return res, err
}

// CurrentRoot returns the ledger's current committed transaction id and root
// hash (hex) — the tamper-evident anchor over all records.
func (l *Ledger) CurrentRoot(ctx context.Context) (txID uint64, rootHash string, err error) {
	err = l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		state, serr := db.CurrentState()
		if serr != nil {
			return serr
		}
		txID = state.TxId
		rootHash = hex.EncodeToString(state.TxHash)
		return nil
	})
	return txID, rootHash, err
}
