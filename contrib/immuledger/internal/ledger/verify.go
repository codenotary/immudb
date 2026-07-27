/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
// consistent store — for that, pin an external anchor (expectedRoot together with
// the tx id it was taken at) via VerifyDecision.
const TrustNote = "Proof anchor is this store's current root; pass an expected root plus its tx id to detect directory replacement."

// VerifyDecision performs a genuine client-side verified read of a decision,
// fully in-process (no immudb server). It mirrors the verification pkg/client
// does for VerifiedGet: it checks the entry's inclusion proof against the
// transaction's entry-hash root, then a dual proof linking that transaction to
// the ledger's current committed state. Because the embedded store builds a
// complete DualProof (including the linear-advance proof), no gRPC round trip is
// required.
// VerifyDecision verifies decision id. If expectedRoot is non-empty it is an
// externally pinned anchor, taken at expectedRootTxID (both values are what
// CurrentRoot and the ledger_status tool return together). Verification then
// additionally proves the ledger's current root is a consistent extension of
// that anchor, which detects a replaced data directory and not just in-place
// tampering. Passing expectedRootTxID = 0 falls back to a plain equality check
// against the current root, which only holds if nothing has been appended since.
func (l *Ledger) VerifyDecision(ctx context.Context, project string, id int, expectedRoot string, expectedRootTxID uint64) (VerifyResult, error) {
	project = sanitizeProject(project)
	res := VerifyResult{DecisionID: id, Key: decisionKey(project, id)}

	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		// Trust anchor: the ledger's current committed root.
		state, err := db.CurrentState()
		if err != nil {
			return err
		}

		anchored := ""
		if expectedRoot != "" {
			detail, ok := verifyAnchor(ctx, db, state, expectedRoot, expectedRootTxID)
			if !ok {
				res.Verified = false
				res.RootTxID = state.TxId
				res.RootHash = hex.EncodeToString(state.TxHash)
				res.Detail = detail
				return nil
			}
			anchored = detail
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
		if anchored != "" {
			res.Detail += "; " + anchored
		}
		return nil
	})

	return res, err
}

// verifyAnchor checks the ledger's current state against an externally pinned
// anchor. It returns a detail string and whether the anchor holds.
//
// The anchor is deliberately NOT compared for equality with the current root:
// every append advances the root, so an anchor recorded out of band is stale the
// moment the next decision or event lands, and an equality test would report a
// fabricated directory-replacement attack on every healthy ledger. What actually
// proves the anchor is a consistency (dual) proof from the pinned transaction to
// the current one — the store can build it in-process, so no server is involved.
func verifyAnchor(
	ctx context.Context,
	db database.DB,
	state *schema.ImmutableState,
	expectedRoot string,
	anchorTxID uint64,
) (string, bool) {
	expectedRoot = strings.TrimSpace(expectedRoot)
	raw, derr := hex.DecodeString(expectedRoot)
	if derr != nil || len(raw) != sha256.Size {
		return "expected_root is not a 32-byte hex hash", false
	}
	var anchorAlh, currentAlh [sha256.Size]byte
	copy(anchorAlh[:], raw)
	copy(currentAlh[:], state.TxHash)

	matchesCurrent := strings.EqualFold(expectedRoot, hex.EncodeToString(state.TxHash))

	switch {
	case anchorTxID == 0:
		// Without the tx id the anchor was taken at, the only thing that can be
		// checked is equality with the current root.
		if matchesCurrent {
			return fmt.Sprintf("the pinned root matches the ledger's current root (tx %d)", state.TxId), true
		}
		return "the pinned root does not match the ledger's current root. Every append advances the root, " +
			"so an anchor recorded earlier will never match on a live ledger: pass expected_root_tx_id " +
			"alongside expected_root (ledger_status returns both) so consistency from that anchor can be " +
			"proven instead", false

	case anchorTxID > state.TxId:
		return fmt.Sprintf("the pinned anchor is at tx %d but the ledger only holds %d transactions: "+
			"the data directory may have been replaced or truncated", anchorTxID, state.TxId), false

	case anchorTxID == state.TxId:
		if matchesCurrent {
			return fmt.Sprintf("the pinned root matches the ledger's current root (tx %d)", state.TxId), true
		}
		return fmt.Sprintf("the ledger's tx %d does not carry the pinned root: "+
			"the data directory may have been replaced", anchorTxID), false
	}

	// anchorTxID < state.TxId — prove the current root descends from the anchor.
	vtx, err := db.VerifiableTxByID(ctx, &schema.VerifiableTxRequest{
		Tx:           state.TxId,
		ProveSinceTx: anchorTxID,
	})
	if err != nil {
		return fmt.Sprintf("could not build a consistency proof from tx %d to tx %d: %v",
			anchorTxID, state.TxId, err), false
	}

	if !store.VerifyDualProof(
		schema.DualProofFromProto(vtx.DualProof),
		anchorTxID, state.TxId,
		anchorAlh, currentAlh,
	) {
		return fmt.Sprintf("the current ledger root (tx %d) is not a consistent extension of the pinned "+
			"root at tx %d: the data directory may have been replaced", state.TxId, anchorTxID), false
	}

	return fmt.Sprintf("the current root (tx %d) is cryptographically consistent with the pinned anchor at tx %d",
		state.TxId, anchorTxID), true
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
