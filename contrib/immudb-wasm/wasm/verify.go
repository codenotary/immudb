//go:build wasip1

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

package main

import (
	"crypto/sha256"
	"errors"

	"github.com/codenotary/immudb/embedded/store"
)

type verifyResult struct {
	Value    []byte `json:"value"`
	Found    bool   `json:"found"`
	Verified bool   `json:"verified"`
	TxID     uint64 `json:"tx_id"`
	RootTxID uint64 `json:"root_tx_id"`
	RootHash string `json:"root_hash"`
}

// verifiedGet performs an in-process client-side verified read: it proves the
// stored value is included in its committing transaction (inclusion proof), then
// that this transaction is consistent with the store's current committed root
// (dual proof). Uses store-native proof primitives only — no server, no gRPC,
// no protobuf.
func (e *engine) verifiedGet(key []byte) (verifyResult, error) {
	var res verifyResult

	lastTxID, lastAlh := e.st.CommittedAlh()
	// The current committed root is a property of the store, not of the key, so
	// report it even when the key is absent (callers use it as a status anchor).
	res.RootTxID = lastTxID
	res.RootHash = hexString(lastAlh[:])

	ikey := kvKey(key)
	valRef, err := e.st.Get(ctx, ikey)
	if errors.Is(err, store.ErrKeyNotFound) {
		return res, nil // Found stays false
	}
	if err != nil {
		return res, err
	}
	value, err := valRef.Resolve()
	if err != nil {
		return res, err
	}
	res.Found = true
	res.Value = value
	res.TxID = valRef.Tx()

	// Read the committing transaction and build the entry inclusion proof.
	tx, err := e.txPool.Alloc()
	if err != nil {
		return res, err
	}
	defer e.txPool.Release(tx)

	if err := e.st.ReadTx(res.TxID, false, tx); err != nil {
		return res, err
	}
	vHdr := tx.Header()

	inclusionProof, err := tx.Proof(ikey)
	if err != nil {
		return res, err
	}
	digestFor, err := store.EntrySpecDigestFor(vHdr.Version)
	if err != nil {
		return res, err
	}
	entryDigest := digestFor(&store.EntrySpec{Key: ikey, Value: value})

	if !store.VerifyInclusion(inclusionProof, entryDigest, vHdr.Eh) {
		res.Verified = false
		return res, nil
	}

	// Link the committing transaction to the current committed root.
	if res.TxID < lastTxID {
		targetHdr, err := e.st.ReadTxHeader(lastTxID, false, false)
		if err != nil {
			return res, err
		}
		dualProof, err := e.st.DualProof(vHdr, targetHdr)
		if err != nil {
			return res, err
		}
		var srcAlh, tgtAlh [sha256.Size]byte
		srcAlh = vHdr.Alh()
		tgtAlh = lastAlh
		if !store.VerifyDualProof(dualProof, res.TxID, lastTxID, srcAlh, tgtAlh) {
			res.Verified = false
			return res, nil
		}
	}

	res.Verified = true
	return res, nil
}
