/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
package verification

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	structpb "github.com/golang/protobuf/ptypes/struct"
)

func VerifyDocument(ctx context.Context,
	proof *protomodel.ProofDocumentResponse,
	doc *structpb.Struct,
	knownState *schema.ImmutableState,
	serverSigningPubKey *ecdsa.PublicKey,
) (*schema.ImmutableState, error) {

	if proof == nil || doc == nil {
		return nil, store.ErrIllegalArguments
	}

	docID, ok := doc.Fields[proof.IdFieldName]
	if !ok {
		return nil, fmt.Errorf("%w: missing field '%s'", store.ErrIllegalArguments, proof.IdFieldName)
	}

	encDocKey, err := encodedKeyForDocument(proof.CollectionId, docID.GetStringValue())
	if err != nil {
		return nil, err
	}

	var keyFound int

	for _, txEntry := range proof.VerifiableTx.Tx.Entries {
		if bytes.Equal(txEntry.Key, encDocKey) {
			hVal := sha256.Sum256(proof.EncodedDocument)

			if !bytes.Equal(hVal[:], txEntry.HValue) {
				return nil, store.ErrInvalidProof
			}

			keyFound++
		}
	}

	if keyFound != 1 {
		return nil, fmt.Errorf("%w: document entry was not found or it was found multiple times", store.ErrInvalidProof)
	}

	// check encoded value is consistent with raw document
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}

	voff := sql.EncLenLen + sql.EncIDLen

	// DocumentIDField
	_, n, err := sql.DecodeValue(proof.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, err
	}

	if n > document.MaxDocumentIDLength {
		return nil, fmt.Errorf("%w: the proof contains invalid document data", store.ErrInvalidProof)
	}

	voff += n + sql.EncIDLen

	// DocumentBLOBField
	encodedDoc, _, err := sql.DecodeValue(proof.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(docBytes, encodedDoc.RawValue().([]byte)) {
		return nil, fmt.Errorf("%w: the document does not match the proof provided", store.ErrInvalidProof)
	}

	entries := proof.VerifiableTx.Tx.Entries

	htree, err := htree.New(len(entries))
	if err != nil {
		return nil, err
	}

	entrySpecDigest, err := store.EntrySpecDigestFor(int(proof.VerifiableTx.Tx.Header.Version))
	if err != nil {
		return nil, err
	}

	digests := make([][sha256.Size]byte, len(entries))

	for i, e := range entries {
		eSpec := &store.EntrySpec{
			Key:              e.Key,
			Metadata:         schema.KVMetadataFromProto(e.Metadata),
			HashValue:        schema.DigestFromProto(e.HValue),
			IsValueTruncated: true,
		}
		digests[i] = entrySpecDigest(eSpec)
	}

	err = htree.BuildWith(digests)
	if err != nil {
		return nil, err
	}

	txHdr := schema.TxHeaderFromProto(proof.VerifiableTx.Tx.Header)

	if htree.Root() != txHdr.Eh {
		return nil, store.ErrInvalidProof
	}

	dualProof := schema.DualProofV2FromProto(proof.VerifiableTx.DualProof)

	sourceID := proof.VerifiableTx.DualProof.SourceTxHeader.Id
	targetID := proof.VerifiableTx.DualProof.TargetTxHeader.Id

	if targetID < sourceID {
		return nil, fmt.Errorf("%w: source tx is newer than target tx", store.ErrInvalidProof)
	}

	sourceAlh := schema.TxHeaderFromProto(proof.VerifiableTx.DualProof.SourceTxHeader).Alh()
	targetAlh := schema.TxHeaderFromProto(proof.VerifiableTx.DualProof.TargetTxHeader).Alh()

	if txHdr.ID == sourceID {
		if txHdr.Alh() != sourceAlh {
			return nil, fmt.Errorf("%w: tx must match source or target tx headers", store.ErrInvalidProof)
		}
	} else if txHdr.ID == targetID {
		if txHdr.Alh() != targetAlh {
			return nil, fmt.Errorf("%w: tx must match source or target tx headers", store.ErrInvalidProof)
		}
	} else {
		return nil, fmt.Errorf("%w: tx must match source or target tx headers", store.ErrInvalidProof)
	}

	if knownState == nil || knownState.TxId == 0 {
		if sourceID != 1 {
			return nil, fmt.Errorf("%w: proof should start from the first transaction when no previous state was specified", store.ErrInvalidProof)
		}
	} else {
		if knownState.TxId == sourceID {
			if !bytes.Equal(knownState.TxHash, sourceAlh[:]) {
				return nil, fmt.Errorf("%w: knownState alh must match source or target tx alh", store.ErrInvalidProof)
			}
		} else if knownState.TxId == targetID {
			if !bytes.Equal(knownState.TxHash, targetAlh[:]) {
				return nil, fmt.Errorf("%w: knownState alh must match source or target tx alh", store.ErrInvalidProof)
			}
		} else {
			return nil, fmt.Errorf("%w: knownState alh must match source or target tx alh", store.ErrInvalidProof)
		}
	}

	err = store.VerifyDualProofV2(
		dualProof,
		sourceID,
		targetID,
		sourceAlh,
		targetAlh,
	)
	if err != nil {
		return nil, err
	}

	state := &schema.ImmutableState{
		Db:        proof.Database,
		TxId:      targetID,
		TxHash:    targetAlh[:],
		Signature: proof.VerifiableTx.Signature,
	}

	if serverSigningPubKey != nil {
		ok, err := state.CheckSignature(serverSigningPubKey)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, store.ErrInvalidProof
		}
	}

	return state, nil
}

func encodedKeyForDocument(collectionID uint32, documentID string) ([]byte, error) {
	docID, err := document.NewDocumentIDFromHexEncodedString(documentID)
	if err != nil {
		return nil, err
	}

	valbuf := bytes.Buffer{}

	rval := sql.NewBlob(docID[:])
	encVal, err := sql.EncodeRawValueAsKey(rval.RawValue(), sql.BLOBType, document.MaxDocumentIDLength)
	if err != nil {
		return nil, err
	}
	_, err = valbuf.Write(encVal)
	if err != nil {
		return nil, err
	}

	pkEncVals := valbuf.Bytes()

	return sql.MapKey(
		[]byte{3}, // database.DocumentPrefix
		sql.PIndexPrefix,
		sql.EncodeID(1), // fixed database identifier
		sql.EncodeID(collectionID),
		sql.EncodeID(0), // pk index id
		pkEncVals,
	), nil
}
