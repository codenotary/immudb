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

package database

import (
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrReferencedKeyCannotBeAReference = errors.New("referenced key cannot be a reference")
var ErrFinalKeyCannotBeConvertedIntoReference = errors.New("final key cannot be converted into a reference")
var ErrNoWaitOperationMustBeSelfContained = fmt.Errorf("no wait operation must be self-contained: %w", store.ErrIllegalArguments)

//Reference ...
func (d *db) SetReference(req *schema.ReferenceRequest) (*schema.TxHeader, error) {
	if req == nil || len(req.Key) == 0 || len(req.ReferencedKey) == 0 {
		return nil, store.ErrIllegalArguments
	}

	if (req.AtTx == 0 && req.BoundRef) || (req.AtTx > 0 && !req.BoundRef) {
		return nil, store.ErrIllegalArguments
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	lastTxID, _ := d.st.Alh()
	err := d.st.WaitForIndexingUpto(lastTxID, nil)
	if err != nil {
		return nil, err
	}

	txHolder := d.st.NewTxHolder()

	// check key does not exists or it's already a reference
	entry, err := d.getAtTx(EncodeKey(req.Key), req.AtTx, 0, d.st, txHolder, 0)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}
	if entry != nil && entry.ReferencedBy == nil {
		return nil, ErrFinalKeyCannotBeConvertedIntoReference
	}

	// check referenced key exists and it's not a reference
	refEntry, err := d.getAtTx(EncodeKey(req.ReferencedKey), req.AtTx, 0, d.st, txHolder, 0)
	if err != nil {
		return nil, err
	}
	if refEntry.ReferencedBy != nil {
		return nil, ErrReferencedKeyCannotBeAReference
	}

	tx, err := d.st.NewWriteOnlyTx()
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	e := EncodeReference(
		req.Key,
		nil,
		req.ReferencedKey,
		req.AtTx,
	)

	err = tx.Set(e.Key, e.Metadata, e.Value)
	if err != nil {
		return nil, err
	}

	for i := range req.Preconditions {
		c, err := PreconditionFromProto(req.Preconditions[i])
		if err != nil {
			return nil, err
		}

		err = tx.AddPrecondition(c)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", store.ErrInvalidPrecondition, err)
		}
	}

	var hdr *store.TxHeader

	if req.NoWait {
		hdr, err = tx.AsyncCommit()
	} else {
		hdr, err = tx.Commit()
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), err
}

//SafeReference ...
func (d *db) VerifiableSetReference(req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	lastTxID, _ := d.st.Alh()
	if lastTxID < req.ProveSinceTx {
		return nil, store.ErrIllegalArguments
	}

	txMetatadata, err := d.SetReference(req.ReferenceRequest)
	if err != nil {
		return nil, err
	}

	lastTx := d.st.NewTxHolder()

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveSinceTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.st.NewTxHolder()

		err = d.st.ReadTx(req.ProveSinceTx, prevTx)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTx, lastTx)
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxToProto(lastTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}
