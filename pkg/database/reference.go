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

package database

import (
	"errors"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrReferencedKeyCannotBeAReference = errors.New("referenced key cannot be a reference")
var ErrFinalKeyCannotBeConvertedIntoReference = errors.New("final key cannot be converted into a reference")

//Reference ...
func (d *db) SetReference(req *schema.ReferenceRequest) (*schema.TxMetadata, error) {
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

	// check key does not exists or it's already a reference
	entry, err := d.getAt(EncodeKey(req.Key), req.AtTx, 0, d.st, d.tx1)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}
	if entry != nil && entry.ReferencedBy == nil {
		return nil, ErrFinalKeyCannotBeConvertedIntoReference
	}

	// check referenced key exists and it's not a reference
	refEntry, err := d.getAt(EncodeKey(req.ReferencedKey), req.AtTx, 0, d.st, d.tx1)
	if err != nil {
		return nil, err
	}
	if refEntry.ReferencedBy != nil {
		return nil, ErrReferencedKeyCannotBeAReference
	}

	meta, err := d.st.Commit([]*store.KV{EncodeReference(req.Key, req.ReferencedKey, req.AtTx)}, !req.NoWait)
	if err != nil {
		return nil, err
	}

	return schema.TxMetatadaTo(meta), err
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

	d.mutex.Lock()
	defer d.mutex.Unlock()

	lastTx := d.tx1

	err = d.st.ReadTx(uint64(txMetatadata.Id), lastTx)
	if err != nil {
		return nil, err
	}

	var prevTx *store.Tx

	if req.ProveSinceTx == 0 {
		prevTx = lastTx
	} else {
		prevTx = d.tx2

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
		Tx:        schema.TxTo(lastTx),
		DualProof: schema.DualProofTo(dualProof),
	}, nil
}
