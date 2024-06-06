/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var ErrReferencedKeyCannotBeAReference = errors.New("referenced key cannot be a reference")
var ErrFinalKeyCannotBeConvertedIntoReference = errors.New("final key cannot be converted into a reference")
var ErrNoWaitOperationMustBeSelfContained = fmt.Errorf("no wait operation must be self-contained: %w", store.ErrIllegalArguments)

// Reference ...
func (d *db) SetReference(ctx context.Context, req *schema.ReferenceRequest) (*schema.TxHeader, error) {
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

	lastTxID, _ := d.st.CommittedAlh()
	err := d.st.WaitForIndexingUpto(ctx, lastTxID)
	if err != nil {
		return nil, err
	}

	// check key does not exists or it's already a reference
	entry, err := d.getAtTx(ctx, EncodeKey(req.Key), req.AtTx, 0, d.st, 0, true)
	if err != nil && err != store.ErrKeyNotFound {
		return nil, err
	}
	if entry != nil && entry.ReferencedBy == nil {
		return nil, ErrFinalKeyCannotBeConvertedIntoReference
	}

	// check referenced key exists and it's not a reference
	refEntry, err := d.getAtTx(ctx, EncodeKey(req.ReferencedKey), req.AtTx, 0, d.st, 0, true)
	if err != nil {
		return nil, err
	}
	if refEntry.ReferencedBy != nil {
		return nil, ErrReferencedKeyCannotBeAReference
	}

	tx, err := d.st.NewWriteOnlyTx(ctx)
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
		hdr, err = tx.AsyncCommit(ctx)
	} else {
		hdr, err = tx.Commit(ctx)
	}
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), err
}

// SafeReference ...
func (d *db) VerifiableSetReference(ctx context.Context, req *schema.VerifiableReferenceRequest) (*schema.VerifiableTx, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	lastTxID, _ := d.st.CommittedAlh()
	if lastTxID < req.ProveSinceTx {
		return nil, store.ErrIllegalArguments
	}

	// Preallocate tx buffers
	lastTx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(lastTx)

	txMetatadata, err := d.SetReference(ctx, req.ReferenceRequest)
	if err != nil {
		return nil, err
	}

	err = d.st.ReadTx(uint64(txMetatadata.Id), false, lastTx)
	if err != nil {
		return nil, err
	}

	var prevTxHdr *store.TxHeader

	if req.ProveSinceTx == 0 {
		prevTxHdr = lastTx.Header()
	} else {
		prevTxHdr, err = d.st.ReadTxHeader(req.ProveSinceTx, false, false)
		if err != nil {
			return nil, err
		}
	}

	dualProof, err := d.st.DualProof(prevTxHdr, lastTx.Header())
	if err != nil {
		return nil, err
	}

	return &schema.VerifiableTx{
		Tx:        schema.TxToProto(lastTx),
		DualProof: schema.DualProofToProto(dualProof),
	}, nil
}
