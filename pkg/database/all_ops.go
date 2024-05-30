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
	"crypto/sha256"
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// ExecAll like Set it permits many insertions at once.
// The difference is that is possible to to specify a list of a mix of key value set and zAdd insertions.
// If zAdd reference is not yet present on disk it's possible to add it as a regular key value and the reference is done onFly
func (d *db) ExecAll(ctx context.Context, req *schema.ExecAllRequest) (*schema.TxHeader, error) {
	if req == nil {
		return nil, store.ErrIllegalArguments
	}

	if err := req.Validate(); err != nil {
		return nil, err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if !req.NoWait {
		lastTxID, _ := d.st.CommittedAlh()
		err := d.st.WaitForIndexingUpto(ctx, lastTxID)
		if err != nil {
			return nil, err
		}
	}

	callback := func(txID uint64, index store.KeyIndex) ([]*store.EntrySpec, []store.Precondition, error) {
		entries := make([]*store.EntrySpec, len(req.Operations))

		// In order to:
		// * make a memory efficient check system for keys that need to be referenced
		// * store the index of the future persisted zAdd referenced entries
		// we build a map in which we store sha256 sum as key and the index as value
		kmap := make(map[[sha256.Size]byte]bool)

		for i, op := range req.Operations {
			e := &store.EntrySpec{}

			switch x := op.Operation.(type) {

			case *schema.Op_Kv:

				kmap[sha256.Sum256(x.Kv.Key)] = true

				if len(x.Kv.Key) == 0 {
					return nil, nil, store.ErrIllegalArguments
				}

				e = EncodeEntrySpec(
					x.Kv.Key,
					schema.KVMetadataFromProto(x.Kv.Metadata),
					x.Kv.Value,
				)

			case *schema.Op_Ref:
				if len(x.Ref.Key) == 0 || len(x.Ref.ReferencedKey) == 0 {
					return nil, nil, store.ErrIllegalArguments
				}

				if x.Ref.AtTx > 0 && !x.Ref.BoundRef {
					return nil, nil, store.ErrIllegalArguments
				}

				if req.NoWait && (x.Ref.AtTx != 0 || !x.Ref.BoundRef) {
					return nil, nil, fmt.Errorf(
						"%w: can only set references to keys added within same transaction, please use bound references with AtTx set to 0",
						ErrNoWaitOperationMustBeSelfContained)
				}

				_, exists := kmap[sha256.Sum256(x.Ref.ReferencedKey)]

				if req.NoWait && !exists {
					return nil, nil, fmt.Errorf("%w: can not create a reference to a key that was not set in the same transaction", ErrNoWaitOperationMustBeSelfContained)
				}

				if !req.NoWait {
					// check key does not exists or it's already a reference
					entry, err := d.getAtTx(ctx, EncodeKey(x.Ref.Key), 0, 0, index, 0, true)
					if err != nil && err != store.ErrKeyNotFound {
						return nil, nil, err
					}
					if entry != nil && entry.ReferencedBy == nil {
						return nil, nil, ErrFinalKeyCannotBeConvertedIntoReference
					}

					if !exists || x.Ref.AtTx > 0 {
						// check referenced key exists and it's not a reference
						refEntry, err := d.getAtTx(ctx, EncodeKey(x.Ref.ReferencedKey), x.Ref.AtTx, 0, index, 0, true)
						if err != nil {
							return nil, nil, err
						}
						if refEntry.ReferencedBy != nil {
							return nil, nil, ErrReferencedKeyCannotBeAReference
						}
					}
				}

				// reference arguments are converted in regular key value items and then atomically inserted
				if x.Ref.BoundRef && x.Ref.AtTx == 0 {
					e = EncodeReference(
						x.Ref.Key,
						nil,
						x.Ref.ReferencedKey,
						txID,
					)
				} else {
					e = EncodeReference(
						x.Ref.Key,
						nil,
						x.Ref.ReferencedKey,
						x.Ref.AtTx,
					)
				}

			case *schema.Op_ZAdd:
				if len(x.ZAdd.Set) == 0 || len(x.ZAdd.Key) == 0 {
					return nil, nil, store.ErrIllegalArguments
				}

				if x.ZAdd.AtTx > 0 && !x.ZAdd.BoundRef {
					return nil, nil, store.ErrIllegalArguments
				}

				if req.NoWait && (x.ZAdd.AtTx != 0 || !x.ZAdd.BoundRef) {
					return nil, nil, fmt.Errorf(
						"%w: can only set references to keys added within same transaction, please use bound references with AtTx set to 0",
						ErrNoWaitOperationMustBeSelfContained)
				}

				_, exists := kmap[sha256.Sum256(x.ZAdd.Key)]

				if req.NoWait && !exists {
					return nil, nil, fmt.Errorf("%w: can not create a reference into a set for a key that was not set in the same transaction", ErrNoWaitOperationMustBeSelfContained)
				}

				if !req.NoWait {
					if !exists || x.ZAdd.AtTx > 0 {
						// check referenced key exists and it's not a reference
						refEntry, err := d.getAtTx(ctx, EncodeKey(x.ZAdd.Key), x.ZAdd.AtTx, 0, index, 0, true)
						if err != nil {
							return nil, nil, err
						}
						if refEntry.ReferencedBy != nil {
							return nil, nil, ErrReferencedKeyCannotBeAReference
						}
					}
				}

				// zAdd arguments are converted in regular key value items and then atomically inserted
				key := EncodeKey(x.ZAdd.Key)

				if x.ZAdd.BoundRef && x.ZAdd.AtTx == 0 {
					e = EncodeZAdd(x.ZAdd.Set, x.ZAdd.Score, key, txID)
				} else {
					e = EncodeZAdd(x.ZAdd.Set, x.ZAdd.Score, key, x.ZAdd.AtTx)
				}
			}

			entries[i] = e
		}

		preconditions := make([]store.Precondition, len(req.Preconditions))
		for i := 0; i < len(req.Preconditions); i++ {
			c, err := PreconditionFromProto(req.Preconditions[i])
			if err != nil {
				return nil, nil, err
			}

			preconditions[i] = c
		}

		return entries, preconditions, nil
	}

	hdr, err := d.st.CommitWith(ctx, callback, !req.NoWait)
	if err != nil {
		return nil, err
	}

	return schema.TxHeaderToProto(hdr), nil
}
