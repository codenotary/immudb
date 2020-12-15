/*
Copyright 2019-2020 vChain, Inc.

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
	"crypto/sha256"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ExecAllOps like SetBatch it permits many insertions at once.
// The difference is that is possible to to specify a list of a mix of key value set and zAdd insertions.
// If zAdd reference is not yet present on disk it's possible to add it as a regular key value and the reference is done onFly
func (d *db) ExecAllOps(ops *schema.Ops) (*schema.TxMetadata, error) {
	if err := ops.Validate(); err != nil {
		return nil, err
	}

	//var kvList schema.KVList
	tsEntriesKv := make([]*schema.KeyValue, 0)

	// In order to:
	// * make a memory efficient check system for keys that need to be referenced
	// * store the index of the future persisted zAdd referenced entries
	// we build a map in which we store sha256 sum as key and the index as value
	kmap := make(map[[32]byte]bool)

	for _, op := range ops.Operations {

		switch x := op.Operation.(type) {
		case *schema.Op_Kv:
			kmap[sha256.Sum256(x.Kv.Key)] = true
			tsEntriesKv = append(tsEntriesKv, x.Kv)
		case *schema.Op_ZAdd:
			// zAdd arguments are converted in regular key value items and then atomically inserted
			skipPersistenceCheck := false
			if _, exists := kmap[sha256.Sum256(x.ZAdd.Key)]; exists {
				skipPersistenceCheck = true
				x.ZAdd.AtTx = int64(d.tx1.ID)
			} else if x.ZAdd.AtTx == 0 {
				return nil, ErrZAddIndexMissing
			}
			// if skipPersistenceCheck is true it means that the reference will be done with a key value that is not yet
			// persisted in the store, but it's present in the previous key value list.
			// if skipPersistenceCheck is false it means that the reference is already persisted on disk.
			k, v, err := d.getSortedSetKeyVal(x.ZAdd, skipPersistenceCheck)
			if err != nil {
				return nil, err
			}
			kv := &schema.KeyValue{
				Key:   k,
				Value: v,
			}
			tsEntriesKv = append(tsEntriesKv, kv)
		case *schema.Op_Ref:
			// reference arguments are converted in regular key value items and then atomically inserted
			skipPersistenceCheck := false
			if _, exists := kmap[sha256.Sum256(x.Ref.Key)]; exists {
				skipPersistenceCheck = true
				x.Ref.AtTx = int64(d.tx1.ID)
			} else if x.Ref.AtTx == 0 {
				return nil, ErrReferenceIndexMissing
			}
			// if skipPersistenceCheck is true it means that the reference will be done with a key value that is not yet
			// persisted in the store, but it's present in the previous key value list.
			// if skipPersistenceCheck is false it means that the reference is already persisted on disk.
			v, err := d.getReferenceVal(x.Ref, skipPersistenceCheck)
			if err != nil {
				return nil, err
			}
			kv := &schema.KeyValue{
				Key:   x.Ref.Reference,
				Value: v,
			}
			tsEntriesKv = append(tsEntriesKv, kv)
		case nil:
			return nil, status.New(codes.InvalidArgument, "batch operation is not set").Err()
		default:
			return nil, status.Newf(codes.InvalidArgument, "batch operation has unexpected type %T", x).Err()
		}
	}

	return d.Set(&schema.SetRequest{KVs: tsEntriesKv})
}
