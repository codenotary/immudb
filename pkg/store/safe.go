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

package store

import (
	"math"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/tree"

	"github.com/dgraph-io/badger/v2"
)

func (t *Store) SafeSet(options schema.SafeSetOptions) (proof *schema.Proof, err error) {
	kv := options.Kv
	if kv.Key[0] == tsPrefix {
		err = InvalidKeyErr
		return
	}

	if options.RootIndex.Index > 0 {
		if lastIndex := t.tree.LastIndex(); lastIndex > options.RootIndex.Index {
			return nil, InvalidRootIndexErr
		}
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: kv.Value,
	}); err != nil {
		return
	}

	tsEntry := t.tree.NewEntry(kv.Key, kv.Value)
	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	err = txn.CommitAt(tsEntry.ts, nil)
	if err != nil {
		t.tree.Discard(tsEntry)
		return
	}

	t.tree.Commit(tsEntry)
	t.tree.WaitUntil(index)

	t.tree.RLock()
	defer t.tree.RUnlock()

	at := t.tree.w - 1
	root := tree.Root(t.tree)

	proof = &schema.Proof{
		Leaf:            leaf,
		Index:           index,
		Root:            root[:],
		At:              at,
		InclusionPath:   tree.InclusionProof(t.tree, at, index).ToSlice(),
		ConsistencyPath: tree.ConsistencyProof(t.tree, at, options.RootIndex.Index).ToSlice(),
	}

	return
}
