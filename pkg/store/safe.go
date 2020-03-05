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
	"github.com/codenotary/merkletree"

	"github.com/dgraph-io/badger/v2"
)

func getPrevRootIdx(lastIndex uint64, rootIdx *schema.Index) (uint64, error) {
	if rootIdx != nil && rootIdx.Index > 0 {
		if lastIndex > rootIdx.Index {
			return 0, ErrInvalidRootIndex
		}
		return rootIdx.Index, nil
	}
	return 0, nil
}

func (t *Store) SafeSet(options schema.SafeSetOptions) (proof *schema.Proof, err error) {
	kv := options.Kv
	err = checkKey(kv.Key)
	if err != nil {
		return
	}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: kv.Value,
	}); err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(kv.Key, kv.Value)
	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	err = txn.CommitAt(tsEntry.ts, nil)
	if err != nil {
		t.tree.Discard(tsEntry)
		err = mapError(err)
		return
	}

	t.tree.Commit(tsEntry)
	t.tree.WaitUntil(index)

	t.tree.RLock()
	defer t.tree.RUnlock()

	at := t.tree.w - 1
	root := merkletree.Root(t.tree)

	proof = &schema.Proof{
		Leaf:            leaf,
		Index:           index,
		Root:            root[:],
		At:              at,
		InclusionPath:   merkletree.InclusionProof(t.tree, at, index).ToSlice(),
		ConsistencyPath: merkletree.ConsistencyProof(t.tree, at, prevRootIdx).ToSlice(),
	}

	return
}

func (t *Store) SafeGet(options schema.SafeGetOptions) (safeItem *schema.SafeItem, err error) {
	var item *schema.Item
	var i *badger.Item

	key := options.Key.Key
	err = checkKey(key)
	if err != nil {
		return
	}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	i, err = txn.Get(key)
	if err != nil {
		err = mapError(err)
		return
	}

	if err == nil && i.UserMeta()&bitReferenceEntry == bitReferenceEntry {
		var refkey []byte
		if refkey, err = i.ValueCopy(refkey); err != nil {
			return nil, err
		}
		i, err = txn.Get(refkey)
		key = i.Key()
		if err != nil {
			return
		}
	}

	item, err = itemToSchema(key, i)
	if err != nil {
		return
	}
	safeItem = &schema.SafeItem{
		Item: item,
	}

	t.tree.WaitUntil(item.Index)
	t.tree.RLock()
	defer t.tree.RUnlock()

	at := t.tree.w - 1
	root := merkletree.Root(t.tree)

	safeItem.Proof = &schema.Proof{
		Leaf:            item.Hash(),
		Index:           item.Index,
		Root:            root[:],
		At:              at,
		InclusionPath:   merkletree.InclusionProof(t.tree, at, item.Index).ToSlice(),
		ConsistencyPath: merkletree.ConsistencyProof(t.tree, at, prevRootIdx).ToSlice(),
	}

	return
}

func (t *Store) SafeReference(options schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	ro := options.Ro
	err = checkKey(ro.Key.Key)
	err = checkKey(ro.Reference.Key)
	if err != nil {
		return
	}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	i, err := txn.Get(ro.Key.Key)
	if err != nil {
		err = mapError(err)
		return
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      ro.Reference.Key,
		Value:    i.Key(),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(ro.Reference.Key, i.Key())

	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	err = txn.CommitAt(tsEntry.ts, nil)
	if err != nil {
		t.tree.Discard(tsEntry)
		err = mapError(err)
		return
	}

	t.tree.Commit(tsEntry)
	t.tree.WaitUntil(index)

	t.tree.RLock()
	defer t.tree.RUnlock()

	at := t.tree.w - 1
	root := merkletree.Root(t.tree)

	proof = &schema.Proof{
		Leaf:            leaf,
		Index:           index,
		Root:            root[:],
		At:              at,
		InclusionPath:   merkletree.InclusionProof(t.tree, at, index).ToSlice(),
		ConsistencyPath: merkletree.ConsistencyProof(t.tree, at, prevRootIdx).ToSlice(),
	}

	return
}
