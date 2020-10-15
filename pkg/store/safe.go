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
		if lastIndex < rootIdx.Index {
			return 0, ErrInvalidRootIndex
		}
		return rootIdx.Index, nil
	}
	return 0, nil
}

// SafeSet adds an entry and returns the inclusion proof for it and
// the consistency proof for the previous root
func (t *Store) SafeSet(options schema.SafeSetOptions) (proof *schema.Proof, err error) {
	kv := options.Kv

	if err = checkKey(kv.Key); err != nil {
		return nil, err
	}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	tsEntry := t.tree.NewEntry(kv.Key, kv.Value)

	if err = txn.SetEntry(&badger.Entry{
		Key:   kv.Key,
		Value: WrapValueWithTS(kv.Value, tsEntry.ts),
	}); err != nil {
		err = mapError(err)
		return
	}

	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

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

// SafeGet fetches the entry having the specified key together with the inclusion proof
// for it and the consistency proof for the current root
func (t *Store) SafeGet(options schema.SafeGetOptions) (safeItem *schema.SafeItem, err error) {
	var item *schema.Item
	var i *badger.Item
	key := options.Key

	if err = checkKey(key); err != nil {
		return nil, err
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
		var refKey []byte
		err = i.Value(func(val []byte) error {
			refKey, _ = UnwrapValueWithTS(val)
			return nil
		})
		if err != nil {
			return nil, err
		}
		i, err = txn.Get(refKey)
		key = i.Key()
		if err != nil {
			return nil, err
		}
	}

	item, err = itemToSchema(key, i)
	if err != nil {
		return nil, err
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

// SafeReference adds a reference entry to an existing key and returns the
// inclusion proof for it and the consistency proof for the previous root
func (t *Store) SafeReference(options schema.SafeReferenceOptions) (proof *schema.Proof, err error) {
	ro := options.Ro
	if err = checkKey(ro.Key); err != nil {
		return nil, err
	}
	if err = checkKey(ro.Reference); err != nil {
		return nil, err
	}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	i, err := txn.Get(ro.Key)
	if err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(ro.Reference, i.Key())

	if err = txn.SetEntry(&badger.Entry{
		Key:      ro.Reference,
		Value:    WrapValueWithTS(i.Key(), tsEntry.ts),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

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

// SafeZAdd adds the specified score and key to a sorted set and returns
// the inclusion proof for it and the consistency proof for the previous root
// As a parameter of SafeZAddOptions is possible to provide the associated index of the provided key. In this way, when resolving reference, the specified version of the key will be returned.
// If the index is not provided the resolution will use only the key and last version of the item will be returned
func (t *Store) SafeZAdd(options schema.SafeZAddOptions) (proof *schema.Proof, err error) {

	if err = checkKey(options.Zopts.Key); err != nil {
		return nil, err
	}
	if err = checkSet(options.Zopts.Set); err != nil {
		return nil, err
	}
	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	var referenceValue []byte
	if options.Zopts.Index != nil {
		// convert to internal timestamp for itemAt
		_, key, _, err := t.itemAt(options.Zopts.Index.Index + 1)
		if err != nil {
			err = mapError(err)
			return nil, err
		}
		// here we append the index to the reference value
		referenceValue = WrapZIndexReference(key, options.Zopts.Index)

	} else {
		var i *badger.Item
		i, err = txn.Get(options.Zopts.Key)
		if err != nil {
			err = mapError(err)
			return nil, err
		}
		// here we append a flag that the index reference was not specified. Thanks to this we will use only the key to calculate digest
		referenceValue = WrapZIndexReference(i.Key(), nil)
	}

	ik, err := SetKey(options.Zopts.Key, options.Zopts.Set, options.Zopts.Score)
	if err != nil {
		err = mapError(err)
		return
	}

	tsEntry := t.tree.NewEntry(ik, referenceValue)

	if err = txn.SetEntry(&badger.Entry{
		Key:      ik,
		Value:    WrapValueWithTS(referenceValue, tsEntry.ts),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

	index := tsEntry.Index()
	leaf := tsEntry.HashCopy()

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		err = mapError(err)
		return
	}

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

// BySafeIndex fetches the entry at the specified index together with the inclusion proof
// for it and the consistency proof for the current root
func (t *Store) BySafeIndex(options schema.SafeIndexOptions) (safeitem *schema.SafeItem, err error) {

	var item *schema.Item

	idx, key, value, err := t.itemAt(options.Index + 1)
	if err != nil {
		return nil, err
	}

	item = &schema.Item{Key: key, Value: value, Index: idx}

	prevRootIdx, err := getPrevRootIdx(t.tree.LastIndex(), options.RootIndex)
	if err != nil {
		return
	}

	safeItem := &schema.SafeItem{
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

	return safeItem, err
}
