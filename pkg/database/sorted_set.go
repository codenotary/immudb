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
	"bytes"
	"fmt"
	"math"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/common"
)

// ZAdd adds a score for an existing key in a sorted set
// As a parameter of ZAddOptions is possible to provide the associated index of the provided key. In this way, when resolving reference, the specified version of the key will be returned.
// If the index is not provided the resolution will use only the key and last version of the item will be returned
// If ZAddOptions.index is provided key is optional
func (d *db) ZAdd(zaddOpts *schema.ZAddRequest) (*schema.TxMetadata, error) {

	ik, referenceValue, err := d.getSortedSetKeyVal(zaddOpts, false)
	if err != nil {
		return nil, err
	}

	meta, err := d.st.Commit([]*store.KV{{Key: ik, Value: referenceValue}})

	return schema.TxMetatadaTo(meta), err
}

// ZScan ...
func (d *db) ZScan(options *schema.ZScanRequest) (*schema.ZItemList, error) {
	/*if len(options.Set) == 0 || isReservedKey(options.Set) {
		return nil, ErrInvalidSet
	}

	if isReservedKey(options.Offset) {
		return nil, ErrInvalidOffset
	}*/

	offsetKey := common.WrapSeparatorToSet(options.Set)

	// here we compose the offset if Min score filter is provided only if is not reversed order
	if options.Min != nil && !options.Reverse {
		offsetKey = common.AppendScoreToSet(options.Set, options.Min.Score)
	}
	// here we compose the offset if Max score filter is provided only if is reversed order
	if options.Max != nil && options.Reverse {
		offsetKey = common.AppendScoreToSet(options.Set, options.Max.Score)
	}
	// if offset is provided by client it takes precedence
	if len(options.Offset) > 0 {
		offsetKey = options.Offset
	}

	r, err := store.NewReader(
		d.st,
		store.ReaderSpec{
			IsPrefix:   true,
			InitialKey: offsetKey,
			AscOrder:   options.Reverse,
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var items []*schema.ZItem
	i := uint64(0)

	var limit = options.Limit
	if limit == 0 {
		// we're reusing max batch count to enforce the default scan limit
		limit = math.MaxUint64
	}

	for {
		sortedSetItemKey, value, sortedSetItemIndex, err := r.Read()
		if err == tbtree.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		refVal, err := d.st.Resolve(value)
		if err != nil {
			return nil, err
		}

		var zitem *schema.ZItem
		var item *schema.Item

		//Reference lookup
		if bytes.HasPrefix(sortedSetItemKey, common.SortedSetSeparator) {

			refKey, refAtTx := common.UnwrapReferenceAt(refVal)

			// here check for index reference, if present we resolve reference with itemAt
			if refAtTx > 0 {
				if err = d.st.ReadTx(refAtTx, d.tx1); err != nil {
					return nil, err
				}
				val, err := d.st.ReadValue(d.tx1, refKey)
				if err != nil {
					return nil, err
				}

				item = &schema.Item{Key: refKey, Value: val, Tx: refAtTx}
			} else {
				item, err = d.Get(&schema.KeyRequest{Key: refKey})
				if err != nil {
					return nil, err
				}
			}
		}

		if item != nil {
			zitem = &schema.ZItem{
				Item:          item,
				Score:         common.SetKeyScore(sortedSetItemKey, options.Set),
				CurrentOffset: sortedSetItemKey,
				Tx:            sortedSetItemIndex,
			}
		}

		// Guard to ensure that score match the filter range if filter is provided
		if options.Min != nil && zitem.Score < options.Min.Score {
			continue
		}
		if options.Max != nil && zitem.Score > options.Max.Score {
			continue
		}

		items = append(items, zitem)
		if i++; i == limit {
			break
		}
	}

	list := &schema.ZItemList{
		Items: items,
	}

	return list, nil
}

//SafeZAdd ...
func (d *db) VerifiableZAdd(opts *schema.VerifiableZAddRequest) (*schema.VerifiableTx, error) {
	//return d.st.SafeZAdd(*opts)
	return nil, fmt.Errorf("Functionality not yet supported: %s", "SafeZAdd")
}

// getSortedSetKeyVal return a key value pair that represent a sorted set entry.
// If skipPersistenceCheck is true and index is not provided reference lookup is disabled.
// This is used in Ops, to enable an key value creation with reference insertion in the same transaction.
func (d *db) getSortedSetKeyVal(zaddOpts *schema.ZAddRequest, skipPersistenceCheck bool) (k, v []byte, err error) {

	var referenceValue []byte
	var index uint64
	var key []byte
	if zaddOpts.AtTx > 0 {
		if !skipPersistenceCheck {
			if err := d.st.ReadTx(uint64(zaddOpts.AtTx), d.tx1); err != nil {
				return nil, nil, err
			}
			// check if specific key exists at the referenced index
			if _, err := d.st.ReadValue(d.tx1, zaddOpts.Key); err != nil {
				return nil, nil, ErrIndexKeyMismatch
			}
			key = zaddOpts.Key
		} else {
			key = zaddOpts.Key
		}
		// here the index is appended the reference value
		// In case that skipPersistenceCheck == true index need to be assigned carefully
		index = uint64(zaddOpts.AtTx)
	} else {
		i, err := d.Get(&schema.KeyRequest{Key: zaddOpts.Key})
		if err != nil {
			return nil, nil, err
		}
		if bytes.Compare(i.Key, zaddOpts.Key) != 0 {
			return nil, nil, ErrIndexKeyMismatch
		}
		key = zaddOpts.Key
		// Index has not to be stored inside the reference if not submitted by the client. This is needed to permit verifications in SDKs
		index = 0
	}
	ik := common.BuildSetKey(key, zaddOpts.Set, zaddOpts.Score.Score, index)

	// append the index to the reference. In this way the resolution will be index based
	referenceValue = common.WrapReferenceAt(key, index)

	return ik, referenceValue, err
}
