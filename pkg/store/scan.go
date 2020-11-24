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
	"github.com/dgraph-io/badger/v2"
)

// Scan fetch the entries having the specified key prefix
func (t *Store) Scan(options schema.ScanOptions) (list *schema.ItemList, err error) {
	if isReservedKey(options.Prefix) {
		return nil, ErrInvalidKeyPrefix
	}

	if isReservedKey(options.Offset) {
		return nil, ErrInvalidOffset
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   int(options.Limit),
		Prefix:         options.Prefix,
		Reverse:        options.Reverse,
	})
	defer it.Close()

	offsettedKey := options.Prefix

	if len(options.Offset) > 0 {
		offsettedKey = options.Offset
	}

	if options.Reverse {
		offsettedKey = append(offsettedKey, 0xFF)
	}

	it.Seek(offsettedKey)

	if len(options.Offset) > 0 && it.Valid() {
		it.Next() // skip the offset item
	}

	var limit = options.Limit
	if limit == 0 {
		// we're reusing max batch count to enforce the default scan limit
		limit = uint64(t.db.MaxBatchCount())
	}

	var items []*schema.Item
	i := uint64(0)

	for ; it.Valid(); it.Next() {
		var item *schema.Item

		if it.Item().UserMeta()&bitReferenceEntry == bitReferenceEntry {
			if !options.Deep {
				continue
			}
			var refKey []byte

			err = it.Item().Value(func(val []byte) error {
				refKey, _ = UnwrapValueWithTS(val)
				return nil
			})
			if err != nil {
				return nil, err
			}

			refKey, flag, refIndex := UnwrapZIndexReference(refKey)

			// here check for index reference, if present we resolve reference with itemAt
			if flag == byte(1) {
				idx, key, val, err := t.itemAt(refIndex + 1) // itemAt returns index
				if err != nil {
					return nil, err
				}

				item = &schema.Item{
					Key:   key,
					Value: val,
					Index: idx,
				}
			} else {
				if ref, err := txn.Get(refKey); err == nil {
					item, err = itemToSchema(refKey, ref)
					if err != nil {
						return nil, err
					}
				}
			}
		} else {
			item, err = itemToSchema(nil, it.Item())
			if err != nil {
				return nil, err
			}
		}

		items = append(items, item)
		if i++; i == limit {
			break
		}
	}

	list = &schema.ItemList{
		Items: items,
	}

	return
}

// ZScan The SCAN command is used in order to incrementally iterate over a collection of elements.
func (t *Store) ZScan(options schema.ZScanOptions) (list *schema.ZItemList, err error) {
	if len(options.Set) == 0 || isReservedKey(options.Set) {
		return nil, ErrInvalidSet
	}

	if isReservedKey(options.Offset) {
		return nil, ErrInvalidOffset
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	set := WrapSeparatorToSet(options.Set)

	offsetKey := set

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   int(options.Limit),
		Prefix:         set,
		Reverse:        options.Reverse,
	})
	defer it.Close()

	// here we compose the offset if Min score filter is provided only if is not reversed order
	if options.Min != nil && !options.Reverse {
		offsetKey = AppendScoreToSet(options.Set, options.Min.Score)
	}
	// here we compose the offset if Max score filter is provided only if is reversed order
	if options.Max != nil && options.Reverse {
		offsetKey = AppendScoreToSet(options.Set, options.Max.Score)
	}
	// if offset is provided by client it takes precedence
	if len(options.Offset) > 0 {
		offsetKey = options.Offset
	}

	if options.Reverse {
		offsetKey = append(offsetKey, 0xFF)
	}

	it.Seek(offsetKey)

	if len(options.Offset) > 0 && it.Valid() {
		it.Next() // skip the offset item
	}

	var limit = options.Limit
	if limit == 0 {
		// we're reusing max batch count to enforce the default scan limit
		limit = uint64(t.db.MaxBatchCount())
	}

	var items []*schema.ZItem
	i := uint64(0)

	for ; it.Valid(); it.Next() {

		var zitem *schema.ZItem
		var item *schema.Item
		var sortedSetItemKey []byte
		var sortedSetItemIndex uint64

		if it.Item().UserMeta()&bitReferenceEntry == bitReferenceEntry {
			var refKey []byte

			err = it.Item().Value(func(val []byte) error {
				refKey, _ = UnwrapValueWithTS(val)
				sortedSetItemKey = it.Item().KeyCopy(nil)
				sortedSetItemIndex = it.Item().Version() - 1
				return nil
			})
			if err != nil {
				return nil, err
			}

			refKey, flag, refIndex := UnwrapZIndexReference(refKey)

			// here check for index reference, if present we resolve reference with itemAt
			if flag == byte(1) {
				idx, key, val, err := t.itemAt(refIndex + 1) // itemAt returns index
				if err != nil {
					return nil, err
				}

				item = &schema.Item{
					Key:   key,
					Value: val,
					Index: idx,
				}
			} else {
				if ref, err := txn.Get(refKey); err == nil {
					item, err = itemToSchema(refKey, ref)
					if err != nil {
						return nil, err
					}
				}
			}
		}

		if item != nil {
			zitem = &schema.ZItem{
				Item:          item,
				Score:         SetKeyScore(sortedSetItemKey, options.Set),
				CurrentOffset: sortedSetItemKey,
				Index:         sortedSetItemIndex,
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
	list = &schema.ZItemList{
		Items: items,
	}

	return
}

// IScan iterates over all entries by the insertion order
func (t *Store) IScan(options schema.IScanOptions) (list *schema.Page, err error) {
	page := &schema.Page{}
	page.More = true

	s := uint64(0)

	if options.PageNumber > 1 {
		s += (options.PageNumber - 1) * options.PageSize
	}

	for {
		item, err := t.ByIndex(schema.Index{Index: s})
		if err != nil {
			if err == ErrIndexNotFound {
				page.More = false
				break
			} else {
				return nil, err
			}
		}

		if item == nil {
			break
		}

		page.Items = append(page.Items, item)

		s++
		if uint64(len(page.Items)) >= options.PageSize {
			if _, err := t.ByIndex(schema.Index{Index: s}); err != nil {
				if err == ErrIndexNotFound {
					page.More = false
				}
			}
			break
		}
	}

	if len(page.Items) == 0 {
		return nil, ErrIndexNotFound
	}

	return page, nil
}
