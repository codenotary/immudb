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
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/common"
	"math"
)

//Scan ...
func (d *db) Scan(options *schema.ScanRequest) (*schema.ItemList, error) {
	/*if isReservedKey(options.Prefix) {
		return nil, ErrInvalidKeyPrefix
	}

	if isReservedKey(options.Offset) {
		return nil, ErrInvalidOffset
	}
	*/

	//offsettedKey := options.Prefix

	/*if len(options.Offset) > 0 {
		offsettedKey = options.Offset
	}*/

	/*if options.Reverse {
		offsettedKey = append(offsettedKey, 0xFF)
	}*/

	var limit = options.Limit
	if limit == 0 {
		limit = math.MaxUint64
	}

	var items []*schema.Item
	i := uint64(0)

	r, err := store.NewReader(
		d.st,
		store.ReaderSpec{
			IsPrefix:   true,
			InitialKey: options.Prefix,
			AscOrder:   options.Reverse,
		})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	for {
		var item *schema.Item
		key, val, tx, err := r.Read()
		if err == tbtree.ErrNoMoreEntries {
			break
		}
		if err != nil {
			return nil, err
		}

		//Reference lookup
		if bytes.HasPrefix(key, common.SortedSetSeparator) {

		} else {
			var value []byte
			value, err := d.st.Resolve(val)
			if err != nil {
				return nil, err
			}
			item = &schema.Item{
				Key:   key,
				Value: value,
				Tx:    tx,
			}
			if err != nil {
				return nil, err
			}
		}

		items = append(items, item)
		if i++; i == limit {
			break
		}
	}

	return &schema.ItemList{
		Items: items,
	}, nil
}
