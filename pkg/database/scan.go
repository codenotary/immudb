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
