package store

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/dgraph-io/badger/v2"
	"math"
)

// SetBatch adds many entries at once
func (t *Store) SetBatch(list schema.KVList, options ...WriteOption) (index *schema.Index, err error) {
	if err = list.Validate(); err != nil {
		return nil, err
	}
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	tsEntries := t.tree.NewBatch(&list)

	for i, kv := range list.KVs {
		if err = checkKey(kv.Key); err != nil {
			return nil, err
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: WrapValueWithTS(kv.Value, tsEntries[i].ts),
		}); err != nil {
			return nil, mapError(err)
		}
	}

	ts := tsEntries[len(tsEntries)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}

	for _, leafEntry := range tsEntries {
		if err = txn.SetEntry(&badger.Entry{
			Key:      treeKey(uint8(0), leafEntry.ts-1),
			Value:    refTreeKey(*leafEntry.h, *leafEntry.r),
			UserMeta: bitTreeEntry,
		}); err != nil {
			return nil, mapError(err)
		}
	}

	cb := func(err error) {
		if err == nil {
			for _, entry := range tsEntries {
				t.tree.Commit(entry)
			}
		} else {
			for _, entry := range tsEntries {
				t.tree.Discard(entry)
			}
		}

		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = mapError(txn.CommitAt(ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(ts, nil))
		cb(err)
	}
	return
}
