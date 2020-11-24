package store

import (
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/dgraph-io/badger/v2"
	"math"
)

// Reference adds a new entry who's value is an existing key
func (t *Store) Reference(refOpts *schema.ReferenceOptions, options ...WriteOption) (index *schema.Index, err error) {
	opts := makeWriteOptions(options...)
	if isReservedKey(refOpts.Key) {
		return nil, ErrInvalidKey
	}
	if isReservedKey(refOpts.Reference) {
		return nil, ErrInvalidReference
	}

	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	k, err := t.getReferenceVal(txn, refOpts, false)

	if err != nil {
		return nil, mapError(err)
	}

	tsEntry := t.tree.NewEntry(refOpts.Reference, k)

	if err = txn.SetEntry(&badger.Entry{
		Key:      refOpts.Reference,
		Value:    WrapValueWithTS(k, tsEntry.ts),
		UserMeta: bitReferenceEntry,
	}); err != nil {
		return nil, mapError(err)
	}

	index = &schema.Index{
		Index: tsEntry.ts - 1,
	}

	if err = txn.SetEntry(&badger.Entry{
		Key:      treeKey(uint8(0), tsEntry.ts-1),
		Value:    refTreeKey(*tsEntry.h, *tsEntry.r),
		UserMeta: bitTreeEntry,
	}); err != nil {
		return nil, mapError(err)
	}

	cb := func(err error) {
		if err == nil {
			t.tree.Commit(tsEntry)
		} else {
			t.tree.Discard(tsEntry)
		}
		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = mapError(txn.CommitAt(tsEntry.ts, cb)) // cb will be executed in a new goroutine
	} else {
		err = mapError(txn.CommitAt(tsEntry.ts, nil))
		cb(err)
	}

	return index, err
}
