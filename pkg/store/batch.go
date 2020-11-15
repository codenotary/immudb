package store

import (
	"crypto/sha256"
	"fmt"
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

// SetBatchAtomicOperations like SetBatch it permits many insertions at once.
// The difference is that is possible to to specify a list of a mix of key value set and zAdd insertions.
// If zAdd reference is not yet present on disk it's possible to add it as a regular key value and the reference is done onFly
func (t *Store) SetBatchAtomicOperations(ops *schema.AtomicOperations, options ...WriteOption) (index *schema.Index, err error) {

	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	var kvList schema.KVList
	var tsEntriesKv []*treeStoreEntry
	// In order to:
	// * make a memory efficient check system for keys that need to be referenced
	// * store the index of the future persisted zAdd referenced entries
	// we build a map in which we store sha256 sum as key and the index as value
	kmap := make(map[[32]byte]uint64)

	for _, op := range ops.Operations {
		switch x := op.Operation.(type) {
		case *schema.AtomicOperation_KVs:
			kvList.KVs = append(kvList.KVs, x.KVs)
			entry := t.tree.NewEntry(x.KVs.Key, x.KVs.Value)
			kmap[sha256.Sum256(x.KVs.Key)] = entry.Index()
			tsEntriesKv = append(tsEntriesKv, entry)
		case *schema.AtomicOperation_ZOpts:
			// zAdd arguments are converted in regular key value items and then batch generation
			skipPersistenceCheck := false
			if idx, exists := kmap[sha256.Sum256(x.ZOpts.Key)]; exists {
				skipPersistenceCheck = true
				x.ZOpts.Index = &schema.Index{Index: idx}
			}
			// if skipPersistenceCheck is true it means that the reference will be done with a key value that is not yet
			// persisted in the store, but it's present in the previous key value list.
			// if skipPersistenceCheck is false it means that the reference is already persisted on disk.
			k, v, err := t.getSortedSetKeyVal(txn, x.ZOpts, skipPersistenceCheck)
			if err != nil {
				return nil, err
			}
			kv := &schema.KeyValue{
				Key:   k,
				Value: v,
			}
			kvList.KVs = append(kvList.KVs, kv)
			entry := t.tree.NewEntry(kv.Key, kv.Value)
			tsEntriesKv = append(tsEntriesKv, entry)
		case nil:
			// The field is not set.
			continue
		default:
			return nil, fmt.Errorf("batch operation has unexpected type %T", x)
		}
	}
	if err = kvList.Validate(); err != nil {
		return nil, err
	}

	// storing key value items in badger
	for i, kv := range kvList.KVs {
		if err := checkKey(kv.Key); err != nil {
			return nil, err
		}
		var userMeta byte
		// if key is not present it means that current element is a zAdd type, then we need to flag it as a reference
		if _, exists := kmap[sha256.Sum256(kv.Key)]; !exists {
			// storing zAdd key value items in badger and flag them as reference
			userMeta = bitReferenceEntry
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:      kv.Key,
			Value:    WrapValueWithTS(kv.Value, tsEntriesKv[i].ts),
			UserMeta: userMeta,
		}); err != nil {
			return nil, mapError(err)
		}
	}

	// merkle tree elements generation
	ts := tsEntriesKv[len(tsEntriesKv)-1].ts
	index = &schema.Index{
		Index: ts - 1,
	}
	for _, leafEntry := range tsEntriesKv {
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
			for _, entry := range tsEntriesKv {
				t.tree.Commit(entry)
			}
		} else {
			for _, entry := range tsEntriesKv {
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
