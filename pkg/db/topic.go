/*
Copyright 2019 vChain, Inc.

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

package db

import (
	"math"
	"sync"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
)

type Topic struct {
	db    *badger.DB
	store *treeStore
	wg    sync.WaitGroup
	log   logger.Logger
}

func Open(options Options) (*Topic, error) {
	opt := options.dataStore()
	db, err := badger.OpenManaged(opt)
	if err != nil {
		return nil, err
	}

	t := &Topic{
		db:    db,
		store: newTreeStore(db, 750_000, opt.Logger),
		log:   opt.Logger,
	}

	t.log.Infof("Topic ready with directory: %s", opt.Dir)
	return t, nil
}

func (t *Topic) Close() error {
	defer t.log.Infof("Topic closed")
	t.wg.Wait()
	t.store.Close()
	return t.db.Close()
}

func (t *Topic) Wait() {
	t.wg.Wait()
}

func (t *Topic) SetBatch(kvPairs []KVPair, options ...WriteOption) (index uint64, err error) {
	opts := makeWriteOptions(options...)
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()

	for _, kv := range kvPairs {
		if kv.Key[0] == tsPrefix {
			err = InvalidKeyErr
		}
		if err = txn.SetEntry(&badger.Entry{
			Key:   kv.Key,
			Value: kv.Value,
		}); err != nil {
			return
		}
	}

	tsEntries := t.store.NewBatch(kvPairs)
	ts := tsEntries[len(tsEntries)-1].ts
	index = ts - 1

	cb := func(err error) {
		if err == nil {
			for _, entry := range tsEntries {
				t.store.Commit(entry)
			}
		} else {
			for _, entry := range tsEntries {
				t.store.Discard(entry)
			}
		}

		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = txn.CommitAt(ts, cb) // cb will be executed in a new goroutine
	} else {
		err = txn.CommitAt(ts, nil)
		cb(err)
	}
	return
}

func (t *Topic) Set(key, value []byte, options ...WriteOption) (index uint64, err error) {
	opts := makeWriteOptions(options...)
	if key[0] == tsPrefix {
		err = InvalidKeyErr
		return
	}
	txn := t.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	if err = txn.SetEntry(&badger.Entry{
		Key:   key,
		Value: value,
	}); err != nil {
		return
	}

	tsEntry := t.store.NewEntry(key, value)
	index = tsEntry.ts - 1

	cb := func(err error) {
		if err == nil {
			t.store.Commit(tsEntry)
		} else {
			t.store.Discard(tsEntry)
		}
		if opts.asyncCommit {
			t.wg.Done()
		}
	}

	if opts.asyncCommit {
		t.wg.Add(1)
		err = txn.CommitAt(tsEntry.ts, cb) // cb will be executed in a new goroutine
	} else {
		err = txn.CommitAt(tsEntry.ts, nil)
		cb(err)
	}

	return
}

func (t *Topic) Get(key []byte) (value []byte, index uint64, err error) {
	txn := t.db.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return
	}
	value, err = item.ValueCopy(nil)
	return value, item.Version() - 1, nil
}

func (t *Topic) HealthCheck() bool {
	_, _, err := t.Get([]byte{0})
	return err == nil || err == badger.ErrKeyNotFound
}
