/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
)

type indexer struct {
	path string

	store *ImmuStore
	tx    *Tx

	index *tbtree.TBtree

	cancellation chan struct{}
	wHub         *watchers.WatchersHub

	state     int
	stateCond *sync.Cond

	closed bool

	compactionMutex sync.Mutex
	mutex           sync.Mutex
}

type runningState = int

const (
	running runningState = iota
	stopped
	paused
)

func newIndexer(path string, store *ImmuStore, indexOpts *tbtree.Options, maxWaitees int) (*indexer, error) {
	index, err := tbtree.Open(path, indexOpts)
	if err != nil {
		return nil, err
	}

	var wHub *watchers.WatchersHub
	if maxWaitees > 0 {
		wHub = watchers.New(0, maxWaitees)
	}

	tx, err := store.fetchAllocTx()
	if err != nil {
		return nil, err
	}

	indexer := &indexer{
		store:     store,
		tx:        tx,
		path:      path,
		index:     index,
		wHub:      wHub,
		state:     stopped,
		stateCond: sync.NewCond(&sync.Mutex{}),
	}

	indexer.resume()

	return indexer, nil
}

func (idx *indexer) Ts() uint64 {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	return idx.index.Ts()
}

func (idx *indexer) Get(key []byte) (value []byte, tx uint64, hc uint64, err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	return idx.index.Get(key)
}

func (idx *indexer) History(key []byte, offset uint64, descOrder bool, limit int) (txs []uint64, err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.History(key, offset, descOrder, limit)
}

func (idx *indexer) Snapshot() (*tbtree.Snapshot, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.Snapshot()
}

func (idx *indexer) SnapshotSince(tx uint64) (*tbtree.Snapshot, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.SnapshotSince(tx)
}

func (idx *indexer) ExistKeyWith(prefix []byte, neq []byte) (bool, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return false, ErrAlreadyClosed
	}

	return idx.index.ExistKeyWith(prefix, neq)
}

func (idx *indexer) Sync() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	return idx.index.Sync()
}

func (idx *indexer) Close() error {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.stop()
	idx.wHub.Close()
	idx.store.releaseAllocTx(idx.tx)

	idx.closed = true

	return idx.index.Close()
}

func (idx *indexer) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	if idx.wHub != nil {
		return idx.wHub.WaitFor(txID, cancellation)
	}

	return watchers.ErrMaxWaitessLimitExceeded
}

func (idx *indexer) CompactIndex() (err error) {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.store.notify(Info, true, "Compacting index '%s'...", idx.store.path)

	defer func() {
		if err == nil {
			idx.store.notify(Info, true, "Index '%s' sucessfully compacted", idx.store.path)
		} else {
			idx.store.notify(Info, true, "Compaction of index '%s' returned: %v", idx.store.path, err)
		}
	}()

	_, err = idx.index.Compact()
	if err != nil {
		return err
	}

	return idx.restartIndex()
}

func (idx *indexer) stop() {
	idx.stateCond.L.Lock()
	idx.state = stopped
	close(idx.cancellation)
	idx.stateCond.L.Unlock()
	idx.stateCond.Signal()

	idx.store.notify(Info, true, "Indexing gracefully stopped at '%s'", idx.store.path)
}

func (idx *indexer) resume() {
	idx.stateCond.L.Lock()
	idx.state = running
	idx.cancellation = make(chan struct{})
	go idx.doIndexing(idx.cancellation)
	idx.stateCond.L.Unlock()

	idx.store.notify(Info, true, "Indexing in progress at '%s'", idx.store.path)
}

func (idx *indexer) restartIndex() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.stop()
	defer idx.resume()

	opts := idx.index.GetOptions()

	err := idx.index.Close()
	if err != nil {
		return err
	}

	index, err := tbtree.Open(idx.path, opts)
	if err != nil {
		return err
	}

	idx.index = index

	return err
}

func (idx *indexer) Resume() {
	idx.stateCond.L.Lock()
	idx.state = running
	idx.stateCond.L.Unlock()
	idx.stateCond.Signal()
}

func (idx *indexer) Pause() {
	idx.stateCond.L.Lock()
	idx.state = paused
	idx.stateCond.L.Unlock()
}

func (idx *indexer) doIndexing(cancellation <-chan struct{}) {
	for {
		lastIndexedTx := idx.index.Ts()

		if idx.wHub != nil {
			idx.wHub.DoneUpto(lastIndexedTx)
		}

		err := idx.store.WaitForTx(lastIndexedTx+1, cancellation)
		if err == watchers.ErrCancellationRequested || err == watchers.ErrAlreadyClosed {
			return
		}
		if err != nil {
			idx.store.notify(Error, true, "Indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}

		committedTxID, _, _ := idx.store.commitState()

		txsToIndex := committedTxID - lastIndexedTx
		idx.store.notify(Info, false, "%d transaction/s to be indexed at '%s'", txsToIndex, idx.store.path)

		idx.stateCond.L.Lock()
		for {
			if idx.state == stopped {
				idx.stateCond.L.Unlock()
				return
			}
			if idx.state == running {
				break
			}
			idx.stateCond.Wait()
		}
		idx.stateCond.L.Unlock()

		err = idx.indexSince(lastIndexedTx+1, 10)
		if err == ErrAlreadyClosed || err == tbtree.ErrAlreadyClosed {
			return
		}
		if err != nil {
			idx.store.notify(Error, true, "Indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}
	}
}

func (idx *indexer) indexSince(txID uint64, limit int) error {
	txReader, err := idx.store.newTxReader(txID, false, idx.tx)
	if err != nil {
		return err
	}

	for i := 0; i < limit; i++ {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		txEntries := tx.Entries()

		var txmd []byte

		if tx.header.Metadata != nil {
			txmd = tx.header.Metadata.Bytes()
		}

		txmdLen := len(txmd)

		for i, e := range txEntries {
			// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmd
			var b [lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen]byte
			o := 0

			binary.BigEndian.PutUint32(b[o:], uint32(e.vLen))
			o += lszSize

			binary.BigEndian.PutUint64(b[o:], uint64(e.vOff))
			o += offsetSize

			copy(b[o:], e.hVal[:])
			o += sha256.Size

			binary.BigEndian.PutUint16(b[o:], uint16(txmdLen))
			o += sszSize

			copy(b[o:], txmd)
			o += txmdLen

			var kvmd []byte

			if e.md != nil {
				kvmd = e.md.Bytes()
			}

			kvmdLen := len(kvmd)

			binary.BigEndian.PutUint16(b[o:], uint16(kvmdLen))
			o += sszSize

			copy(b[o:], kvmd)
			o += kvmdLen

			idx.store._kvs[i].K = e.key()
			idx.store._kvs[i].V = b[:o]
		}

		err = idx.index.BulkInsert(idx.store._kvs[:len(txEntries)])
		if err != nil {
			return err
		}
	}

	return nil
}
