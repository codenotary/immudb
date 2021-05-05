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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
)

type indexer struct {
	store *ImmuStore

	indexPath string

	index        *tbtree.TBtree
	running      bool
	closed       bool
	cancellation chan struct{}

	wHub *watchers.WatchersHub

	compactionMutex sync.Mutex
	mutex           sync.Mutex
}

func newIndexer(store *ImmuStore, indexDirname string, indexOpts *tbtree.Options, maxWaitees int) (*indexer, error) {
	indexPath := filepath.Join(store.path, indexDirname)

	index, err := tbtree.Open(indexPath, indexOpts)
	if err != nil {
		return nil, err
	}

	var wHub *watchers.WatchersHub
	if maxWaitees > 0 {
		wHub = watchers.New(0, maxWaitees)
	}

	return &indexer{
		store:     store,
		indexPath: indexPath,
		index:     index,
		wHub:      wHub,
	}, nil
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

	idx.closed = true

	return idx.index.Close()
}

func (idx *indexer) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	if idx.wHub != nil {
		return idx.wHub.WaitFor(txID, cancellation)
	}

	return watchers.ErrMaxWaitessLimitExceeded
}

func (idx *indexer) CompactIndex() error {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	compactedIndexID, err := idx.index.CompactIndex()
	if err != nil {
		return err
	}

	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.stop()
	defer idx.resume()

	return idx.replaceIndex(compactedIndexID)
}

func (idx *indexer) replaceIndex(compactedIndexID uint64) error {
	opts := idx.index.GetOptions()

	err := idx.index.Close()
	if err != nil {
		return err
	}

	nLogPath := filepath.Join(idx.indexPath, "nodes")
	err = os.RemoveAll(nLogPath)
	if err != nil {
		return err
	}

	cLogPath := filepath.Join(idx.indexPath, "commit")
	err = os.RemoveAll(cLogPath)
	if err != nil {
		return err
	}

	cnLogPath := filepath.Join(idx.indexPath, fmt.Sprintf("nodes_%d", compactedIndexID))
	ccLogPath := filepath.Join(idx.indexPath, fmt.Sprintf("commit_%d", compactedIndexID))

	err = os.Rename(cnLogPath, nLogPath)
	if err != nil {
		return err
	}

	err = os.Rename(ccLogPath, cLogPath)
	if err != nil {
		return err
	}

	index, err := tbtree.Open(idx.indexPath, opts)
	if err != nil {
		return err
	}

	idx.index = index
	return nil
}

func (idx *indexer) Resume() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.resume()
	return nil
}

func (idx *indexer) resume() {
	if !idx.running {
		idx.cancellation = make(chan struct{})
		go idx.doIndexing()
		idx.running = true
		idx.store.notify(Info, true, "Indexing in progress at '%s'", idx.store.path)
	}
}

func (idx *indexer) Stop() error {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.stop()
	return nil
}

func (idx *indexer) stop() {
	if idx.running {
		close(idx.cancellation)
		idx.running = false
		idx.store.notify(Info, true, "Indexing gracefully stopped at '%s'", idx.store.path)
	}
}

func (idx *indexer) doIndexing() {
	for {
		lastIndexedTx := idx.index.Ts()

		if idx.wHub != nil {
			idx.wHub.DoneUpto(lastIndexedTx)
		}

		err := idx.store.wHub.WaitFor(lastIndexedTx+1, idx.cancellation)
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

		err = idx.indexSince(lastIndexedTx+1, 10)
		if err == ErrAlreadyClosed {
			return
		}
		if err != nil {
			idx.store.notify(Error, true, "Indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (idx *indexer) indexSince(txID uint64, limit int) error {
	tx, err := idx.store.fetchAllocTx()
	if err != nil {
		return err
	}
	defer idx.store.releaseAllocTx(tx)

	txReader, err := idx.store.newTxReader(txID, false, tx)
	if err == ErrNoMoreEntries {
		return nil
	}
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

		for i, e := range txEntries {
			var b [szSize + offsetSize + sha256.Size]byte
			binary.BigEndian.PutUint32(b[:], uint32(e.vLen))
			binary.BigEndian.PutUint64(b[szSize:], uint64(e.vOff))
			copy(b[szSize+offsetSize:], e.hVal[:])

			idx.store._kvs[i].K = e.key()
			idx.store._kvs[i].V = b[:]
		}

		err = idx.index.BulkInsert(idx.store._kvs[:len(txEntries)])
		if err != nil {
			return err
		}
	}

	return nil
}
