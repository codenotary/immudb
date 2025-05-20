/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/codenotary/immudb/v2/embedded/container"
	"github.com/codenotary/immudb/v2/embedded/logger"
	"github.com/codenotary/immudb/v2/embedded/tbtree"
	"github.com/codenotary/immudb/v2/embedded/util/backoff"
	"github.com/codenotary/immudb/v2/embedded/watchers"
)

var (
	ErrIndexingStopped       = errors.New("indexing stopped")
	ErrBufferNotFullyFlushed = errors.New("buffer not fully flushed")
)

// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmds
const maxEntryValueSize = lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen

type indexEntry struct {
	index *index
}

type Indexer struct {
	logger logger.Logger

	mtx   sync.RWMutex
	queue *container.Dequeue[indexEntry]

	closed bool

	// tracks the total number of transactions available for indexing across all ledgers
	indexingWHub *watchers.WatchersHub

	ctx    context.Context
	cancel context.CancelFunc

	backpressure backoff.Backoff

	wb *tbtree.WriteBuffer

	vEntrybuf [maxEntryValueSize]byte
	tx        *Tx
}

func NewIndexer(
	opts *Options,
	wb *tbtree.WriteBuffer,
	indexingWHub *watchers.WatchersHub,
) Indexer {
	wb.Grow(1)

	tx := NewTx(opts.MaxTxEntries, opts.MaxKeyLen)

	ctx, cancel := context.WithCancel(context.Background())

	return Indexer{
		ctx:          ctx,
		cancel:       cancel,
		logger:       opts.logger,
		tx:           tx,
		wb:           wb,
		indexingWHub: indexingWHub,
		queue:        container.NewDequeue[indexEntry](10),
		backpressure: backoff.Backoff{
			MinDelay:   opts.IndexOpts.BackpressureMinDelay,
			MaxDelay:   opts.IndexOpts.BackpressureMaxDelay,
			MaxRetries: -1,
		},
	}
}

func (idx *Indexer) Start() {
	go idx.doIndexing()
}

func (indexer *Indexer) doIndexing() {
	defer func() {
		indexer.logger.Infof("exiting from indexing loop")
	}()

	for {
		doneUpTo, _, err := indexer.indexingWHub.Status()
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			break
		}

		for indexer.indexNext() {
		}

		err = indexer.indexingWHub.WaitFor(indexer.Context(), doneUpTo+1)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			break
		}
	}
}

func (indexer *Indexer) Context() context.Context {
	indexer.mtx.RLock()
	defer indexer.mtx.RUnlock()

	return indexer.ctx
}

func (indexer *Indexer) indexNext() bool {
	var ready bool
	_ = indexer.backpressure.Retry(func(n int) error {
		var err error
		ready, err = indexer.tryIndexNext()

		if errors.Is(err, tbtree.ErrWriteBufferFull) {
			if err := indexer.tryFlushBuffer(); err != nil {
				return err
			}
			return nil
		}

		if err != nil {
			indexer.logger.Warningf("while attempting indexing: %s, attempt=%d", err, n+1)
		}
		return err
	})
	return ready
}

func (indexer *Indexer) tryIndexNext() (bool, error) {
	idx := indexer.popIndex()
	if idx == nil {
		return false, nil
	}

	push := true
	defer func() {
		if push {
			indexer.pushIndex(idx, false)
		}
	}()

	if !idx.shouldIndex() {
		return false, fmt.Errorf("unexpected attempt to index up-to-date index at path %s", idx.path)
	}

	upToTx := idx.ledger.LastCommittedTxID()
	if srcIdx := idx.srcIdx; srcIdx != nil && upToTx > srcIdx.Ts() {
		upToTx = srcIdx.Ts()
	}

	// index must be at least as up to date as its source indexer
	if upToTx <= idx.Ts() {
		return true, nil
	}

	err := indexer.indexUpTo(idx, upToTx)
	switch {
	case errors.Is(err, watchers.ErrAlreadyClosed),
		errors.Is(err, ErrAlreadyClosed):
		push = false
		err = nil
	case errors.Is(err, tbtree.ErrTreeLocked):
		err = nil
	}
	return true, err
}

func (indexer *Indexer) pushIndex(idx *index, cancelCtx bool) {
	indexer.mtx.Lock()

	indexer.queue.PushBack(indexEntry{idx})

	if cancelCtx {
		if cancel := indexer.cancel; cancel != nil {
			ctx, newCancel := context.WithCancel(context.Background())

			indexer.ctx = ctx
			indexer.cancel = newCancel

			cancel()
		}
	}

	indexer.mtx.Unlock()
}

func (indexer *Indexer) popIndex() *index {
	indexer.mtx.Lock()
	defer indexer.mtx.Unlock()

	numIndexes := indexer.queue.Len()
	for n := 0; n < numIndexes; n++ {
		e, ok := indexer.queue.PopFront()
		if !ok {
			break
		}

		if e.index.IndexingLag() > 0 {
			return e.index
		}

		indexer.queue.PushBack(e)
	}
	return nil
}

func (indexer *Indexer) indexUpTo(idx *index, upToTx uint64) error {
	for txID := idx.Ts() + 1; txID <= upToTx; txID++ {
		err := idx.ledger.ReadTxAt(txID, indexer.tx)
		if err != nil {
			return err
		}

		entriesIndexed, err := indexer.indexEntries(idx, indexer.tx)
		if err != nil {
			// If the write buffer fills up while indexing transaction T, a flush may be required.
			// We can persist the snapshot, but T's entries must stay hidden.
			// Thus, we must track the last fully indexed transaction T and the number of indexed entries in T+1.
			if err := idx.advanceTs(txID-1, entriesIndexed); err != nil {
				return err
			}
			return err
		}
	}
	return idx.advanceTs(upToTx, 0)
}

func (indexer *Indexer) indexEntries(idx *index, tx *Tx) (uint32, error) {
	entries := tx.Entries()
	if len(entries) == 0 {
		return 0, nil
	}

	n := idx.EntriesIndexedAtTs(tx.header.ID)
	if n >= uint32(len(entries)) {
		return n, nil
	}

	for i := range entries[n:] {
		nEntry := uint32(i) + n
		e := entries[nEntry]

		sourceKey, indexEntry, shouldIndex, err := indexer.mapEntryAt(idx, tx, int(nEntry))
		if err != nil {
			return n + uint32(i), err
		}

		if !shouldIndex {
			continue
		}

		if idx.srcIdx != nil {
			if err := indexer.markPrevEntryAsDeleted(
				idx,
				e.key(),
				sourceKey,
				indexEntry.Key,
				tx.header.ID,
				nEntry,
			); err != nil {
				return 0, err
			}
		}

		if err := idx.InsertAdvance(indexEntry, tx.header.ID-1, nEntry+1); err != nil {
			return n + uint32(i), err
		}
	}
	return math.MaxUint32, nil
}

func (indexer *Indexer) markPrevEntryAsDeleted(
	idx *index,
	key []byte,
	sourceKey []byte,
	targetKey []byte,
	txID uint64,
	nEntry uint32,
) error {
	srcIdx := idx.srcIdx

	_, prevTxID, _, err := srcIdx.GetBetween(sourceKey, 0, txID-1)
	if errors.Is(err, tbtree.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	prevEntry, prevTxHdr, err := srcIdx.ledger.ReadTxEntry(prevTxID, key, false)
	if err != nil {
		return err
	}

	var txmd []byte
	if prevTxHdr.Metadata != nil {
		txmd = prevTxHdr.Metadata.Bytes()
	}

	var kvmd *KVMetadata
	if prevEntry.Metadata() != nil {
		kvmd = prevEntry.Metadata()
	} else {
		kvmd = NewKVMetadata()
	}
	kvmd.AsDeleted(true)

	e, err := indexer.getIndexEntry(
		idx,
		sourceKey,
		txID,
		prevEntry,
		txmd,
		kvmd.Bytes(),
	)
	if err != nil {
		return err
	}

	if bytes.Equal(targetKey, e.Key) {
		return nil
	}

	if !hasPrefix(e.Key, idx.spec.TargetPrefix) {
		return fmt.Errorf("%w: the target entry mapper has not generated a key with the specified target prefix", ErrIllegalArguments)
	}

	err = idx.InsertAdvance(e, txID-1, nEntry)
	if errors.Is(err, tbtree.ErrInvalidTimestamp) {
		// the key was already indexed in a previous attempt
		return nil
	}
	return err
}

func (indexer *Indexer) tryFlushBuffer() error {
	allFlushed, err := indexer.tryFlushIndexes()
	if err != nil {
		return err
	}

	if allFlushed {
		indexer.wb.Reset()
		return nil
	}
	return ErrBufferNotFullyFlushed
}

func (indexer *Indexer) flushIndexes() error {
	for {
		allFlushed, err := indexer.tryFlushIndexes()
		if err != nil {
			return err
		}

		if allFlushed {
			return nil
		}
	}
}

func (indexer *Indexer) tryFlushIndexes() (bool, error) {
	indexer.mtx.RLock()
	defer indexer.mtx.RUnlock()

	// TODO: indexes may be concurrently flushed
	nFlushed := 0

	numIndexes := indexer.queue.Len()
	for n := 0; n < numIndexes; n++ {
		e, ok := indexer.queue.PopFront()
		if !ok {
			return false, fmt.Errorf("queue is empty")
		}

		err := e.index.tree.TryFlush()
		if errors.Is(err, tbtree.ErrTreeLocked) {
			indexer.queue.PushBack(e)
			continue
		} else if err != nil {
			indexer.queue.PushBack(e)
			return false, err
		} else {
			nFlushed++
		}

		indexer.queue.PushBack(e)
	}
	return nFlushed == numIndexes, nil
}

func (indexer *Indexer) mapEntryAt(idx *index, tx *Tx, i int) ([]byte, tbtree.Entry, bool, error) {
	e := tx.entries[i]

	var txmd []byte
	if tx.header.Metadata != nil {
		txmd = tx.header.Metadata.Bytes()
	}

	if e.md != nil && e.md.NonIndexable() {
		return nil, tbtree.Entry{}, false, nil
	}

	if !bytes.HasPrefix(e.key(), idx.spec.SourcePrefix) {
		return nil, tbtree.Entry{}, false, nil
	}

	sourceKey, err := indexer.mapKey(idx.ledger, e.key(), e.vLen, e.vOff, e.hVal, idx.spec.SourceEntryMapper)
	if err != nil {
		return nil, tbtree.Entry{}, false, err
	}

	var kvmd []byte
	if e.Metadata() != nil {
		kvmd = e.Metadata().Bytes()
	}

	indexEntry, err := indexer.getIndexEntry(idx, sourceKey, tx.header.ID, e, txmd, kvmd)
	return sourceKey, indexEntry, err == nil, err
}

func (indexer *Indexer) getIndexEntry(
	idx *index,
	sourceKey []byte,
	txID uint64,
	e *TxEntry,
	txmd []byte,
	kvmd []byte,
) (tbtree.Entry, error) {
	targetKey, err := indexer.mapKey(idx.ledger, sourceKey, e.vLen, e.vOff, e.hVal, idx.spec.TargetEntryMapper)
	if err != nil {
		return tbtree.Entry{}, err
	}

	if !bytes.HasPrefix(targetKey, idx.spec.TargetPrefix) {
		return tbtree.Entry{}, fmt.Errorf("%w: the target entry mapper has not generated a key with the specified target prefix", ErrIllegalArguments)
	}

	n := serializeIndexableEntry(indexer.vEntrybuf[:], txmd, e, kvmd)

	return tbtree.Entry{
		Ts:    txID,
		HC:    0,
		HOff:  tbtree.OffsetNone,
		Key:   targetKey, // TODO: target key should also be buffered
		Value: indexer.vEntrybuf[:n],
	}, nil
}

func serializeIndexableEntry(b []byte, txmd []byte, e *TxEntry, kvmd []byte) int {
	n := 0

	txmdLen := len(txmd)

	binary.BigEndian.PutUint32(b[n:], uint32(e.vLen))
	n += lszSize

	binary.BigEndian.PutUint64(b[n:], uint64(e.vOff))
	n += offsetSize

	copy(b[n:], e.hVal[:])
	n += sha256.Size

	binary.BigEndian.PutUint16(b[n:], uint16(txmdLen))
	n += sszSize

	copy(b[n:], txmd)
	n += txmdLen

	kvmdLen := len(kvmd)

	binary.BigEndian.PutUint16(b[n:], uint16(kvmdLen))
	n += sszSize

	copy(b[n:], kvmd)
	n += kvmdLen

	return n
}

func (indexer *Indexer) mapKey(ledger IndexableLedger, key []byte, vLen int, vOff int64, hVal [sha256.Size]byte, mapper EntryMapper) (mappedKey []byte, err error) {
	if mapper == nil {
		return key, nil
	}

	valReader, err := ledger.ValueReaderAt(vLen, vOff, hVal, false)
	if err != nil {
		return nil, err
	}
	return mapper(key, valReader)
}

func (indexer *Indexer) Indexes() int {
	indexer.mtx.RLock()
	defer indexer.mtx.RUnlock()

	return indexer.queue.Len()
}

func (indexer *Indexer) Close() error {
	indexer.mtx.Lock()
	defer indexer.mtx.Unlock()

	if indexer.closed {
		return ErrAlreadyClosed
	}

	indexer.closed = true
	if indexer.cancel != nil {
		indexer.cancel()
	}
	return nil
}
