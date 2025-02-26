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
	"time"

	"github.com/codenotary/immudb/embedded/container"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/util/backoff"
)

var (
	ErrIndexingStopped       = errors.New("indexing stopped")
	ErrBufferNotFullyFlushed = errors.New("buffer not fully flushed")
)

// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmds
const maxEntryValueSize = lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen

type Indexer struct {
	mtx sync.RWMutex

	logger         logger.Logger
	compactionThld float64

	ctx          context.Context
	backpressure backoff.Backoff

	wb    *tbtree.WriteBuffer
	queue *container.Dequeue[indexEntry]

	vEntrybuf [maxEntryValueSize]byte
	tx        *Tx
}

func NewIndexer(
	opts *Options,
	wb *tbtree.WriteBuffer,
) Indexer {
	wb.Grow(1)

	tx := NewTx(opts.MaxTxEntries, opts.MaxKeyLen)

	return Indexer{
		ctx:            context.Background(),
		logger:         opts.logger,
		tx:             tx,
		wb:             wb,
		compactionThld: opts.IndexOpts.CompactionThld,
		queue:          container.NewDequeue[indexEntry](10),
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

// TODO: for reading a tx we can use a normal TxBuffer

func (indexer *Indexer) doIndexing() error {
	indexWhileReady := func() {
		ready := true
		for ready {
			ready = indexer.indexNext()
		}
	}

	for {
		indexWhileReady()

		// TODO: use a condition variable to listen to all ledgers.
		time.Sleep(time.Millisecond * 10)
	}
}

func (indexer *Indexer) indexNext() bool {
	var ready bool
	_ = indexer.backpressure.Retry(func(_ int) error {
		var err error
		ready, err = indexer.tryIndexNext()
		// TODO: add debug log
		return err
	})
	return ready
}

func (indexer *Indexer) tryIndexNext() (bool, error) {
	// TODO: need to use a deque + tracking of minimum instead
	// of a priority queue.
	idx := indexer.popIndex()
	if idx == nil {
		return false, nil
	}
	defer func() {
		indexer.pushIndex(idx)
	}()

	if !idx.shouldIndex() {
		panic("condition shouldIndex() should always be true")
	}

	err := indexer.indexUpTo(idx, idx.ledger.LastCommittedTxID())
	if errors.Is(err, tbtree.ErrTreeLocked) {
		// NOTE: if the index didn't manage to make progress, than
		// the indexer could get stack on it, while other indexes are waiting.

		// TODO: introduce a secondary queue containing indexes
		// which cannot progress immediately. An index is pushed to the main queue if progress was made,
		// to the second queue otherwise.

		// The next index to be indexed will be popped always from the first queue if that is non empty,
		// from the second queue otherwise.

		// The second queue could even be a standard queue.
		return true, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (indexer *Indexer) pushIndex(idx *index) {
	indexer.mtx.Lock()

	indexer.queue.PushBack(indexEntry{idx})

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

func (indexer *Indexer) indexUpTo(index *index, upToTx uint64) error {
	indexer.logger.Infof("indexing %s up to %d", index.path, upToTx)

	for txID := index.Ts() + 1; txID <= upToTx; txID++ {
		err := index.ledger.ReadTxAt(txID, indexer.tx)
		if err != nil {
			return err
		}

		entriesIndexed, err := indexer.indexEntries(index, indexer.tx)
		if err != nil {
			// If the write buffer fills up while indexing transaction T, a flush may be required.
			// We can persist the snapshot, but T's entries must stay hidden.
			// Thus, we must track the last fully indexed transaction T and the number of indexed entries in T+1.
			if err := index.advance(txID-1, entriesIndexed); err != nil {
				return err
			}
			return err
		}
	}
	return index.advance(upToTx, 0)
}

func (indexer *Indexer) indexEntries(index *index, tx *Tx) (uint32, error) {
	entries := tx.Entries()
	if len(entries) == 0 { // TODO: can this case happen?
		return 0, nil
	}

	n := index.EntriesIndexedAtTs(tx.header.ID)
	if n == math.MaxUint32 {
		return n, nil
	}

	for i := range entries[n:] {
		// TODO: entries should be read one by one from reader
		e, shouldIndex, err := indexer.mapEntryAt(index, tx, i)
		if err != nil {
			return n + uint32(i), err
		}

		if !shouldIndex {
			continue
		}

		if err := indexer.insert(index, &e); err != nil {
			return n + uint32(i), err
		}
	}
	return math.MaxUint32, nil
}

func (indexer *Indexer) insert(idx *index, e *tbtree.Entry) error {
	err := idx.tree.Insert(*e)
	if err == nil {
		return nil
	}

	if errors.Is(err, tbtree.ErrNoChunkAvailable) {
		panic("chunk not available error")
	}

	if errors.Is(err, tbtree.ErrWriteBufferFull) {
		err := indexer.tryFlushBuffer()
		if err != nil {
			return err
		}
		return idx.tree.Insert(*e)
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

	nFlushed := 0

	numIndexes := indexer.queue.Len()
	for n := 0; n < numIndexes; n++ {
		e, ok := indexer.queue.PopFront()
		if !ok {
			panic("queue is empty")
		}

		err := e.index.tree.TryFlush(context.Background(), false)
		if errors.Is(err, tbtree.ErrTreeLocked) {
			continue
		} else if err != nil {
			return false, err
		} else {
			nFlushed++
		}

		indexer.queue.PushBack(e)
	}
	return nFlushed == numIndexes, nil
}

func (indexer *Indexer) mapEntryAt(idx *index, tx *Tx, i int) (tbtree.Entry, bool, error) {
	e := tx.entries[i]

	var txmd []byte
	if tx.header.Metadata != nil {
		txmd = tx.header.Metadata.Bytes()
	}

	if e.md != nil && e.md.NonIndexable() {
		return tbtree.Entry{}, false, nil
	}

	if !bytes.HasPrefix(e.key(), idx.spec.SourcePrefix) {
		return tbtree.Entry{}, false, nil
	}

	sourceKey, err := indexer.mapKey(idx.ledger, e.key(), e.vLen, e.vOff, e.hVal, idx.spec.SourceEntryMapper)
	if err != nil {
		return tbtree.Entry{}, false, err
	}

	targetKey, err := indexer.mapKey(idx.ledger, sourceKey, e.vLen, e.vOff, e.hVal, idx.spec.TargetEntryMapper)
	if err != nil {
		return tbtree.Entry{}, false, err
	}

	if !bytes.HasPrefix(targetKey, idx.spec.TargetPrefix) {
		return tbtree.Entry{}, false, fmt.Errorf("%w: the target entry mapper has not generated a key with the specified target prefix", ErrIllegalArguments)
	}

	var kvmd []byte
	if e.Metadata() != nil {
		kvmd = e.Metadata().Bytes()
	}

	n := serializeIndexableEntry(indexer.vEntrybuf[:], txmd, e, kvmd)

	return tbtree.Entry{
		Ts:    tx.header.ID,
		HC:    0,
		HOff:  tbtree.OffsetNone,
		Key:   targetKey, // TODO: target key should also be buffered
		Value: indexer.vEntrybuf[:n],
	}, true, nil
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

func (indexer *Indexer) Close() error {
	indexer.mtx.Lock()
	defer indexer.mtx.Unlock()

	// TODO: signal the indexer thread to exit
	return nil
}

type indexEntry struct {
	index *index
}
