/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
	"math/rand"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var ErrWriteStalling = errors.New("write stalling")

const (
	writeStallingSleepDurationMin = 10 * time.Millisecond
	writeStallingSleepDurationMax = 50 * time.Millisecond
)

type indexer struct {
	path string

	store *ImmuStore

	spec *IndexSpec

	tx *Tx

	maxBulkSize            int
	bulkPreparationTimeout time.Duration

	_kvs []*tbtree.KVT            //pre-allocated for multi-tx bulk indexing
	_val [DefaultMaxValueLen]byte //pre-allocated buffer to read entry values while mapping

	index *tbtree.TBtree

	ctx        context.Context
	cancelFunc context.CancelFunc
	wHub       *watchers.WatchersHub

	state     int
	stateCond *sync.Cond

	closed bool

	compactionMutex sync.Mutex
	rwmutex         sync.RWMutex

	metricsLastCommittedTrx prometheus.Gauge
	metricsLastIndexedTrx   prometheus.Gauge
}

type EntryMapper = func(key []byte, value []byte) ([]byte, error)

type runningState = int

const (
	running runningState = iota
	stopped
	paused
)

var (
	metricsLastIndexedTrxId = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_last_indexed_trx_id",
		Help: "The highest id of indexed transaction",
	}, []string{
		"db",
		"index",
	})
	metricsLastCommittedTrx = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_last_committed_trx_id",
		Help: "The highest id of committed transaction",
	}, []string{
		"db",
		"index",
	})
)

func newIndexer(path string, store *ImmuStore, opts *Options) (*indexer, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrIllegalArguments)
	}

	id := atomic.AddUint64(&store.nextIndexerID, 1)
	if id-1 > math.MaxUint16 {
		return nil, ErrMaxIndexersLimitExceeded
	}

	indexOpts := tbtree.DefaultOptions().
		WithIdentifier(uint16(id - 1)).
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithLogger(opts.logger).
		WithFileSize(opts.FileSize).
		WithCacheSize(opts.IndexOpts.CacheSize).
		WithCache(store.indexCache).
		WithFlushThld(opts.IndexOpts.FlushThld).
		WithSyncThld(opts.IndexOpts.SyncThld).
		WithFlushBufferSize(opts.IndexOpts.FlushBufferSize).
		WithCleanupPercentage(opts.IndexOpts.CleanupPercentage).
		WithMaxActiveSnapshots(opts.IndexOpts.MaxActiveSnapshots).
		WithMaxNodeSize(opts.IndexOpts.MaxNodeSize).
		WithMaxKeySize(opts.MaxKeyLen).
		WithMaxValueSize(lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen). // indexed values
		WithNodesLogMaxOpenedFiles(opts.IndexOpts.NodesLogMaxOpenedFiles).
		WithHistoryLogMaxOpenedFiles(opts.IndexOpts.HistoryLogMaxOpenedFiles).
		WithCommitLogMaxOpenedFiles(opts.IndexOpts.CommitLogMaxOpenedFiles).
		WithRenewSnapRootAfter(opts.IndexOpts.RenewSnapRootAfter).
		WithCompactionThld(opts.IndexOpts.CompactionThld).
		WithDelayDuringCompaction(opts.IndexOpts.DelayDuringCompaction).
		WithMaxBufferedDataSize(opts.IndexOpts.MaxBufferedDataSize).
		WithOnFlushFunc(func(releasedDataSize int) {
			store.memSemaphore.Release(uint64(releasedDataSize))
		})

	if opts.appFactory != nil {
		indexOpts.WithAppFactory(tbtree.AppFactoryFunc(opts.appFactory))
	}

	if opts.appRemove != nil {
		indexOpts.WithAppRemoveFunc(tbtree.AppRemoveFunc(opts.appRemove))
	}

	index, err := tbtree.Open(path, indexOpts)
	if err != nil {
		return nil, err
	}

	var wHub *watchers.WatchersHub
	if opts.MaxWaitees > 0 {
		wHub = watchers.New(0, opts.MaxWaitees)
	}

	tx := NewTx(opts.MaxTxEntries, opts.MaxKeyLen)

	kvs := make([]*tbtree.KVT, store.maxTxEntries*opts.IndexOpts.MaxBulkSize)
	for i := range kvs {
		// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmd
		elen := lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen
		kvs[i] = &tbtree.KVT{K: make([]byte, store.maxKeyLen), V: make([]byte, elen)}
	}

	indexer := &indexer{
		store:                  store,
		tx:                     tx,
		maxBulkSize:            opts.IndexOpts.MaxBulkSize,
		bulkPreparationTimeout: opts.IndexOpts.BulkPreparationTimeout,
		_kvs:                   kvs,
		path:                   path,
		index:                  index,
		wHub:                   wHub,
		state:                  stopped,
		stateCond:              sync.NewCond(&sync.Mutex{}),
	}

	dbName := filepath.Base(store.path)
	idxName := filepath.Base(path)
	indexer.metricsLastIndexedTrx = metricsLastIndexedTrxId.WithLabelValues(dbName, idxName)
	indexer.metricsLastCommittedTrx = metricsLastCommittedTrx.WithLabelValues(dbName, idxName)

	return indexer, nil
}

func (idx *indexer) init(spec *IndexSpec) {
	idx.rwmutex.Lock()
	defer idx.rwmutex.Unlock()

	idx.spec = spec

	idx.resume()
}

func (idx *indexer) SourcePrefix() []byte {
	return idx.spec.SourcePrefix
}

func (idx *indexer) TargetPrefix() []byte {
	return idx.spec.TargetPrefix
}

func (idx *indexer) Ts() uint64 {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	return idx.index.Ts()
}

func (idx *indexer) SyncSnapshot() (*tbtree.Snapshot, error) {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	return idx.index.SyncSnapshot()
}

func (idx *indexer) Get(key []byte) (value []byte, tx uint64, hc uint64, err error) {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	return idx.index.Get(key)
}

func (idx *indexer) GetBetween(key []byte, initialTxID uint64, finalTxID uint64) (value []byte, tx uint64, hc uint64, err error) {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	return idx.index.GetBetween(key, initialTxID, finalTxID)
}

func (idx *indexer) History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []tbtree.TimedValue, hCount uint64, err error) {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, 0, ErrAlreadyClosed
	}

	return idx.index.History(key, offset, descOrder, limit)
}

func (idx *indexer) Snapshot() (*tbtree.Snapshot, error) {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.Snapshot()
}

func (idx *indexer) SnapshotMustIncludeTxIDWithRenewalPeriod(ctx context.Context, txID uint64, renewalPeriod time.Duration) (*tbtree.Snapshot, error) {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.SnapshotMustIncludeTsWithRenewalPeriod(txID, renewalPeriod)
}

func (idx *indexer) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, tx uint64, hc uint64, err error) {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	return idx.index.GetWithPrefix(prefix, neq)
}

func (idx *indexer) Sync() error {
	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	return idx.index.Sync()
}

func (idx *indexer) Close() error {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.rwmutex.RLock()
	defer idx.rwmutex.RUnlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.stop()
	idx.wHub.Close()

	idx.closed = true

	return idx.index.Close()
}

func (idx *indexer) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	if idx.wHub != nil {
		err := idx.wHub.WaitFor(ctx, txID)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return ErrAlreadyClosed
		}
		return err
	}

	return watchers.ErrMaxWaitessLimitExceeded
}

func (idx *indexer) CompactIndex() (err error) {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	idx.store.logger.Infof("compacting index '%s'...", idx.store.path)

	defer func() {
		if err == nil {
			idx.store.logger.Infof("index '%s' sucessfully compacted", idx.store.path)
		} else if errors.Is(err, tbtree.ErrCompactionThresholdNotReached) {
			idx.store.logger.Infof("compaction of index '%s' not needed: %v", idx.store.path, err)
		} else {
			idx.store.logger.Warningf("%v: while compacting index '%s'", err, idx.store.path)
		}
	}()

	_, err = idx.index.Compact()
	if errors.Is(err, tbtree.ErrAlreadyClosed) {
		return ErrAlreadyClosed
	}
	if err != nil {
		return err
	}

	return idx.restartIndex()
}

func (idx *indexer) FlushIndex(cleanupPercentage float32, synced bool) (err error) {
	idx.compactionMutex.Lock()
	defer idx.compactionMutex.Unlock()

	_, _, err = idx.index.FlushWith(cleanupPercentage, synced)
	if errors.Is(err, tbtree.ErrAlreadyClosed) {
		return ErrAlreadyClosed
	}
	if err != nil {
		return err
	}

	return nil
}

func (idx *indexer) stop() {
	idx.stateCond.L.Lock()
	idx.state = stopped
	idx.cancelFunc()
	idx.stateCond.L.Unlock()
	idx.stateCond.Signal()

	idx.store.notify(Info, true, "indexing gracefully stopped at '%s'", idx.store.path)
}

func (idx *indexer) resume() {
	idx.stateCond.L.Lock()
	idx.state = running
	idx.ctx, idx.cancelFunc = context.WithCancel(context.Background())
	go idx.doIndexing()
	idx.stateCond.L.Unlock()

	idx.store.notify(Info, true, "indexing in progress at '%s'", idx.store.path)
}

func (idx *indexer) restartIndex() error {
	idx.rwmutex.Lock()
	defer idx.rwmutex.Unlock()

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

func (idx *indexer) doIndexing() {
	committedTxID := idx.store.LastCommittedTxID()
	idx.metricsLastCommittedTrx.Set(float64(committedTxID))

	for {
		lastIndexedTx := idx.index.Ts()
		idx.metricsLastIndexedTrx.Set(float64(lastIndexedTx))

		if idx.wHub != nil {
			idx.wHub.DoneUpto(lastIndexedTx)
		}

		err := idx.store.commitWHub.WaitFor(idx.ctx, lastIndexedTx+1)
		if idx.ctx.Err() != nil || errors.Is(err, watchers.ErrAlreadyClosed) {
			return
		}
		if err != nil {
			idx.store.logger.Errorf("indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}

		committedTxID := idx.store.LastCommittedTxID()
		idx.metricsLastCommittedTrx.Set(float64(committedTxID))

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

		err = idx.indexSince(lastIndexedTx + 1)
		if errors.Is(err, ErrAlreadyClosed) || errors.Is(err, tbtree.ErrAlreadyClosed) {
			return
		}

		if err != nil && !errors.Is(err, ErrWriteStalling) {
			idx.store.logger.Errorf("indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}

		if err := idx.handleWriteStalling(err); err != nil {
			idx.store.logger.Errorf("indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}
	}
}

func (idx *indexer) handleWriteStalling(err error) error {
	if !errors.Is(err, ErrWriteStalling) {
		return nil
	}

	if err := idx.store.FlushIndexes(0, false); err != nil {
		return err
	}
	// NOSONAR   (rand is fine here)
	sleepTime := writeStallingSleepDurationMin + time.Duration(rand.Intn(int(writeStallingSleepDurationMax-writeStallingSleepDurationMin+1)))
	time.Sleep(sleepTime)
	return nil
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

func (idx *indexer) mapKey(key []byte, vLen int, vOff int64, hVal [sha256.Size]byte, mapper EntryMapper) (mappedKey []byte, err error) {
	if mapper == nil {
		return key, nil
	}

	buf := idx.valBuffer(vLen)
	_, err = idx.store.readValueAt(buf, vOff, hVal, false)
	if err != nil {
		return nil, err
	}

	return mapper(key, buf)
}

func (idx *indexer) valBuffer(vLen int) []byte {
	if vLen > len(idx._val) {
		return make([]byte, vLen)
	}
	return idx._val[:vLen]
}

func (idx *indexer) indexSince(txID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), idx.bulkPreparationTimeout)
	defer cancel()

	acquiredMem := 0
	bulkSize := 0
	indexableEntries := 0

	for i := 0; i < idx.maxBulkSize; i++ {
		err := idx.store.readTx(txID+uint64(i), false, false, idx.tx)
		if err != nil {
			return err
		}

		txIndexedEntries := 0
		txEntries := idx.tx.Entries()

		var txmd []byte

		if idx.tx.header.Metadata != nil {
			txmd = idx.tx.header.Metadata.Bytes()
		}

		for _, e := range txEntries {
			if e.md != nil && e.md.NonIndexable() {
				continue
			}

			if !hasPrefix(e.key(), idx.spec.SourcePrefix) {
				continue
			}

			sourceKey, err := idx.mapKey(e.key(), e.vLen, e.vOff, e.hVal, idx.spec.SourceEntryMapper)
			if err != nil {
				return err
			}

			targetKey, err := idx.mapKey(sourceKey, e.vLen, e.vOff, e.hVal, idx.spec.TargetEntryMapper)
			if err != nil {
				return err
			}

			if !hasPrefix(targetKey, idx.spec.TargetPrefix) {
				return fmt.Errorf("%w: the target entry mapper has not generated a key with the specified target prefix", ErrIllegalArguments)
			}

			// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmds
			var b [lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen]byte

			var kvmd []byte

			if e.Metadata() != nil {
				kvmd = e.Metadata().Bytes()
			}

			n := serializeIndexableEntry(b[:], txmd, e, kvmd)

			idx._kvs[indexableEntries].K = targetKey
			idx._kvs[indexableEntries].V = b[:n]
			idx._kvs[indexableEntries].T = txID + uint64(i)

			indexableEntries++
			txIndexedEntries++

			if idx.spec.InjectiveMapping && txID > 1 {
				// wait for source indexer to be up to date
				sourceIndexer, err := idx.store.getIndexerFor(sourceKey)
				if errors.Is(err, ErrIndexNotFound) {
					continue
				} else if err != nil {
					return err
				}

				err = sourceIndexer.WaitForIndexingUpto(context.Background(), txID-1)
				if err != nil {
					return err
				}

				// the previous entry as of txID must be deleted from the target index
				_, prevTxID, _, err := sourceIndexer.index.GetBetween(sourceKey, 1, txID-1)
				if err == nil {
					prevEntry, prevTxHdr, err := idx.store.ReadTxEntry(prevTxID, e.key(), false)
					if err != nil {
						return err
					}

					targetPrevKey, err := idx.mapKey(sourceKey, prevEntry.vLen, prevEntry.vOff, prevEntry.hVal, idx.spec.TargetEntryMapper)
					if err != nil {
						return err
					}

					if bytes.Equal(targetKey, targetPrevKey) {
						continue
					}

					if !hasPrefix(targetPrevKey, idx.spec.TargetPrefix) {
						return fmt.Errorf("%w: the target entry mapper has not generated a key with the specified target prefix", ErrIllegalArguments)
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
					if err != nil {
						return err
					}

					var b [lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen]byte

					n := serializeIndexableEntry(b[:], txmd, prevEntry, kvmd.Bytes())

					idx._kvs[indexableEntries].K = targetPrevKey
					idx._kvs[indexableEntries].V = b[:n]
					idx._kvs[indexableEntries].T = txID + uint64(i)

					indexableEntries++
					txIndexedEntries++
				} else if !errors.Is(err, ErrKeyNotFound) {
					return err
				}
			}
		}

		if indexableEntries > 0 && txIndexedEntries > 0 {
			size := estimateEntriesSize(idx._kvs[indexableEntries-txIndexedEntries : indexableEntries])
			if !idx.store.memSemaphore.Acquire(uint64(size)) {
				if acquiredMem == 0 {
					return ErrWriteStalling
				}
				break
			}
			acquiredMem += size
		}

		bulkSize++

		if bulkSize < idx.maxBulkSize {
			// wait for the next tx to be committed
			err = idx.store.commitWHub.WaitFor(ctx, txID+uint64(i+1))
		}
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			return err
		}
	}

	var err error

	if indexableEntries == 0 {
		// if there are no entries to be indexed, the logical time in the tree
		// is still moved forward to indicate up to what point has transaction
		// indexing been completed
		err = idx.index.IncreaseTs(txID + uint64(bulkSize-1))
	} else {
		err = idx.index.BulkInsert(idx._kvs[:indexableEntries])
	}
	if err != nil {
		return err
	}

	idx.metricsLastIndexedTrx.Set(float64(txID + uint64(bulkSize-1)))

	return nil
}

func estimateEntriesSize(kvs []*tbtree.KVT) int {
	size := 0
	for _, kv := range kvs {
		size += len(kv.K) + len(kv.V) + 8
	}
	return size
}
