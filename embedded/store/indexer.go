/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type indexer struct {
	path string

	store *ImmuStore

	spec *IndexSpec

	tx *Tx

	maxBulkSize            int
	bulkPreparationTimeout time.Duration

	_kvs []*tbtree.KVT //pre-allocated for multi-tx bulk indexing
	_val []byte        //pre-allocated buffer to read entry values while mapping

	index *tbtree.TBtree

	ctx        context.Context
	cancelFunc context.CancelFunc
	wHub       *watchers.WatchersHub

	state     int
	stateCond *sync.Cond

	closed bool

	compactionMutex sync.Mutex
	mutex           sync.Mutex

	metricsLastCommittedTrx prometheus.Gauge
	metricsLastIndexedTrx   prometheus.Gauge
}

type EntryMapper = func(key []byte, value []byte) ([]byte, error)
type EntryUpdateProcessor = func(key []byte, prevValue []byte) error

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
	})
	metricsLastCommittedTrx = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "immudb_last_committed_trx_id",
		Help: "The highest id of committed transaction",
	}, []string{
		"db",
	})
)

func newIndexer(path string, store *ImmuStore, opts *Options) (*indexer, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil store", ErrIllegalArguments)
	}

	indexOpts := tbtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithLogger(opts.logger).
		WithFileSize(opts.FileSize).
		WithCacheSize(opts.IndexOpts.CacheSize).
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
		WithDelayDuringCompaction(opts.IndexOpts.DelayDuringCompaction)

	if opts.appFactory != nil {
		indexOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(store.path, filepath.Join(indexDirname, subPath), appOpts)
		})
	}

	index, err := tbtree.Open(path, indexOpts)
	if err != nil {
		return nil, err
	}

	var wHub *watchers.WatchersHub
	if opts.MaxWaitees > 0 {
		wHub = watchers.New(0, opts.MaxWaitees)
	}

	tx, err := store.fetchAllocTx()
	if err != nil {
		return nil, err
	}

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
		_val:                   make([]byte, store.maxValueLen),
		path:                   path,
		index:                  index,
		wHub:                   wHub,
		state:                  stopped,
		stateCond:              sync.NewCond(&sync.Mutex{}),
	}

	dbName := filepath.Base(store.path)
	indexer.metricsLastIndexedTrx = metricsLastIndexedTrxId.WithLabelValues(dbName)
	indexer.metricsLastCommittedTrx = metricsLastCommittedTrx.WithLabelValues(dbName)

	return indexer, nil
}

func (idx *indexer) init(spec *IndexSpec) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	idx.spec = spec

	idx.resume()
}

func (idx *indexer) Prefix() []byte {
	return idx.spec.TargetPrefix
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

func (idx *indexer) History(key []byte, offset uint64, descOrder bool, limit int) (txs []uint64, hCount uint64, err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, 0, ErrAlreadyClosed
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

func (idx *indexer) SnapshotMustIncludeTxIDWithRenewalPeriod(txID uint64, renewalPeriod time.Duration) (*tbtree.Snapshot, error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	return idx.index.SnapshotMustIncludeTsWithRenewalPeriod(txID, renewalPeriod)
}

func (idx *indexer) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, tx uint64, hc uint64, err error) {
	idx.mutex.Lock()
	defer idx.mutex.Unlock()

	if idx.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	return idx.index.GetWithPrefix(prefix, neq)
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

func (idx *indexer) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	if idx.wHub != nil {
		err := idx.wHub.WaitFor(ctx, txID)
		if err == watchers.ErrAlreadyClosed {
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
		} else if err == tbtree.ErrCompactionThresholdNotReached {
			idx.store.logger.Infof("compaction of index '%s' not needed: %v", idx.store.path, err)
		} else {
			idx.store.logger.Warningf("%v: while compacting index '%s'", err, idx.store.path)
		}
	}()

	_, err = idx.index.Compact()
	if err == tbtree.ErrAlreadyClosed {
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
	if err == tbtree.ErrAlreadyClosed {
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
		if err == ErrAlreadyClosed || err == tbtree.ErrAlreadyClosed {
			return
		}
		if err != nil {
			idx.store.logger.Errorf("indexing failed at '%s' due to error: %v", idx.store.path, err)
			time.Sleep(60 * time.Second)
		}
	}
}

func (idx *indexer) indexSince(txID uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), idx.bulkPreparationTimeout)
	defer cancel()

	bulkSize := 0
	indexableEntries := 0

	for i := 0; i < idx.maxBulkSize; i++ {
		err := idx.store.readTx(txID+uint64(i), false, false, idx.tx)
		if err != nil {
			return err
		}

		txEntries := idx.tx.Entries()

		var txmd []byte

		if idx.tx.header.Metadata != nil {
			txmd = idx.tx.header.Metadata.Bytes()
		}

		txmdLen := len(txmd)

		for _, e := range txEntries {
			if e.md != nil && e.md.NonIndexable() {
				continue
			}

			if !hasPrefix(e.key(), idx.spec.SourcePrefix) {
				continue
			}

			var mappedKey []byte

			if idx.spec.EntryMapper == nil {
				mappedKey = e.key()
			} else {
				_, err := idx.store.readValueAt(idx._val[:e.vLen], e.vOff, e.hVal, false)
				if err != nil {
					return err
				}

				mappedKey, err = idx.spec.EntryMapper(e.key(), idx._val[:e.vLen])
				if err != nil {
					return err
				}
			}

			if !hasPrefix(mappedKey, idx.spec.TargetPrefix) {
				continue
			}

			if idx.spec.EntryUpdateProcessor != nil {
				prevValRef, err := idx.store.Get(e.key())
				if err == nil {
					prevVal, err := prevValRef.Resolve()
					if err != nil {
						return err
					}

					err = idx.spec.EntryUpdateProcessor(e.key(), prevVal)
					if err != nil {
						return err
					}
				} else if !errors.Is(err, ErrKeyNotFound) {
					return err
				}
			}

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

			idx._kvs[indexableEntries].K = mappedKey
			idx._kvs[indexableEntries].V = b[:o]
			idx._kvs[indexableEntries].T = txID + uint64(i)

			indexableEntries++
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
