package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/metrics"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
)

const maxWaitingDefault = 100

var ErrIndexLimitExceeded = errors.New("maximum allowed number of indexes exceeded")

type IndexableLedger interface {
	ID() LedgerID
	Path() string
	LastCommittedTxID() uint64
	ValueReaderAt(vlen int, off int64, hvalue [sha256.Size]byte, skipIntegrityCheck bool) (io.Reader, error)
	ReadTxAt(txID uint64, tx *Tx) error
	Options() *Options
}

type IndexerManager struct {
	mtx    sync.RWMutex
	logger logger.Logger

	pgBuf *tbtree.PageCache

	indexes map[LedgerID][]*index

	indexers    []Indexer
	nextIndexID atomic.Uint32

	indexingWHub *watchers.WatchersHub

	closed bool
}

func NewIndexerManager(opts *Options) (*IndexerManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	indexingWHub := watchers.New(0, opts.MaxWaitees)

	indexers, err := createIndexers(opts, indexingWHub)
	if err != nil {
		return nil, err
	}

	return &IndexerManager{
		logger: opts.logger,
		pgBuf: tbtree.NewPageCache(
			opts.IndexOpts.PageBufferSize,
			metrics.NewPrometheusPageCacheMetrics(),
		),
		indexers:     indexers,
		indexes:      make(map[LedgerID][]*index),
		indexingWHub: indexingWHub,
	}, nil
}

func (m *IndexerManager) ForEachIndex(ledgerID LedgerID, onIndex func(index *index) error) error {
	m.mtx.RLock()

	if m.closed {
		m.mtx.RUnlock()
		return ErrAlreadyClosed
	}
	m.mtx.RUnlock()

	indexes := m.indexes[ledgerID]
	for _, idx := range indexes {
		if err := onIndex(idx); err != nil {
			return err
		}
	}
	return nil
}

func (m *IndexerManager) PauseIndexing() error {
	return nil
}

func (m *IndexerManager) ResumeIndexing() error {
	return nil
}

func (m *IndexerManager) CompactIndexes(id LedgerID, force bool) error {
	indexes, err := m.ledgerIndexes(id)
	if err != nil {
		return err
	}

	for _, idx := range indexes {
		err := idx.Compact(context.Background(), force)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *IndexerManager) Flush() error {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if m.closed {
		return ErrAlreadyClosed
	}

	for i := range m.indexers {
		err := m.indexers[i].flushIndexes()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *IndexerManager) WaitForIndexingUpTo(ctx context.Context, ledgerID LedgerID, txID uint64) error {
	return m.ForEachIndex(ledgerID, func(index *index) error {
		err := index.WaitForIndexingUpTo(ctx, txID)
		if errors.Is(err, ErrAlreadyClosed) {
			return nil
		}
		return err
	})
}

func createIndexers(opts *Options, indexingWHub *watchers.WatchersHub) ([]Indexer, error) {
	swb := tbtree.NewSharedWriteBuffer(
		opts.IndexOpts.SharedWriteBufferSize,
		opts.IndexOpts.WriteBufferChunkSize,
	)

	indexers := make([]Indexer, opts.IndexOpts.NumIndexers)
	for i := range indexers {
		wb, err := tbtree.NewWriteBuffer(
			swb,
			opts.IndexOpts.MinWriteBufferSize,
			opts.IndexOpts.MaxWriteBufferSize,
			metrics.NewPrometheusWriteBufferMetrics(i),
		)
		if err != nil {
			return nil, err
		}

		indexers[i] = NewIndexer(opts, wb, indexingWHub)
	}
	return indexers, nil
}

func (m *IndexerManager) Start() {
	for i := range m.indexers {
		idx := &m.indexers[i]
		idx.Start()
	}
}

func (m *IndexerManager) InitIndexing(ledger IndexableLedger, spec IndexSpec) (*index, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.closed {
		return nil, ErrAlreadyClosed
	}

	if len(spec.TargetPrefix) == 0 && len(spec.SourcePrefix) > 0 {
		return nil, fmt.Errorf("%w: empty prefix can not have a source prefix", ErrIllegalArguments)
	}

	for _, idx := range m.indexes[ledger.ID()] {
		if bytes.Equal(idx.TargetPrefix(), spec.TargetPrefix) {
			return nil, ErrIndexAlreadyInitialized
		}
	}

	indexPath := spec.Path(ledger.Path())
	nextIndexID := m.nextIndexID.Add(1) - 1
	indexerID := int(nextIndexID) % len(m.indexers)
	var srcIndex *index

	if spec.InjectiveMapping {
		var err error
		indexes := m.indexes[ledger.ID()]
		srcIndex, err = getIndexerFor(spec.SourcePrefix, indexes)
		if err != nil {
			return nil, err
		}
		indexerID = srcIndex.indexerID
	}

	if nextIndexID > math.MaxUint16 {
		return nil, ErrIndexLimitExceeded
	}

	indexer := &m.indexers[indexerID]
	index, err := indexer.newIndex(
		uint16(nextIndexID),
		indexerID,
		srcIndex,
		indexPath,
		ledger,
		spec,
		m.pgBuf,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: could not open indexer", err)
	}

	if index.Ts() > ledger.LastCommittedTxID() {
		return nil, fmt.Errorf("%w: index size is too large", ErrCorruptedIndex)

		// TODO: if indexing is done on pre-committed txs, the index may be rollback to a previous snapshot where it was already synced
		// NOTE: compaction should preserve snapshot which are not synced... so to ensure rollback can be achieved
	}

	m.indexes[ledger.ID()] = append(m.indexes[ledger.ID()], index)
	return index, nil
}

func (m *IndexerManager) ledgerIndexes(ledgerID LedgerID) ([]*index, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if m.closed {
		return nil, ErrAlreadyClosed
	}
	return m.indexes[ledgerID], nil
}

func (m *IndexerManager) CloseLedgerIndexing(ledgerID LedgerID) error {
	indexes, err := m.ledgerIndexes(ledgerID)
	if err != nil {
		return err
	}

	merr := multierr.NewMultiErr()
	for _, idx := range indexes {
		if idx.ledger.ID() != ledgerID {
			continue
		}

		err := idx.Close()
		if err != nil && !errors.Is(err, ErrAlreadyClosed) {
			merr.Append(err)
		}
	}
	return merr.Reduce()
}

func (m *IndexerManager) CloseIndexing(ledgerID LedgerID, prefix []byte) (*index, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	if m.closed {
		return nil, ErrAlreadyClosed
	}
	return m.closeIndexing(ledgerID, prefix)
}

func (m *IndexerManager) closeIndexing(ledgerID LedgerID, prefix []byte) (*index, error) {
	for _, idx := range m.indexes[ledgerID] {
		if bytes.HasPrefix(prefix, idx.spec.TargetPrefix) {
			err := idx.Close()
			return idx, err
		}
	}
	return nil, ErrIndexNotFound
}

func (m *IndexerManager) DeleteIndexing(ledgerID LedgerID, prefix []byte) (*index, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.closed {
		return nil, ErrAlreadyClosed
	}

	index, err := m.closeIndexing(ledgerID, prefix)
	if errors.Is(err, ErrAlreadyClosed) {
		return nil, ErrIndexNotFound
	}
	if err != nil {
		return nil, err
	}

	m.logger.Infof("deleting index path: '%s' ...", index.path)

	if err := os.RemoveAll(index.path); err != nil {
		return nil, err
	}
	m.indexes[ledgerID] = removeIndex(m.indexes[ledgerID], prefix)

	return index, nil
}

func removeIndex(indexes []*index, prefix []byte) []*index {
	for i, idx := range indexes {
		if bytes.Equal(idx.TargetPrefix(), prefix) {
			indexes[i] = indexes[0]
			return indexes[1:]
		}
	}
	return indexes
}

func (m *IndexerManager) Close() error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.closed {
		return ErrAlreadyClosed
	}

	merr := multierr.NewMultiErr()
	for _, indexes := range m.indexes {
		for _, idx := range indexes {
			merr.Append(idx.Close())
		}
	}

	m.indexingWHub.Close()

	for i := range m.indexers {
		_ = m.indexers[i].Close()
	}
	m.closed = true
	return nil
}

func (m *IndexerManager) GetIndexFor(ledgerID LedgerID, key []byte) (*index, error) {
	indexes, err := m.ledgerIndexes(ledgerID)
	if err != nil {
		return nil, err
	}

	for _, idx := range indexes {
		if !idx.Closed() && bytes.HasPrefix(key, idx.spec.TargetPrefix) {
			return idx, nil
		}
	}
	return nil, ErrIndexNotFound
}

func getIndexerFor(key []byte, indexes []*index) (*index, error) {
	for _, idx := range indexes {
		if !idx.Closed() && bytes.HasPrefix(key, idx.spec.TargetPrefix) {
			return idx, nil
		}
	}
	return nil, ErrIndexNotFound
}

func (indexer *Indexer) newIndex(
	id uint16,
	indexerID int,
	srcIdx *index,
	path string,
	ledger IndexableLedger,
	spec IndexSpec,
	pgBuf *tbtree.PageCache,
) (*index, error) {
	opts := ledger.Options()

	treeOpts := tbtree.DefaultOptions().
		WithTreeID(tbtree.TreeID(id)).
		WithWriteBuffer(indexer.wb).
		WithPageBuffer(pgBuf).
		WithMaxActiveSnapshots(opts.IndexOpts.MaxActiveSnapshots).
		WithLogger(indexer.logger)

	if opts.appFactory != nil {
		treeOpts = treeOpts.WithAppFactory(tbtree.AppFactoryFunc(opts.appFactory))
	}

	if opts.appRemove != nil {
		treeOpts = treeOpts.WithAppRemove(tbtree.AppRemoveFunc(opts.appRemove))
	}

	if opts.readDir != nil {
		treeOpts = treeOpts.WithReadDirFunc(tbtree.ReadDirFunc(opts.readDir))
	}

	tree, err := tbtree.Open(path, treeOpts)
	if err != nil {
		return nil, err
	}

	wHub := watchers.New(0, opts.MaxWaitees)
	if err := wHub.DoneUpto(tree.Ts()); err != nil {
		return nil, err
	}

	idx := &index{
		path:      path,
		indexerID: indexerID,
		ledger:    ledger,
		srcIdx:    srcIdx,
		spec:      spec,
		tree:      tree,
		wHub:      wHub,
	}

	indexer.pushIndex(idx, true)

	return idx, nil
}

func (m *IndexerManager) NotifyTransactions(n uint64) {
	m.indexingWHub.Inc(n)
}
