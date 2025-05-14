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
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/v2/embedded/ahtree"
	"github.com/codenotary/immudb/v2/embedded/appendable"
	"github.com/codenotary/immudb/v2/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/v2/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/v2/embedded/cache"
	"github.com/codenotary/immudb/v2/embedded/htree"
	"github.com/codenotary/immudb/v2/embedded/logger"
	"github.com/codenotary/immudb/v2/embedded/multierr"
	"github.com/codenotary/immudb/v2/embedded/tbtree"
	"github.com/codenotary/immudb/v2/embedded/watchers"
	"github.com/codenotary/immudb/v2/pkg/helpers/slices"
)

type LedgerID uint16

type Ledger struct {
	path string

	store *ImmuStore
	id    LedgerID

	logger           logger.Logger
	lastNotification time.Time
	notifyMutex      sync.Mutex

	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	vLogCache *cache.Cache

	txLog      appendable.Appendable
	txLogCache *cache.Cache

	cLog          appendable.Appendable
	cLogEntrySize int

	cLogBuf *precommitBuffer

	committedTxID uint64
	committedAlh  [sha256.Size]byte

	inmemPrecommittedTxID uint64
	inmemPrecommittedAlh  [sha256.Size]byte

	precommittedTxLogSize int64

	mandatoryMVCCUpToTxID uint64

	commitStateRWMutex sync.RWMutex

	embeddedValues        bool
	preallocFiles         bool
	readOnly              bool
	synced                bool
	syncFrequency         time.Duration
	maxActiveTransactions int
	mvccReadSetLimit      int
	maxWaitees            int
	maxConcurrency        int
	maxIOConcurrency      int
	maxTxEntries          int
	maxKeyLen             int
	maxValueLen           int

	writeTxHeaderVersion int

	timeFunc      TimeFunc
	multiIndexing bool

	useExternalCommitAllowance bool
	commitAllowedUpToTxID      uint64

	txPool TxPool

	waiteesMutex sync.Mutex
	waiteesCount int // current number of go-routines waiting for a tx to be indexed or committed

	_txbs     []byte                   // pre-allocated buffer to support tx serialization
	_valBs    [DefaultMaxValueLen]byte // pre-allocated buffer to support tx exportation
	_valBsMux sync.Mutex

	aht                  *ahtree.AHtree
	inmemPrecommitWHub   *watchers.WatchersHub
	durablePrecommitWHub *watchers.WatchersHub
	commitWHub           *watchers.WatchersHub

	opts   *Options
	closed bool

	mutex sync.Mutex

	compactionDisabled bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

func openLedger(
	name string,
	store *ImmuStore,
	opts *Options,
) (*Ledger, error) {
	path := filepath.Join(store.path, name)

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		err := os.MkdirAll(path, opts.FileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaVersion, Version)
	metadata.PutBool(metaEmbeddedValues, opts.EmbeddedValues)
	metadata.PutBool(metaPreallocFiles, opts.PreallocFiles)
	metadata.PutInt(metaMaxTxEntries, opts.MaxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.MaxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.MaxValueLen)
	metadata.PutInt(metaFileSize, opts.FileSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithWriteBufferSize(opts.WriteBufferSize).
		WithRetryableSync(opts.Synced).
		WithAutoSync(true).
		WithFileSize(opts.FileSize).
		WithFileMode(opts.FileMode).
		WithMetadata(metadata.Bytes())

	appFactory := opts.appFactory
	if appFactory == nil {
		appFactory = func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			path := filepath.Join(rootPath, subPath)
			return multiapp.Open(path, opts)
		}
	}

	appendableOpts.WithFileExt("tx")
	appendableOpts.WithPrealloc(opts.PreallocFiles)
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.TxLogMaxOpenedFiles)
	txLog, err := appFactory(path, "tx", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open transaction log: %w", err)
	}

	metadata = appendable.NewMetadata(txLog.Metadata())

	embeddedValues, ok := metadata.GetBool(metaEmbeddedValues)
	embeddedValues = ok && embeddedValues

	preallocFiles, ok := metadata.GetBool(metaPreallocFiles)
	preallocFiles = ok && preallocFiles

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithFileSize(opts.FileSize)
	appendableOpts.WithPrealloc(preallocFiles)
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.CommitLogMaxOpenedFiles)
	cLog, err := appFactory(path, "commit", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open commit-log: %w", err)
	}

	var vLogs []appendable.Appendable

	if !embeddedValues {
		vLogs = make([]appendable.Appendable, opts.MaxIOConcurrency)
		appendableOpts.WithFileExt("val")
		appendableOpts.WithFileSize(opts.FileSize)
		appendableOpts.WithPrealloc(false)
		appendableOpts.WithCompressionFormat(opts.CompressionFormat)
		appendableOpts.WithCompresionLevel(opts.CompressionLevel)
		appendableOpts.WithMaxOpenedFiles(opts.VLogMaxOpenedFiles)

		for i := 0; i < opts.MaxIOConcurrency; i++ {
			vLog, err := appFactory(path, fmt.Sprintf("val_%d", i), appendableOpts)
			if err != nil {
				return nil, err
			}
			vLogs[i] = vLog
		}
	}
	return openLedgerWith(path, vLogs, txLog, cLog, store, opts)
}

func openLedgerWith(path string, vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, store *ImmuStore, opts *Options) (*Ledger, error) {
	if txLog == nil || cLog == nil {
		return nil, fmt.Errorf("%w: invalid txLog or cLog", ErrIllegalArguments)
	}

	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrIllegalArguments, err)
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	version, ok := metadata.GetInt(metaVersion)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "Version")
	}

	var cLogEntrySize int

	if version <= 1 {
		cLogEntrySize = cLogEntrySizeV1
	} else {
		cLogEntrySize = cLogEntrySizeV2
	}

	embeddedValues, ok := metadata.GetBool(metaEmbeddedValues)
	if !ok {
		if version >= 2 {
			return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "EmbeddedValues")
		}

		embeddedValues = false
	}

	preallocFiles, ok := metadata.GetBool(metaPreallocFiles)
	if !ok {
		if version >= 2 {
			return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "PreallocFiles")
		}

		preallocFiles = false
	}

	if (len(vLogs) == 0 && !embeddedValues) || (len(vLogs) != 0 && embeddedValues) {
		return nil, fmt.Errorf("%w: invalid vLogs", ErrIllegalArguments)
	}

	fileSize, ok := metadata.GetInt(metaFileSize)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "FileSize")
	}

	maxTxEntries, ok := metadata.GetInt(metaMaxTxEntries)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxTxEntries")
	}

	maxKeyLen, ok := metadata.GetInt(metaMaxKeyLen)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxKeyLen")
	}

	maxValueLen, ok := metadata.GetInt(metaMaxValueLen)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxValueLen")
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, fmt.Errorf("corrupted commit-log: could not get size: %w", err)
	}

	if !preallocFiles {
		rem := cLogSize % int64(cLogEntrySize)
		if rem > 0 {
			cLogSize -= rem
			err = cLog.SetOffset(cLogSize)
			if err != nil {
				return nil, fmt.Errorf("corrupted commit log: could not set offset: %w", err)
			}
		}
	}

	if preallocFiles {
		if cLogSize == 0 {
			return nil, fmt.Errorf("corrupted commit log: file should not be empty when file preallocation is enabled")
		}

		// find the last non-zeroed clogEntry
		left := int64(1)
		right := cLogSize / int64(cLogEntrySize)

		b := make([]byte, cLogEntrySize)
		zeroed := make([]byte, cLogEntrySize)

		for left < right {
			middle := left + ((right-left)+1)/2

			_, err := cLog.ReadAt(b, (middle-1)*int64(cLogEntrySize))
			if err != nil {
				return nil, fmt.Errorf("corrupted commit log: could not read the last commit: %w", err)
			}

			if bytes.Equal(b, zeroed) {
				// if cLogEntry is zeroed it's considered as preallocated
				right = middle - 1
			} else {
				left = middle
			}
		}

		_, err := cLog.ReadAt(b, (left-1)*int64(cLogEntrySize))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("corrupted commit log: could not read the last commit: %w", err)
		}

		if bytes.Equal(b, zeroed) {
			cLogSize = 0
		} else {
			cLogSize = left * int64(cLogEntrySize)
		}
	}

	var committedTxLogSize int64
	var committedTxOffset int64
	var committedTxSize int

	var committedTxID uint64

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)

		_, err := cLog.ReadAt(b[:], cLogSize-int64(cLogEntrySize))
		if err != nil {
			return nil, fmt.Errorf("corrupted commit-log: could not read the last commit: %w", err)
		}

		committedTxOffset = int64(binary.BigEndian.Uint64(b[:]))
		committedTxSize = int(binary.BigEndian.Uint32(b[txIDSize:]))
		committedTxLogSize = committedTxOffset + int64(committedTxSize)
		committedTxID = uint64(cLogSize) / uint64(cLogEntrySize)

		if cLogEntrySize == cLogEntrySizeV2 {
			copy(committedAlh[:], b[txIDSize+lszSize:])
		}

		txLogFileSize, err := txLog.Size()
		if err != nil {
			return nil, fmt.Errorf("corrupted transaction log: could not get size: %w", err)
		}

		if txLogFileSize < committedTxLogSize {
			return nil, fmt.Errorf("corrupted transaction log: size is too small: %w", ErrCorruptedTxData)
		}
	}

	txPool, err := newTxPool(txPoolOptions{
		poolSize:     opts.MaxConcurrency,
		maxTxEntries: maxTxEntries,
		maxKeyLen:    maxKeyLen,
		preallocated: true,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid configuration, couldn't initialize transaction holder pool")
	}

	maxTxSize := maxTxSize(maxTxEntries, maxKeyLen, maxTxMetadataLen, maxKVMetadataLen)
	txbs := make([]byte, maxTxSize)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx, _ := txPool.Alloc()

		err = tx.readFrom(txReader, false)
		if err != nil {
			txPool.Release(tx)
			return nil, fmt.Errorf("corrupted transaction log: could not read the last transaction: %w", err)
		}

		txPool.Release(tx)

		if cLogEntrySize == cLogEntrySizeV1 {
			committedAlh = tx.header.Alh()
		}

		if cLogEntrySize == cLogEntrySizeV2 {
			if committedAlh != tx.header.Alh() {
				return nil, fmt.Errorf("corrupted transaction log: digest mismatch in the last transaction: %w", err)
			}
		}
	}

	cLogBuf := newPrecommitBuffer(opts.MaxActiveTransactions)

	precommittedTxID := committedTxID
	precommittedAlh := committedAlh
	precommittedTxLogSize := committedTxLogSize

	if !opts.DiscardPrecommittedTransactions {
		// read pre-committed txs from txLog and insert into cLogBuf to continue with the commit process
		// txLog may be partially written, precommitted transactions loading is terminated if an inconsistency is found
		txReader := appendable.NewReaderFrom(txLog, precommittedTxLogSize, multiapp.DefaultReadBufferSize)

		tx, _ := txPool.Alloc()

		for {
			err = tx.readFrom(txReader, false)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				opts.logger.Infof("%v: discarding pre-committed transaction: %d", err, precommittedTxID+1)
				break
			}

			if tx.header.ID != precommittedTxID+1 || tx.header.PrevAlh != precommittedAlh {
				opts.logger.Infof("%v: discarding pre-committed transaction: %d", ErrCorruptedData, precommittedTxID+1)
				break
			}

			precommittedTxID++
			precommittedAlh = tx.header.Alh()

			txSize := int(txReader.ReadCount() - (precommittedTxLogSize - committedTxLogSize))

			err = cLogBuf.put(precommittedTxID, precommittedAlh, precommittedTxLogSize, txSize)
			if err != nil {
				txPool.Release(tx)
				return nil, fmt.Errorf("%v: while loading pre-committed transaction: %v", err, precommittedTxID+1)
			}

			precommittedTxLogSize += int64(txSize)
		}

		txPool.Release(tx)
	}

	vLogsMap := make(map[byte]*refVLog, len(vLogs))
	vLogUnlockedList := list.New()

	for i, vLog := range vLogs {
		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
	}

	var vLogCache *cache.Cache

	if opts.VLogCacheSize > 0 {
		vLogCache, err = cache.NewCache(opts.VLogCacheSize)
		if err != nil {
			return nil, err
		}
	}

	ahtPath := filepath.Join(path, ahtDirname)

	ahtOpts := ahtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithFileSize(fileSize).
		WithRetryableSync(opts.Synced).
		WithAutoSync(true).
		WithWriteBufferSize(opts.AHTOpts.WriteBufferSize).
		WithSyncThld(opts.AHTOpts.SyncThld)

	if opts.appFactory != nil {
		ahtOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(path, filepath.Join(ahtDirname, subPath), appOpts)
		})
	}

	aht, err := ahtree.Open(ahtPath, ahtOpts)
	if err != nil {
		return nil, fmt.Errorf("could not open aht: %w", err)
	}

	// TODO: optionally it could include up to opts.MaxActiveTransactions upon start
	txLogCache, err := cache.NewCache(opts.TxLogCacheSize)
	if err != nil {
		return nil, err
	}

	ledgerID, err := store.getNextLedgerID()
	if err != nil {
		return nil, err
	}

	ledger := &Ledger{
		id:               ledgerID,
		path:             path,
		store:            store,
		logger:           opts.logger,
		txLog:            txLog,
		txLogCache:       txLogCache,
		vLogs:            vLogsMap,
		vLogUnlockedList: vLogUnlockedList,
		vLogsCond:        sync.NewCond(&sync.Mutex{}),
		vLogCache:        vLogCache,

		cLog:          cLog,
		cLogEntrySize: cLogEntrySize,

		cLogBuf: cLogBuf,

		committedTxID: committedTxID,
		committedAlh:  committedAlh,

		inmemPrecommittedTxID: precommittedTxID,
		inmemPrecommittedAlh:  precommittedAlh,
		precommittedTxLogSize: precommittedTxLogSize,

		embeddedValues: embeddedValues,
		preallocFiles:  preallocFiles,

		readOnly:              opts.ReadOnly,
		synced:                opts.Synced,
		syncFrequency:         opts.SyncFrequency,
		maxActiveTransactions: opts.MaxActiveTransactions,
		mvccReadSetLimit:      opts.MVCCReadSetLimit,
		maxWaitees:            opts.MaxWaitees,
		maxConcurrency:        opts.MaxConcurrency,
		maxIOConcurrency:      opts.MaxIOConcurrency,

		maxTxEntries: maxTxEntries,
		maxKeyLen:    maxKeyLen,
		maxValueLen:  maxValueLen,

		writeTxHeaderVersion: opts.WriteTxHeaderVersion,

		timeFunc:                   opts.TimeFunc,
		multiIndexing:              opts.MultiIndexing,
		useExternalCommitAllowance: opts.UseExternalCommitAllowance,
		commitAllowedUpToTxID:      committedTxID,

		aht: aht,

		inmemPrecommitWHub:   watchers.New(0, opts.MaxActiveTransactions+1), // syncer (TODO: indexer may wait here instead)
		durablePrecommitWHub: watchers.New(0, opts.MaxActiveTransactions+opts.MaxWaitees),
		commitWHub:           watchers.New(0, 1+opts.MaxActiveTransactions+opts.MaxWaitees), // including indexer

		txPool: txPool,
		_txbs:  txbs,

		opts: opts,

		compactionDisabled: opts.CompactionDisabled,
	}

	if ledger.aht.Size() > precommittedTxID {
		err = ledger.aht.ResetSize(precommittedTxID)
		if err != nil {
			ledger.Close()
			return nil, fmt.Errorf("corrupted commit-log: can not truncate aht tree: %w", err)
		}
	}

	if ledger.aht.Size() == precommittedTxID {
		ledger.logger.Infof("binary-linking up to date at '%s'", ledger.path)
	} else {
		err = ledger.syncBinaryLinking()
		if err != nil {
			ledger.Close()
			return nil, fmt.Errorf("binary-linking syncing failed: %w", err)
		}
	}

	err = ledger.inmemPrecommitWHub.DoneUpto(precommittedTxID)
	if err != nil {
		ledger.Close()
		return nil, err
	}

	err = ledger.durablePrecommitWHub.DoneUpto(precommittedTxID)
	if err != nil {
		ledger.Close()
		return nil, err
	}

	err = ledger.doneCommitUpTo(committedTxID, int(committedTxID))
	if err != nil {
		ledger.Close()
		return nil, err
	}

	if !ledger.multiIndexing {
		err := ledger.InitIndexing(IndexSpec{})
		if err != nil {
			ledger.Close()
			return nil, err
		}
	}

	if ledger.synced {
		go ledger.syncer()
	}
	return ledger, nil
}

func (l *Ledger) syncer() {
	for {
		committedTxID := l.LastCommittedTxID()

		// passive wait for one new transaction at least
		err := l.inmemPrecommitWHub.WaitFor(context.Background(), committedTxID+1)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return
		}

		// TODO: waiting on earlier stages of transaction processing may also be possible
		prevLatestPrecommitedTx := committedTxID + 1

		// TODO: parametrize concurrency evaluation
		for i := 0; i < 4; i++ {
			// give some time for more transactions to be precommitted
			time.Sleep(l.syncFrequency / 4)

			latestPrecommitedTx := l.LastPrecommittedTxID()

			if prevLatestPrecommitedTx == latestPrecommitedTx {
				// avoid waiting if there are no new transactions
				break
			}

			prevLatestPrecommitedTx = latestPrecommitedTx
		}

		// ensure durability
		err = l.sync()
		if errors.Is(err, ErrAlreadyClosed) ||
			errors.Is(err, multiapp.ErrAlreadyClosed) ||
			errors.Is(err, singleapp.ErrAlreadyClosed) ||
			errors.Is(err, watchers.ErrAlreadyClosed) {
			return
		}
		if err != nil {
			l.notify(Error, true, "%s: while syncing transactions", err)
		}
	}
}

type NotificationType = int

const NotificationWindow = 60 * time.Second
const (
	Info NotificationType = iota
	Warn
	Error
)

func (l *Ledger) notify(nType NotificationType, mandatory bool, formattedMessage string, args ...interface{}) {
	l.notifyMutex.Lock()
	defer l.notifyMutex.Unlock()

	if mandatory || time.Since(l.lastNotification) > NotificationWindow {
		switch nType {
		case Info:
			{
				l.logger.Infof(formattedMessage, args...)
			}
		case Warn:
			{
				l.logger.Warningf(formattedMessage, args...)
			}
		case Error:
			{
				l.logger.Errorf(formattedMessage, args...)
			}
		}
		l.lastNotification = time.Now()
	}
}

func hasPrefix(key, prefix []byte) bool {
	return len(key) >= len(prefix) && bytes.Equal(prefix, key[:len(prefix)])
}

func (l *Ledger) getIndexFor(key []byte) (*index, error) {
	return l.store.indexerManager.GetIndexFor(l.ID(), key)
}

func (l *Ledger) InitIndexing(spec IndexSpec) error {
	_, err := l.store.indexerManager.InitIndexing(l, spec)
	return err
}

func (l *Ledger) CloseIndexing(prefix []byte) error {
	_, err := l.store.indexerManager.CloseIndexing(l.ID(), prefix)
	return err
}

func (l *Ledger) DeleteIndex(prefix []byte) error {
	_, err := l.store.indexerManager.DeleteIndexing(l.ID(), prefix)
	return err
}

func (l *Ledger) GetBetween(ctx context.Context, key []byte, initialTxID uint64, finalTxID uint64) (ValueRef, error) {
	indexer, err := l.getIndexFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	indexedVal, tx, hc, err := indexer.GetBetween(key, initialTxID, finalTxID)
	if err != nil {
		return nil, err
	}
	return l.valueRefFrom(tx, hc, indexedVal)
}

func (l *Ledger) Get(ctx context.Context, key []byte) (valRef ValueRef, err error) {
	return l.GetWithFilters(ctx, key, IgnoreExpired, IgnoreDeleted)
}

func (l *Ledger) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
	index, err := l.getIndexFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	indexedVal, tx, hc, err := index.Get(key)
	if err != nil {
		return nil, err
	}

	valRef, err = l.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	for _, filter := range filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		if err := filter(valRef, now); err != nil {
			return nil, err
		}
	}
	return valRef, nil
}

func (l *Ledger) GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error) {
	return l.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreExpired, IgnoreDeleted)
}

func (l *Ledger) GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	indexer, err := l.getIndexFor(prefix)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, nil, ErrKeyNotFound
		}

		return nil, nil, err
	}

	key, indexedVal, tx, hc, err := indexer.GetWithPrefix(prefix, neq)
	if err != nil {
		return nil, nil, err
	}

	valRef, err = l.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()

	for _, filter := range filters {
		if filter == nil {
			return nil, nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, now)
		if err != nil {
			return nil, nil, err
		}
	}

	return key, valRef, nil
}

func (l *Ledger) History(key []byte, offset uint64, descOrder bool, limit int) (valRefs []ValueRef, hCount uint64, err error) {
	index, err := l.getIndexFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, 0, ErrKeyNotFound
		}
		return nil, 0, err
	}

	timedValues, hCount, err := index.History(key, offset, descOrder, limit)
	if err != nil {
		return nil, 0, err
	}

	valRefs = make([]ValueRef, len(timedValues))

	rev := offset + 1
	if descOrder {
		rev = hCount - offset
	}

	for i, timedValue := range timedValues {
		val, err := l.valueRefFrom(timedValue.Ts, rev, timedValue.Value)
		if err != nil {
			return nil, 0, err
		}

		valRefs[i] = val

		if descOrder {
			rev--
		} else {
			rev++
		}
	}

	return valRefs, hCount, nil
}

func (l *Ledger) MultiIndexingEnabled() bool {
	return l.multiIndexing
}

func (l *Ledger) UseTimeFunc(timeFunc TimeFunc) error {
	if timeFunc == nil {
		return ErrIllegalArguments
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.timeFunc = timeFunc

	return nil
}

func (l *Ledger) NewTxHolderPool(poolSize int, preallocated bool) (TxPool, error) {
	return newTxPool(txPoolOptions{
		poolSize:     poolSize,
		maxTxEntries: l.maxTxEntries,
		maxKeyLen:    l.maxKeyLen,
		preallocated: preallocated,
	})
}

func (l *Ledger) writeSnapshot(prefix []byte) (*Snapshot, error) {
	index, err := l.getIndexFor(prefix)
	if err != nil {
		return nil, err
	}

	snap, err := index.WriteSnapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     l,
		prefix: prefix,
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

func (l *Ledger) Snapshot(prefix []byte) (*Snapshot, error) {
	indexer, err := l.getIndexFor(prefix)
	if err != nil {
		return nil, err
	}

	snap, err := indexer.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     l,
		prefix: prefix,
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

// SnapshotMustIncludeTxID returns a new snapshot based on an existent dumped root (snapshot reuse).
// Current root may be dumped if there are no previous root already stored on disk or if the dumped one was old enough.
// If txID is 0, any snapshot may be used.
func (l *Ledger) SnapshotMustIncludeTxID(ctx context.Context, prefix []byte, txID uint64) (*Snapshot, error) {
	return l.SnapshotMustIncludeTxIDWithRenewalPeriod(ctx, prefix, txID, 0)
}

// SnapshotMustIncludeTxIDWithRenewalPeriod returns a new snapshot based on an existent dumped root (snapshot reuse).
// Current root may be dumped if there are no previous root already stored on disk or if the dumped one was old enough.
// If txID is 0, any snapshot not older than renewalPeriod may be used.
// If renewalPeriod is 0, renewal period is not taken into consideration
func (l *Ledger) SnapshotMustIncludeTxIDWithRenewalPeriod(ctx context.Context, prefix []byte, txID uint64, renewalPeriod time.Duration) (*Snapshot, error) {
	index, err := l.getIndexFor(prefix)
	if err != nil {
		return nil, err
	}

	err = index.WaitForIndexingUpTo(ctx, txID)
	if err != nil {
		return nil, err
	}

	snap, err := index.SnapshotMustIncludeTxWithRenewalPeriod(ctx, txID, renewalPeriod)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     l,
		prefix: index.TargetPrefix(),
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

func (l *Ledger) CommittedAlh() (uint64, [sha256.Size]byte) {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.committedTxID, l.committedAlh
}

func (l *Ledger) PrecommittedAlh() (uint64, [sha256.Size]byte) {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	durablePrecommittedTxID, _, _ := l.durablePrecommitWHub.Status()

	if durablePrecommittedTxID == l.committedTxID {
		return l.committedTxID, l.committedAlh
	}

	if durablePrecommittedTxID == l.inmemPrecommittedTxID {
		return l.inmemPrecommittedTxID, l.inmemPrecommittedAlh
	}

	// fetch latest precommitted (durable) transaction from s.cLogBuf
	txID, alh, _, _, _ := l.cLogBuf.readAhead(int(durablePrecommittedTxID - l.committedTxID - 1))

	return txID, alh
}

func (l *Ledger) precommittedAlh() (uint64, [sha256.Size]byte) {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.inmemPrecommittedTxID, l.inmemPrecommittedAlh
}

func (l *Ledger) syncBinaryLinking() error {
	l.logger.Infof("syncing binary-linking at '%s'...", l.path)

	tx, err := l.fetchAllocTx()
	if err != nil {
		return err
	}
	defer l.releaseAllocTx(tx)

	txReader, err := l.newTxReader(l.aht.Size()+1, false, true, false, tx)
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return err
		}

		alh := tx.header.Alh()
		l.aht.Append(alh[:])

		if tx.header.ID%1000 == 0 {
			l.logger.Infof("binary-linking at '%s' in progress: processing tx: %d", l.path, tx.header.ID)
		}
	}

	l.logger.Infof("binary-linking up to date at '%s'", l.path)

	return nil
}

func (l *Ledger) WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error {
	l.waiteesMutex.Lock()

	if l.waiteesCount == l.maxWaitees {
		l.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	l.waiteesCount++

	l.waiteesMutex.Unlock()

	defer func() {
		l.waiteesMutex.Lock()
		l.waiteesCount--
		l.waiteesMutex.Unlock()
	}()

	var err error

	if allowPrecommitted {
		err = l.durablePrecommitWHub.WaitFor(ctx, txID)
	} else {
		err = l.commitWHub.WaitFor(ctx, txID)
	}
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return ErrAlreadyClosed
	}
	return err
}

func (l *Ledger) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	l.waiteesMutex.Lock()

	if l.waiteesCount == l.maxWaitees {
		l.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	l.waiteesCount++

	l.waiteesMutex.Unlock()

	defer func() {
		l.waiteesMutex.Lock()
		l.waiteesCount--
		l.waiteesMutex.Unlock()
	}()
	return l.store.indexerManager.WaitForIndexingUpTo(ctx, l.ID(), txID)
}

func (l *Ledger) ForEachIndex(onIndex func(index *index) error) error {
	return l.store.indexerManager.ForEachIndex(l.ID(), onIndex)
}

func (l *Ledger) CompactIndexes(force bool) error {
	if l.compactionDisabled {
		return ErrCompactionDisabled
	}
	return l.store.indexerManager.CompactIndexes(l.ID(), force)
}

func (l *Ledger) FlushIndexes(cleanupPercentage float32, synced bool) error {
	l.mutex.Lock()

	if l.closed {
		l.mutex.Unlock()
		return ErrAlreadyClosed
	}
	l.mutex.Unlock()

	// NOTE: cleanupPercentage and synced params are no more supported
	if cleanupPercentage < 0 || cleanupPercentage > 100 {
		return tbtree.ErrIllegalArguments
	}
	return l.store.indexerManager.Flush()
}

func maxTxSize(maxTxEntries, maxKeyLen, maxTxMetadataLen, maxKVMetadataLen int) int {
	return txIDSize /*txID*/ +
		tsSize /*ts*/ +
		txIDSize /*blTxID*/ +
		sha256.Size /*blRoot*/ +
		sha256.Size /*prevAlh*/ +
		sszSize /*versioin*/ +
		sszSize /*txMetadataLen*/ +
		maxTxMetadataLen +
		lszSize /*|entries|*/ +
		maxTxEntries*(sszSize /*kvMetadataLen*/ +
			maxKVMetadataLen+
			sszSize /*kLen*/ +
			maxKeyLen /*key*/ +
			lszSize /*vLen*/ +
			offsetSize /*vOff*/ +
			sha256.Size /*hValue*/) +
		sha256.Size /*eH*/ +
		sha256.Size /*txH*/
}

func (l *Ledger) ReadOnly() bool {
	return l.readOnly
}

func (l *Ledger) Synced() bool {
	return l.synced
}

func (l *Ledger) MaxActiveTransactions() int {
	return l.maxActiveTransactions
}

func (l *Ledger) MVCCReadSetLimit() int {
	return l.mvccReadSetLimit
}

func (l *Ledger) MaxConcurrency() int {
	return l.maxConcurrency
}

func (l *Ledger) MaxIOConcurrency() int {
	return l.maxIOConcurrency
}

func (l *Ledger) MaxTxEntries() int {
	return l.maxTxEntries
}

func (l *Ledger) MaxKeyLen() int {
	return l.maxKeyLen
}

func (l *Ledger) MaxValueLen() int {
	return l.maxValueLen
}

func (l *Ledger) Size() (uint64, error) {
	var size uint64

	err := filepath.WalkDir(l.path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}

func (l *Ledger) TxCount() uint64 {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.committedTxID
}

func (l *Ledger) fetchAllocTx() (*Tx, error) {
	tx, err := l.txPool.Alloc()
	if errors.Is(err, ErrTxPoolExhausted) {
		return nil, ErrMaxConcurrencyLimitExceeded
	}
	return tx, nil
}

func (l *Ledger) releaseAllocTx(tx *Tx) {
	l.txPool.Release(tx)
}

func encodeOffset(offset int64, vLogID byte) int64 {
	return int64(vLogID)<<56 | offset
}

func decodeOffset(offset int64) (byte, int64) {
	return byte(offset >> 56), offset & ^(0xff << 55)
}

func (l *Ledger) fetchAnyVLog() (vLodID byte, vLog appendable.Appendable) {
	l.vLogsCond.L.Lock()
	defer l.vLogsCond.L.Unlock()

	for l.vLogUnlockedList.Len() == 0 {
		l.vLogsCond.Wait()
	}

	vLogID := l.vLogUnlockedList.Remove(l.vLogUnlockedList.Front()).(byte) + 1
	l.vLogs[vLogID-1].unlockedRef = nil // locked

	return vLogID, l.vLogs[vLogID-1].vLog
}

func (l *Ledger) fetchVLog(vLogID byte) (appendable.Appendable, error) {
	if l.embeddedValues {
		if vLogID > 0 {
			return nil, fmt.Errorf("%w: attempt to read from a non-embedded vlog while using embedded values", ErrUnexpectedError)
		}

		l.commitStateRWMutex.Lock()
		return l.txLog, nil
	}

	l.vLogsCond.L.Lock()
	defer l.vLogsCond.L.Unlock()

	for l.vLogs[vLogID-1].unlockedRef == nil {
		l.vLogsCond.Wait()
	}

	l.vLogUnlockedList.Remove(l.vLogs[vLogID-1].unlockedRef)
	l.vLogs[vLogID-1].unlockedRef = nil // locked

	return l.vLogs[vLogID-1].vLog, nil
}

func (l *Ledger) releaseVLog(vLogID byte) error {
	if l.embeddedValues {
		if vLogID > 0 {
			return fmt.Errorf("%w: attempt to release a non-embedded vlog while using embedded values", ErrUnexpectedError)
		}

		l.commitStateRWMutex.Unlock()
		return nil
	}

	l.vLogsCond.L.Lock()
	l.vLogs[vLogID-1].unlockedRef = l.vLogUnlockedList.PushBack(vLogID - 1) // unlocked
	l.vLogsCond.L.Unlock()
	l.vLogsCond.Signal()

	return nil
}

type appendableResult struct {
	offsets []int64
	err     error
}

func (l *Ledger) appendValuesIntoAnyVLog(entries []*EntrySpec) (offsets []int64, err error) {
	vLogID, vLog := l.fetchAnyVLog()
	defer l.releaseVLog(vLogID)

	offsets, err = l.appendValuesInto(entries, vLog)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(offsets); i++ {
		offsets[i] = encodeOffset(offsets[i], vLogID)
	}

	return offsets, nil
}

func (l *Ledger) appendValuesInto(entries []*EntrySpec, app appendable.Appendable) (offsets []int64, err error) {
	offsets = make([]int64, len(entries))

	for i := 0; i < len(offsets); i++ {
		if len(entries[i].Value) == 0 {
			continue
		}

		offsets[i], _, err = app.Append(entries[i].Value)
		if err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

func (l *Ledger) NewWriteOnlyTx(ctx context.Context) (*OngoingTx, error) {
	return newOngoingTx(ctx, l, &TxOptions{Mode: WriteOnlyTx})
}

func (l *Ledger) NewTx(ctx context.Context, opts *TxOptions) (*OngoingTx, error) {
	return newOngoingTx(ctx, l, opts)
}

func (l *Ledger) commit(ctx context.Context, otx *OngoingTx, expectedHeader *TxHeader, skipIntegrityCheck bool, waitForIndexing bool) (*TxHeader, error) {
	hdr, err := l.precommit(ctx, otx, expectedHeader, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = l.commitWHub.WaitFor(ctx, hdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	if waitForIndexing {
		err = l.WaitForIndexingUpto(ctx, hdr.ID)
		// header is returned because transaction is already committed
		if err != nil {
			return hdr, err
		}
	}
	return hdr, nil
}

func (l *Ledger) precommit(ctx context.Context, otx *OngoingTx, hdr *TxHeader, skipIntegrityCheck bool) (*TxHeader, error) {
	if otx == nil {
		return nil, fmt.Errorf("%w: no transaction", ErrIllegalArguments)
	}

	err := otx.validateAgainst(hdr)
	if err != nil {
		return nil, fmt.Errorf("%w: transaction does not validate against header", err)
	}

	// extra metadata are specified by the client and thus they are only allowed when entries is non empty
	if len(otx.entries) == 0 && (otx.metadata.IsEmpty() || otx.metadata.HasExtraOnly()) {
		return nil, ErrNoEntriesProvided
	}

	err = l.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = l.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	tx, err := l.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer l.releaseAllocTx(tx)

	if hdr == nil {
		tx.header.Version = l.writeTxHeaderVersion
	} else {
		tx.header.Version = hdr.Version
	}

	tx.header.Metadata = otx.metadata

	tx.header.NEntries = len(otx.entries)

	doneWithValuesCh := make(chan appendableResult)
	defer close(doneWithValuesCh)

	go func() {
		// value write is delayed to ensure values are inmediatelly followed by the associated tx header
		if l.embeddedValues {
			doneWithValuesCh <- appendableResult{nil, nil}
			return
		}

		offsets, err := l.appendValuesIntoAnyVLog(otx.entries)
		if err != nil {
			doneWithValuesCh <- appendableResult{nil, err}
			return
		}

		doneWithValuesCh <- appendableResult{offsets, nil}
	}()

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		if e.IsValueTruncated {
			txe.hVal = e.HashValue
		} else {
			txe.hVal = sha256.Sum256(e.Value)
		}
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-doneWithValuesCh // wait for data to be written
		return nil, err
	}

	valueWritingResult := <-doneWithValuesCh // wait for data to be written
	if valueWritingResult.err != nil {
		return nil, valueWritingResult.err
	}

	if !l.embeddedValues {
		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = valueWritingResult.offsets[i]
		}
	}

	if hdr != nil {
		// TODO: Eh validation is disabled as it's not provided
		// when the tx was exported without integrity checks
		if !skipIntegrityCheck && tx.header.Eh != hdr.Eh {
			return nil, fmt.Errorf("%w: entries hash (Eh) differs", ErrIllegalArguments)
		}

		lastPreCommittedTxID := l.LastPrecommittedTxID()

		if lastPreCommittedTxID >= hdr.ID {
			return nil, ErrTxAlreadyCommitted
		}

		if hdr.ID > lastPreCommittedTxID+uint64(l.maxActiveTransactions) {
			return nil, ErrMaxActiveTransactionsLimitExceeded
		}

		// ensure tx is committed in the expected order
		err = l.inmemPrecommitWHub.WaitFor(ctx, hdr.ID-1)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return nil, ErrAlreadyClosed
		}
		if err != nil {
			return nil, err
		}

		var blRoot [sha256.Size]byte

		if hdr.BlTxID > 0 {
			blRoot, err = l.aht.RootAt(hdr.BlTxID)
			if err != nil && !errors.Is(err, ahtree.ErrEmptyTree) {
				return nil, err
			}
		}

		if blRoot != hdr.BlRoot {
			return nil, fmt.Errorf("%w: attempt to commit a tx with invalid blRoot", ErrIllegalArguments)
		}
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return nil, ErrAlreadyClosed
	}

	currPrecomittedTxID, currPrecommittedAlh := l.precommittedAlh()

	var ts int64
	var blTxID uint64
	if hdr == nil {
		ts = l.timeFunc().Unix()
		blTxID = l.aht.Size()
	} else {
		ts = hdr.Ts
		blTxID = hdr.BlTxID

		// currTxID and currAlh were already checked before,
		// but we have to add an additional check once the commit mutex
		// is locked to ensure that those constraints are still valid
		// in case of simultaneous writers
		if currPrecomittedTxID > hdr.ID-1 {
			return nil, ErrTxAlreadyCommitted
		}

		if currPrecomittedTxID < hdr.ID-1 {
			return nil, fmt.Errorf("%w: attempt to commit a tx in wrong order", ErrUnexpectedError)
		}

		if currPrecommittedAlh != hdr.PrevAlh {
			return nil, fmt.Errorf("%w: attempt to commit a tx with invalid prevAlh", ErrIllegalArguments)
		}
	}

	if otx.hasPreconditions() {
		var waitForIndexingUpto uint64

		if otx.unsafeMVCC {
			waitForIndexingUpto = l.mandatoryMVCCUpToTxID
		} else {
			// Preconditions must be executed with up-to-date tree
			waitForIndexingUpto = currPrecomittedTxID
		}

		// TODO: only call WaitForIndexingUpTo for the indexes that have been read by otx.
		err = l.WaitForIndexingUpto(ctx, waitForIndexingUpto)
		if err != nil {
			return nil, err
		}

		err = otx.checkCanCommit(ctx)
		if err != nil {
			return nil, err
		}
	}

	err = l.performPrecommit(tx, otx.entries, ts, blTxID)
	if err != nil {
		return nil, err
	}

	if otx.requireMVCCOnFollowingTxs {
		l.mandatoryMVCCUpToTxID = tx.header.ID
	}
	return tx.Header(), err
}

func (l *Ledger) LastCommittedTxID() uint64 {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.committedTxID
}

func (l *Ledger) LastPrecommittedTxID() uint64 {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.inmemPrecommittedTxID
}

func (l *Ledger) MandatoryMVCCUpToTxID() uint64 {
	l.commitStateRWMutex.RLock()
	defer l.commitStateRWMutex.RUnlock()

	return l.mandatoryMVCCUpToTxID
}

func (l *Ledger) performPrecommit(tx *Tx, entries []*EntrySpec, ts int64, blTxID uint64) error {
	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	// limit the maximum number of pre-committed transactions
	if l.synced && l.committedTxID+uint64(l.maxActiveTransactions) <= l.inmemPrecommittedTxID {
		return ErrMaxActiveTransactionsLimitExceeded
	}

	tx.header.ID = l.inmemPrecommittedTxID + 1
	tx.header.Ts = ts

	tx.header.BlTxID = blTxID

	if blTxID > 0 {
		blRoot, err := l.aht.RootAt(blTxID)
		if err != nil && !errors.Is(err, ahtree.ErrEmptyTree) {
			return err
		}
		tx.header.BlRoot = blRoot
	}

	if tx.header.ID <= tx.header.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.header.PrevAlh = l.inmemPrecommittedAlh

	txSize, txPrefixLen, err := l.serializeTx(tx, entries)
	if err != nil {
		return err
	}

	// tx serialization using pre-allocated buffer
	alh := tx.header.Alh()

	copy(l._txbs[txSize:], alh[:])
	txSize += sha256.Size

	txbs := make([]byte, txSize)
	copy(txbs, l._txbs[:txSize])

	txOff, _, err := l.txLog.Append(txbs)
	if err != nil {
		return err
	}

	_, _, err = l.txLogCache.Put(tx.header.ID, txbs)
	if err != nil {
		return err
	}

	err = l.aht.ResetSize(l.inmemPrecommittedTxID)
	if err != nil {
		return err
	}
	_, _, err = l.aht.Append(alh[:])
	if err != nil {
		return err
	}

	err = l.cLogBuf.put(l.inmemPrecommittedTxID+1, alh, txOff, txSize)
	if err != nil {
		return err
	}

	l.inmemPrecommittedTxID++
	l.inmemPrecommittedAlh = alh
	l.precommittedTxLogSize += int64(txPrefixLen + txSize)

	l.inmemPrecommitWHub.DoneUpto(l.inmemPrecommittedTxID)

	if !l.synced {
		l.durablePrecommitWHub.DoneUpto(l.inmemPrecommittedTxID)

		return l.mayCommit()
	}

	return nil
}

func (l *Ledger) serializeTx(tx *Tx, entries []*EntrySpec) (int, int, error) {
	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(l._txbs[txSize:], uint64(tx.header.ID))
	txSize += txIDSize
	binary.BigEndian.PutUint64(l._txbs[txSize:], uint64(tx.header.Ts))
	txSize += tsSize
	binary.BigEndian.PutUint64(l._txbs[txSize:], uint64(tx.header.BlTxID))
	txSize += txIDSize
	copy(l._txbs[txSize:], tx.header.BlRoot[:])
	txSize += sha256.Size
	copy(l._txbs[txSize:], tx.header.PrevAlh[:])
	txSize += sha256.Size

	binary.BigEndian.PutUint16(l._txbs[txSize:], uint16(tx.header.Version))
	txSize += sszSize

	switch tx.header.Version {
	case 0:
		{
			binary.BigEndian.PutUint16(l._txbs[txSize:], uint16(tx.header.NEntries))
			txSize += sszSize
		}
	case 1:
		{
			var txmdbs []byte

			if tx.header.Metadata != nil {
				txmdbs = tx.header.Metadata.Bytes()
			}

			binary.BigEndian.PutUint16(l._txbs[txSize:], uint16(len(txmdbs)))
			txSize += sszSize

			copy(l._txbs[txSize:], txmdbs)
			txSize += len(txmdbs)

			binary.BigEndian.PutUint32(l._txbs[txSize:], uint32(tx.header.NEntries))
			txSize += lszSize
		}
	default:
		{
			panic(fmt.Errorf("missing tx serialization method for version %d", tx.header.Version))
		}
	}

	// will overwrite partially written and uncommitted data
	err := l.txLog.SetOffset(l.precommittedTxLogSize)
	if err != nil {
		return -1, -1, fmt.Errorf("%w: could not set offset in txLog", err)
	}

	// total values length will be prefixed when values are embedded into txLog
	txPrefixLen := 0

	if l.embeddedValues {
		embeddedValuesLen := 0
		for _, e := range entries {
			embeddedValuesLen += len(e.Value)
		}

		var embeddedValuesLenBs [sszSize]byte
		binary.BigEndian.PutUint16(embeddedValuesLenBs[:], uint16(embeddedValuesLen))

		_, _, err := l.txLog.Append(embeddedValuesLenBs[:])
		if err != nil {
			return -1, -1, fmt.Errorf("%w: writing transaction values", err)
		}

		offsets, err := l.appendValuesInto(entries, l.txLog)
		if err != nil {
			return -1, -1, fmt.Errorf("%w: writing transaction values", err)
		}

		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = offsets[i]
		}

		txPrefixLen = sszSize + embeddedValuesLen
	}

	for i := 0; i < tx.header.NEntries; i++ {
		txe := tx.entries[i]

		// tx serialization using pre-allocated buffer
		// md is stored before key to ensure backward compatibility
		var kvmdbs []byte

		if txe.md != nil {
			kvmdbs = txe.md.Bytes()
		}

		binary.BigEndian.PutUint16(l._txbs[txSize:], uint16(len(kvmdbs)))
		txSize += sszSize
		copy(l._txbs[txSize:], kvmdbs)
		txSize += len(kvmdbs)
		binary.BigEndian.PutUint16(l._txbs[txSize:], uint16(txe.kLen))
		txSize += sszSize
		copy(l._txbs[txSize:], txe.k[:txe.kLen])
		txSize += txe.kLen
		binary.BigEndian.PutUint32(l._txbs[txSize:], uint32(txe.vLen))
		txSize += lszSize
		binary.BigEndian.PutUint64(l._txbs[txSize:], uint64(txe.vOff))
		txSize += offsetSize
		copy(l._txbs[txSize:], txe.hVal[:])
		txSize += sha256.Size
	}
	return txSize, txPrefixLen, nil
}

func (l *Ledger) SetExternalCommitAllowance(enabled bool) {
	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	l.useExternalCommitAllowance = enabled

	if enabled {
		l.commitAllowedUpToTxID = l.committedTxID
	}
}

// DiscardPrecommittedTxsSince discard precommitted txs
// No truncation is made into txLog which means, if the store is reopened
// some precommitted transactions may be reloaded.
// Discarding may need to be redone after re-opening the store.
func (l *Ledger) DiscardPrecommittedTxsSince(txID uint64) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return 0, ErrAlreadyClosed
	}

	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	if txID == 0 {
		return 0, fmt.Errorf("%w: invalid transaction ID", ErrIllegalArguments)
	}

	if txID <= l.committedTxID {
		return 0, fmt.Errorf("%w: only precommitted transactions can be discarded", ErrIllegalArguments)
	}

	if txID > l.inmemPrecommittedTxID {
		return 0, nil
	}

	txsToDiscard := int(l.inmemPrecommittedTxID + 1 - txID)

	err := l.aht.ResetSize(l.aht.Size() - uint64(txsToDiscard))
	if err != nil {
		return 0, err
	}

	// s.cLogBuf inludes all precommitted transactions (even durable ones)
	err = l.cLogBuf.recedeWriter(txsToDiscard)
	if err != nil {
		return 0, err
	}

	defer func() {
		durablePrecommittedTxID, _, _ := l.durablePrecommitWHub.Status()
		if durablePrecommittedTxID > l.inmemPrecommittedTxID {
			l.durablePrecommitWHub.RecedeTo(l.inmemPrecommittedTxID)
		}
	}()

	if txID-1 == l.committedTxID {
		l.inmemPrecommittedTxID = l.committedTxID
		l.inmemPrecommittedAlh = l.committedAlh
		return txsToDiscard, nil
	}

	tx, alh, _, _, err := l.cLogBuf.readAhead(int(l.inmemPrecommittedTxID-l.committedTxID-1) - txsToDiscard)
	if err != nil || tx != txID-1 {
		l.inmemPrecommittedTxID = l.committedTxID
		l.inmemPrecommittedAlh = l.committedAlh
		l.logger.Warningf("precommitted transactions has been discarded due to unexpected error in cLogBuf")
		return 0, err
	}

	l.inmemPrecommittedTxID = txID - 1
	l.inmemPrecommittedAlh = alh

	return txsToDiscard, nil
}

func (l *Ledger) AllowCommitUpto(txID uint64) error {
	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	if !l.useExternalCommitAllowance {
		return fmt.Errorf("%w: the external commit allowance mode is not enabled", ErrIllegalState)
	}

	if txID <= l.commitAllowedUpToTxID {
		// once a commit is allowed, it cannot be revoked
		return nil
	}

	if l.inmemPrecommittedTxID < txID {
		// commit allowances apply only to pre-committed transactions
		l.commitAllowedUpToTxID = l.inmemPrecommittedTxID
	} else {
		l.commitAllowedUpToTxID = txID
	}

	if !l.synced {
		return l.mayCommit()
	}
	return nil
}

// commitAllowedUpTo requires the caller to have already acquired the commitStateRWMutex lock
func (l *Ledger) commitAllowedUpTo() uint64 {
	if !l.useExternalCommitAllowance {
		return l.inmemPrecommittedTxID
	}

	return l.commitAllowedUpToTxID
}

// commitAllowedUpTo requires the caller to have already acquired the commitStateRWMutex lock
func (l *Ledger) mayCommit() error {
	commitAllowedUpToTxID := l.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - l.committedTxID)

	if txsCountToBeCommitted == 0 {
		return nil
	}

	// will overwrite partially written and uncommitted data
	err := l.cLog.SetOffset(int64(l.committedTxID) * int64(l.cLogEntrySize))
	if err != nil {
		return err
	}

	var commitUpToTxID uint64
	var commitUpToTxAlh [sha256.Size]byte

	for i := 0; i < txsCountToBeCommitted; i++ {
		txID, alh, txOff, txSize, err := l.cLogBuf.readAhead(i)
		if err != nil {
			return err
		}

		cb := make([]byte, l.cLogEntrySize)
		binary.BigEndian.PutUint64(cb, uint64(txOff))
		binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

		if l.cLogEntrySize == cLogEntrySizeV2 {
			copy(cb[offsetSize+lszSize:], alh[:])
		}

		_, _, err = l.cLog.Append(cb)
		if err != nil {
			return err
		}

		commitUpToTxID = txID
		commitUpToTxAlh = alh
	}

	// added as a safety fuse but this situation should NOT happen
	if commitUpToTxID != commitAllowedUpToTxID {
		return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d", ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
	}

	err = l.cLog.Flush()
	if err != nil {
		return err
	}

	err = l.cLogBuf.advanceReader(txsCountToBeCommitted)
	if err != nil {
		return err
	}

	l.committedTxID = commitUpToTxID
	l.committedAlh = commitUpToTxAlh

	l.doneCommitUpTo(commitUpToTxID, txsCountToBeCommitted)

	return nil
}

func (l *Ledger) doneCommitUpTo(upToTx uint64, committedTxs int) error {
	l.store.indexerManager.NotifyTransactions(uint64(committedTxs))

	err := l.commitWHub.DoneUpto(upToTx)
	return err
}

func (l *Ledger) CommitWith(ctx context.Context, callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error), waitForIndexing bool) (*TxHeader, error) {
	hdr, err := l.preCommitWith(ctx, callback)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = l.commitWHub.WaitFor(ctx, hdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	if waitForIndexing {
		err = l.WaitForIndexingUpto(ctx, hdr.ID)

		// header is returned because transaction is already committed
		if err != nil {
			return hdr, err
		}
	}

	return hdr, nil
}

type KeyIndex interface {
	Get(ctx context.Context, key []byte) (valRef ValueRef, err error)
	GetBetween(ctx context.Context, key []byte, initialTxID, finalTxID uint64) (valRef ValueRef, err error)
	GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (valRef ValueRef, err error)
	GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error)
	GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error)
}

type unsafeIndex struct {
	st *Ledger
}

func (index *unsafeIndex) Get(ctx context.Context, key []byte) (ValueRef, error) {
	return index.GetWithFilters(ctx, key, IgnoreDeleted, IgnoreExpired)
}

func (index *unsafeIndex) GetBetween(ctx context.Context, key []byte, initialTxID, finalTxID uint64) (valRef ValueRef, err error) {
	return index.st.GetBetween(ctx, key, initialTxID, finalTxID)
}

func (index *unsafeIndex) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (ValueRef, error) {
	return index.st.GetWithFilters(ctx, key, filters...)
}

func (index *unsafeIndex) GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error) {
	return index.st.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreDeleted, IgnoreExpired)
}

func (index *unsafeIndex) GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	return index.st.GetWithPrefixAndFilters(ctx, prefix, neq, filters...)
}

func (l *Ledger) preCommitWith(ctx context.Context, callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error)) (*TxHeader, error) {
	if callback == nil {
		return nil, ErrIllegalArguments
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return nil, ErrAlreadyClosed
	}

	otx, err := l.NewWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}
	defer otx.Cancel()

	l.store.indexerManager.PauseIndexing()
	defer l.store.indexerManager.ResumeIndexing()

	lastPreCommittedTxID := l.LastPrecommittedTxID()

	otx.entries, otx.preconditions, err = callback(lastPreCommittedTxID+1, &unsafeIndex{st: l})
	if err != nil {
		return nil, err
	}

	if len(otx.entries) == 0 {
		return nil, ErrNoEntriesProvided
	}

	err = l.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = l.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	if otx.hasPreconditions() {
		l.store.indexerManager.ResumeIndexing()

		// Preconditions must be executed with up-to-date tree
		err = l.WaitForIndexingUpto(ctx, lastPreCommittedTxID)
		if err != nil {
			return nil, err
		}

		err = otx.checkCanCommit(ctx)
		if err != nil {
			return nil, err
		}

		l.store.indexerManager.PauseIndexing()
	}

	tx, err := l.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer l.releaseAllocTx(tx)

	tx.header.Version = l.writeTxHeaderVersion
	tx.header.NEntries = len(otx.entries)

	doneWithValuesCh := make(chan appendableResult)
	defer close(doneWithValuesCh)

	go func() {
		if l.embeddedValues {
			// value write is delayed to ensure values are inmediatelly followed by the associated tx header
			doneWithValuesCh <- appendableResult{nil, nil}
			return
		}

		offsets, err := l.appendValuesIntoAnyVLog(otx.entries)
		if err != nil {
			doneWithValuesCh <- appendableResult{nil, err}
			return
		}

		doneWithValuesCh <- appendableResult{offsets, nil}
	}()

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		if e.IsValueTruncated {
			txe.hVal = e.HashValue
		} else {
			txe.hVal = sha256.Sum256(e.Value)
		}
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-doneWithValuesCh // wait for data to be written
		return nil, err
	}

	valueWritingResult := <-doneWithValuesCh // wait for data to be written
	if valueWritingResult.err != nil {
		return nil, valueWritingResult.err
	}

	if !l.embeddedValues {
		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = valueWritingResult.offsets[i]
		}
	}

	err = l.performPrecommit(tx, otx.entries, l.timeFunc().Unix(), l.aht.Size())
	if err != nil {
		return nil, err
	}

	return tx.Header(), nil
}

type DualProofV2 struct {
	SourceTxHeader   *TxHeader
	TargetTxHeader   *TxHeader
	InclusionProof   [][sha256.Size]byte
	ConsistencyProof [][sha256.Size]byte
}

type DualProof struct {
	SourceTxHeader     *TxHeader
	TargetTxHeader     *TxHeader
	InclusionProof     [][sha256.Size]byte
	ConsistencyProof   [][sha256.Size]byte
	TargetBlTxAlh      [sha256.Size]byte
	LastInclusionProof [][sha256.Size]byte
	LinearProof        *LinearProof
	LinearAdvanceProof *LinearAdvanceProof
}

func (l *Ledger) DualProofV2(sourceTxHdr, targetTxHdr *TxHeader) (proof *DualProofV2, err error) {
	if sourceTxHdr == nil || targetTxHdr == nil || sourceTxHdr.ID == 0 {
		return nil, ErrIllegalArguments
	}

	if sourceTxHdr.ID > targetTxHdr.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if sourceTxHdr.ID-1 != sourceTxHdr.BlTxID || targetTxHdr.ID-1 != targetTxHdr.BlTxID {
		return nil, ErrUnexpectedLinkingError
	}

	proof = &DualProofV2{
		SourceTxHeader: sourceTxHdr,
		TargetTxHeader: targetTxHdr,
	}

	if sourceTxHdr.ID < targetTxHdr.ID {
		proof.InclusionProof, err = l.aht.InclusionProof(sourceTxHdr.ID, targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof, err = l.aht.ConsistencyProof(maxUint64(1, sourceTxHdr.BlTxID), targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}
	}

	return proof, nil
}

// DualProof combines linear cryptographic linking i.e. transactions include the linear accumulative hash up to the previous one,
// with binary cryptographic linking generated by appending the linear accumulative hash values into an incremental hash tree, whose
// root is also included as part of each transaction and thus considered when calculating the linear accumulative hash.
// The objective of this proof is the same as the linear proof, that is, generate data for the calculation of the accumulative
// hash value of the target transaction from the linear accumulative hash value up to source transaction.
func (l *Ledger) DualProof(sourceTxHdr, targetTxHdr *TxHeader) (proof *DualProof, err error) {
	if sourceTxHdr == nil || targetTxHdr == nil {
		return nil, ErrIllegalArguments
	}

	if sourceTxHdr.ID > targetTxHdr.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	proof = &DualProof{
		SourceTxHeader: sourceTxHdr,
		TargetTxHeader: targetTxHdr,
	}

	if sourceTxHdr.ID < targetTxHdr.BlTxID {
		binInclusionProof, err := l.aht.InclusionProof(sourceTxHdr.ID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.InclusionProof = binInclusionProof
	}

	if sourceTxHdr.BlTxID > targetTxHdr.BlTxID {
		return nil, fmt.Errorf("%w: binary linking mismatch at tx %d", ErrCorruptedTxData, sourceTxHdr.ID)
	}

	if sourceTxHdr.BlTxID > 0 {
		// first root sourceTx.BlRoot, second one targetTx.BlRoot
		binConsistencyProof, err := l.aht.ConsistencyProof(sourceTxHdr.BlTxID, targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof = binConsistencyProof
	}

	if targetTxHdr.BlTxID > 0 {
		targetBlTxHdr, err := l.ReadTxHeader(targetTxHdr.BlTxID, false, false)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTxHdr.Alh()

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := l.aht.InclusionProof(targetTxHdr.BlTxID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.LastInclusionProof = binLastInclusionProof
	}

	lproof, err := l.LinearProof(maxUint64(sourceTxHdr.ID, targetTxHdr.BlTxID), targetTxHdr.ID)
	if err != nil {
		return nil, err
	}
	proof.LinearProof = lproof

	laproof, err := l.LinearAdvanceProof(
		sourceTxHdr.BlTxID,
		minUint64(sourceTxHdr.ID, targetTxHdr.BlTxID),
		targetTxHdr.BlTxID,
	)
	if err != nil {
		return nil, err
	}
	proof.LinearAdvanceProof = laproof

	return
}

type LinearProof struct {
	SourceTxID uint64
	TargetTxID uint64
	Terms      [][sha256.Size]byte
}

// LinearProof returns a list of hashes to calculate Alh@targetTxID from Alh@sourceTxID
func (l *Ledger) LinearProof(sourceTxID, targetTxID uint64) (*LinearProof, error) {
	if sourceTxID == 0 || sourceTxID > targetTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	tx, err := l.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer l.releaseAllocTx(tx)

	r, err := l.NewTxReader(sourceTxID, false, tx)
	if err != nil {
		return nil, err
	}

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	proof := make([][sha256.Size]byte, targetTxID-sourceTxID+1)
	proof[0] = tx.header.Alh()

	for i := 1; i < len(proof); i++ {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		proof[i] = tx.Header().innerHash()
	}

	return &LinearProof{
		SourceTxID: sourceTxID,
		TargetTxID: targetTxID,
		Terms:      proof,
	}, nil
}

// LinearAdvanceProof returns additional inclusion proof for part of the old linear proof consumed by
// the new Merkle Tree
func (l *Ledger) LinearAdvanceProof(sourceTxID, targetTxID uint64, targetBlTxID uint64) (*LinearAdvanceProof, error) {
	if targetTxID < sourceTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if targetTxID <= sourceTxID+1 {
		return nil, nil // Additional proof is not needed
	}

	tx, err := l.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer l.releaseAllocTx(tx)

	r, err := l.NewTxReader(sourceTxID+1, false, tx)
	if err != nil {
		return nil, err
	}

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	linearProofTerms := make([][sha256.Size]byte, targetTxID-sourceTxID)
	linearProofTerms[0] = tx.header.Alh()

	inclusionProofs := make([][][sha256.Size]byte, targetTxID-sourceTxID-1)

	for txID := sourceTxID + 1; txID < targetTxID; txID++ {
		inclusionProof, err := l.aht.InclusionProof(txID, targetBlTxID)
		if err != nil {
			return nil, err
		}
		inclusionProofs[txID-sourceTxID-1] = inclusionProof

		tx, err := r.Read()
		if err != nil {
			return nil, err
		}
		linearProofTerms[txID-sourceTxID] = tx.Header().innerHash()
	}

	return &LinearAdvanceProof{
		LinearProofTerms: linearProofTerms,
		InclusionProofs:  inclusionProofs,
	}, nil
}

type LinearAdvanceProof struct {
	LinearProofTerms [][sha256.Size]byte
	InclusionProofs  [][][sha256.Size]byte
}

func (l *Ledger) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off := int64(txID-1) * int64(l.cLogEntrySize)

	cb := make([]byte, l.cLogEntrySize)

	_, err := l.cLog.ReadAt(cb[:], int64(off))
	if errors.Is(err, multiapp.ErrAlreadyClosed) || errors.Is(err, singleapp.ErrAlreadyClosed) {
		return 0, 0, ErrAlreadyClosed
	}

	// A partially readable commit record must be discarded -
	// - it is a result of incomplete commit-log write
	// and will be overwritten on the next commit
	if errors.Is(err, io.EOF) {
		return 0, 0, ErrTxNotFound
	}
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(cb))
	txSize := int(binary.BigEndian.Uint32(cb[offsetSize:]))

	return txOffset, txSize, nil
}

type slicedReaderAt struct {
	bs  []byte
	off int64
}

func (r *slicedReaderAt) ReadAt(bs []byte, off int64) (n int, err error) {
	if off < r.off || int(off-r.off) > len(bs) {
		return 0, ErrIllegalState
	}

	o := int(off - r.off)
	available := len(r.bs) - o

	copy(bs, r.bs[o:minInt(available, len(bs))])

	if len(bs) > available {
		return available, io.EOF
	}

	return available, nil
}

func (l *Ledger) ExportTx(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool, tx *Tx) ([]byte, error) {
	err := l.readTx(txID, allowPrecommitted, skipIntegrityCheck, tx)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	hdrBs, err := tx.Header().Bytes()
	if err != nil {
		return nil, err
	}

	var b [lszSize]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(hdrBs)))
	_, err = buf.Write(b[:])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(hdrBs)
	if err != nil {
		return nil, err
	}

	var isValueTruncated bool

	for i, e := range tx.Entries() {
		var blen [lszSize]byte

		// kLen
		binary.BigEndian.PutUint16(blen[:], uint16(e.kLen))
		_, err = buf.Write(blen[:sszSize])
		if err != nil {
			return nil, err
		}

		// key
		_, err = buf.Write(e.Key())
		if err != nil {
			return nil, err
		}

		var md []byte

		if e.md != nil {
			md = e.md.Bytes()
		}

		// mdLen
		binary.BigEndian.PutUint16(blen[:], uint16(len(md)))
		_, err = buf.Write(blen[:sszSize])
		if err != nil {
			return nil, err
		}

		// md
		_, err = buf.Write(md)
		if err != nil {
			return nil, err
		}

		// val
		// TODO: improve value reading implementation, get rid of _valBs
		l._valBsMux.Lock()

		var valBuf []byte
		if e.vLen > len(l._valBs) {
			valBuf = make([]byte, e.vLen)
		} else {
			valBuf = l._valBs[:e.vLen]
		}

		_, err = l.readValueAt(valBuf, e.vOff, e.hVal, skipIntegrityCheck)
		if err != nil && !errors.Is(err, io.EOF) {
			l._valBsMux.Unlock()
			return nil, err
		}

		if err == nil {
			// currently, either all the values are sent or none
			if isValueTruncated {
				return nil, fmt.Errorf("%w: partially truncated transaction", ErrCorruptedData)
			}

			// vLen
			binary.BigEndian.PutUint32(blen[:], uint32(e.vLen))
			_, err = buf.Write(blen[:])
			if err != nil {
				l._valBsMux.Unlock()
				return nil, err
			}

			// val
			_, err = buf.Write(valBuf)
			if err != nil {
				l._valBsMux.Unlock()
				return nil, err
			}
		} else {
			// error is eof, the value has been truncated,
			// value is not available but digest is written instead

			// currently, either all the values are sent or none
			if !isValueTruncated && i > 0 {
				return nil, fmt.Errorf("%w: partially truncated transaction", ErrCorruptedData)
			}

			isValueTruncated = true

			// vHashLen
			binary.BigEndian.PutUint32(blen[:], uint32(len(e.hVal)))
			_, err = buf.Write(blen[:])
			if err != nil {
				l._valBsMux.Unlock()
				return nil, err
			}

			// vHash
			_, err = buf.Write(e.hVal[:])
			if err != nil {
				l._valBsMux.Unlock()
				return nil, err
			}
		}

		l._valBsMux.Unlock()
	}

	// NOTE: adding a boolean to the header to indicate if the transaction has values or not,
	// so that ReplicateTx knows if the transaction should be precommited with no values
	var truncatedValByte [1]byte
	truncatedValByte[0] = 0
	if isValueTruncated {
		truncatedValByte[0] = 1
	}

	binary.BigEndian.PutUint16(b[:], uint16(len(truncatedValByte)))
	_, err = buf.Write(b[:sszSize])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(truncatedValByte[:])
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (l *Ledger) ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*TxHeader, error) {
	if len(exportedTx) == 0 {
		return nil, ErrIllegalArguments
	}

	i := 0

	if len(exportedTx) < lszSize {
		return nil, ErrIllegalArguments
	}

	hdrLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
	i += lszSize

	if len(exportedTx) < i+hdrLen {
		return nil, ErrIllegalArguments
	}

	hdr := &TxHeader{}
	err := hdr.ReadFrom(exportedTx[i : i+hdrLen])
	if err != nil {
		return nil, err
	}
	i += hdrLen

	txSpec, err := l.NewWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}

	txSpec.metadata = hdr.Metadata

	var entries []*EntrySpec = make([]*EntrySpec, 0)

	for e := 0; e < hdr.NEntries; e++ {
		if len(exportedTx) < i+2*sszSize+lszSize {
			return nil, ErrIllegalArguments
		}

		kLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize

		if len(exportedTx) < i+sszSize+lszSize+kLen {
			return nil, ErrIllegalArguments
		}

		key := make([]byte, kLen)
		copy(key, exportedTx[i:])
		i += kLen

		mdLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize

		if len(exportedTx) < i+mdLen {
			return nil, ErrIllegalArguments
		}

		var md *KVMetadata

		if mdLen > 0 {
			md = newReadOnlyKVMetadata()

			err := md.unsafeReadFrom(exportedTx[i : i+mdLen])
			if err != nil {
				return nil, err
			}
			i += mdLen
		}

		// value
		vLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
		i += lszSize

		if len(exportedTx) < i+vLen {
			return nil, ErrIllegalArguments
		}

		entries = append(entries, &EntrySpec{
			Key:      key,
			Metadata: md,
			Value:    exportedTx[i : i+vLen],
		})

		i += vLen
	}

	var isTruncated bool

	// check if there is truncated value information in the transaction
	if i < len(exportedTx) {
		// information for truncated value
		tLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize
		if len(exportedTx) < i+tLen {
			return nil, ErrIllegalArguments
		}

		v := exportedTx[i : i+tLen]
		// v[0] == 1 means that the value is truncated
		// validate that the value is either 0 or 1
		if len(v) > 0 && v[0] > 1 {
			return nil, ErrIllegalTruncationArgument
		}
		isTruncated = v[0] == 1
		i += tLen
	}

	if i != len(exportedTx) {
		return nil, ErrIllegalArguments
	}

	// add entries to tx
	for _, e := range entries {
		var err error
		if isTruncated {
			err = txSpec.set(e.Key, e.Metadata, nil, digest(e.Value), isTruncated, false)
		} else {
			err = txSpec.set(e.Key, e.Metadata, e.Value, e.HashValue, isTruncated, false)
		}
		if err != nil {
			return nil, err
		}
	}

	txHdr, err := l.precommit(ctx, txSpec, hdr, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	// wait for syncing to happen before exposing the header
	err = l.durablePrecommitWHub.WaitFor(ctx, txHdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	l.commitStateRWMutex.Lock()
	waitForCommit := !l.useExternalCommitAllowance
	l.commitStateRWMutex.Unlock()

	if waitForCommit {
		err = l.commitWHub.WaitFor(ctx, txHdr.ID)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return nil, ErrAlreadyClosed
		}
		if err != nil {
			return nil, err
		}

		if waitForIndexing {
			err = l.WaitForIndexingUpto(ctx, txHdr.ID)
			if err != nil {
				return txHdr, err
			}
		}
	}

	return txHdr, nil
}

func (l *Ledger) FirstTxSince(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := l.LastCommittedTxID()

	for left < right {
		middle := left + (right-left)/2

		header, err := l.ReadTxHeader(middle, false, false)
		if err != nil {
			return nil, err
		}

		if header.Ts < ts.Unix() {
			left = middle + 1
		} else {
			right = middle
		}
	}

	header, err := l.ReadTxHeader(left, false, false)
	if err != nil {
		return nil, err
	}

	if header.Ts < ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (l *Ledger) LastTxUntil(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := l.LastCommittedTxID()

	for left < right {
		middle := left + ((right-left)+1)/2

		header, err := l.ReadTxHeader(middle, false, false)
		if err != nil {
			return nil, err
		}

		if header.Ts > ts.Unix() {
			right = middle - 1
		} else {
			left = middle
		}
	}

	header, err := l.ReadTxHeader(left, false, false)
	if err != nil {
		return nil, err
	}

	if header.Ts > ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (l *Ledger) appendableReaderForTx(txID uint64, allowPrecommitted bool) (*appendable.Reader, error) {
	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	if txID > l.inmemPrecommittedTxID || (!allowPrecommitted && txID > l.committedTxID) {
		return nil, ErrTxNotFound
	}

	cacheMiss := false

	txbs, err := l.txLogCache.Get(txID)
	if err != nil {
		if errors.Is(err, cache.ErrKeyNotFound) {
			cacheMiss = true
		} else {
			return nil, err
		}
	}

	var txOff int64
	var txSize int

	if txID <= l.committedTxID {
		txOff, txSize, err = l.txOffsetAndSize(txID)
	} else {
		_, _, txOff, txSize, err = l.cLogBuf.readAhead(int(txID - l.committedTxID - 1))
	}
	if err != nil {
		return nil, err
	}

	var txr io.ReaderAt

	if cacheMiss {
		txr = l.txLog
	} else {
		txr = &slicedReaderAt{bs: txbs.([]byte), off: txOff}
	}
	return appendable.NewReaderFrom(txr, txOff, txSize), nil
}

func (l *Ledger) ReadTxAt(txID uint64, tx *Tx) error {
	return l.readTx(txID, false, false, tx)
}

func (l *Ledger) ReadTx(txID uint64, skipIntegrityCheck bool, tx *Tx) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return ErrAlreadyClosed
	}
	return l.readTx(txID, false, skipIntegrityCheck, tx)
}

func (l *Ledger) readTx(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool, tx *Tx) error {
	r, err := l.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return err
	}

	err = tx.readFrom(r, skipIntegrityCheck)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: unexpected EOF while reading tx %d", ErrCorruptedTxData, txID)
	}
	return err
}

func (l *Ledger) ReadTxHeader(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool) (*TxHeader, error) {
	r, err := l.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return nil, err
	}

	tdr := &txDataReader{r: r, skipIntegrityCheck: skipIntegrityCheck}

	header, err := tdr.readHeader(l.maxTxEntries)
	if err != nil {
		return nil, err
	}

	e := &TxEntry{k: make([]byte, l.maxKeyLen)}
	for i := 0; i < header.NEntries; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, err
		}
	}

	htree, err := htree.New(header.NEntries)
	if err != nil {
		return nil, err
	}

	err = tdr.buildAndValidateHtree(htree)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (l *Ledger) ReadTxEntry(txID uint64, key []byte, skipIntegrityCheck bool) (*TxEntry, *TxHeader, error) {
	var ret *TxEntry

	r, err := l.appendableReaderForTx(txID, false)
	if err != nil {
		return nil, nil, err
	}

	tdr := &txDataReader{r: r, skipIntegrityCheck: skipIntegrityCheck}

	header, err := tdr.readHeader(l.maxTxEntries)
	if err != nil {
		return nil, nil, err
	}

	e := &TxEntry{k: make([]byte, l.maxKeyLen)}

	for i := 0; i < header.NEntries; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, nil, err
		}

		if bytes.Equal(e.key(), key) {
			if ret != nil {
				return nil, nil, ErrCorruptedTxDataDuplicateKey
			}
			ret = e

			// Allocate new placeholder for scanning the rest of entries
			e = &TxEntry{k: make([]byte, l.maxKeyLen)}
		}
	}
	if ret == nil {
		return nil, nil, ErrKeyNotFound
	}

	htree, err := htree.New(header.NEntries)
	if err != nil {
		return nil, nil, err
	}

	err = tdr.buildAndValidateHtree(htree)
	if err != nil {
		return nil, nil, err
	}

	return ret, header, nil
}

// ReadValue returns the actual associated value to a key at a specific transaction
// ErrExpiredEntry is be returned if the specified time has already elapsed
func (l *Ledger) ReadValue(entry *TxEntry) ([]byte, error) {
	if entry == nil || !entry.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && !entry.md.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && entry.md.ExpiredAt(time.Now()) {
		return nil, ErrExpiredEntry
	}

	if entry.vLen == 0 {
		// while not required, nil is returned instead of an empty slice

		// TODO: this step should be done after reading the value to ensure proper validations are made
		// But current changes in ExportTx with truncated transactions are not providing the value length
		// for truncated transactions, making it impossible to differentiate an empty value with a truncated one
		return nil, nil
	}

	b := make([]byte, entry.vLen)

	_, err := l.readValueAt(b, entry.vOff, entry.hVal, false)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (l *Ledger) ValueReaderAt(
	expectedLen int,
	off int64,
	hvalue [sha256.Size]byte,
	skipIntegrityCheck bool,
) (io.Reader, error) {
	vLogID, offset := decodeOffset(off)
	if !l.embeddedValues && vLogID == 0 && expectedLen > 0 {
		return nil, io.EOF // it means value was not stored on any vlog i.e. a truncated transaction was replicated
	}

	r := &valueReader{
		st:          l,
		expectedLen: expectedLen,
		vLogID:      vLogID,
		off:         offset,
		n:           0,
	}
	if skipIntegrityCheck {
		return r, nil
	}

	return &digestCheckReader{
		r:              r,
		h:              sha256.New(),
		expectedDigest: hvalue,
	}, nil
}

// readValueAt fills b with the value referenced by off
// expected value size and digest may be required for validations to pass
func (l *Ledger) readValueAt(b []byte, off int64, hvalue [sha256.Size]byte, skipIntegrityCheck bool) (n int, err error) {
	vLogID, offset := decodeOffset(off)

	if !l.embeddedValues && vLogID == 0 && len(b) > 0 {
		return 0, io.EOF // it means value was not stored on any vlog i.e. a truncated transaction was replicated
	}

	if len(b) > 0 {
		foundInCache := false

		if l.vLogCache != nil {
			val, err := l.vLogCache.Get(off)
			if err == nil {
				bval := val.([]byte) // the requested value was found in the value cache
				copy(b, bval)
				n = len(bval)
				foundInCache = true
			} else if !errors.Is(err, cache.ErrKeyNotFound) {
				return 0, err
			}
		}

		if !foundInCache {
			n, err = l.readValue(vLogID, offset, b)
			if err != nil {
				return n, err
			}

			if err := l.updateValueCache(off, b[:n]); err != nil {
				return 0, err
			}
		}
	}

	// either value was empty (n == 0)
	// or a non-empty value (n > 0) was read from cache or disk

	if !skipIntegrityCheck && (len(b) != n || hvalue != sha256.Sum256(b[:n])) {
		return n, fmt.Errorf("%w: value length or digest mismatch", ErrCorruptedData)
	}
	return n, nil
}

func (l *Ledger) updateValueCache(off int64, v []byte) error {
	if l.vLogCache == nil {
		return nil
	}

	// TODO: update could be done in a separate goroutine.

	cb := make([]byte, len(v))
	copy(cb, v)
	_, _, err := l.vLogCache.Put(off, cb)
	return err
}

func (l *Ledger) readValue(vLogID byte, off int64, buf []byte) (int, error) {
	vLog, err := l.fetchVLog(vLogID)
	if err != nil {
		return -1, err
	}
	defer l.releaseVLog(vLogID)

	n, err := vLog.ReadAt(buf, off)
	if errors.Is(err, multiapp.ErrAlreadyClosed) || errors.Is(err, singleapp.ErrAlreadyClosed) {
		return n, ErrAlreadyClosed
	}
	return n, err
}

func (l *Ledger) validateEntries(entries []*EntrySpec) error {
	if len(entries) > l.maxTxEntries {
		return ErrMaxTxEntriesLimitExceeded
	}

	m := make(map[string]struct{}, len(entries))

	for _, kv := range entries {
		if kv.Key == nil {
			return ErrNullKey
		}

		if len(kv.Key) > l.maxKeyLen {
			return ErrMaxKeyLenExceeded
		}
		if len(kv.Value) > l.maxValueLen {
			return ErrMaxValueLenExceeded
		}

		ks := slices.BytesToString(kv.Key)
		if _, ok := m[ks]; ok {
			return ErrDuplicatedKey
		}
		m[ks] = struct{}{}
	}

	return nil
}

func (l *Ledger) validatePreconditions(preconditions []Precondition) error {
	if len(preconditions) > l.maxTxEntries {
		return ErrInvalidPreconditionTooMany
	}

	for _, c := range preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}

		err := c.Validate(l)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Ledger) Sync() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return ErrAlreadyClosed
	}
	return l.sync()
}

func (l *Ledger) sync() error {
	l.commitStateRWMutex.Lock()
	defer l.commitStateRWMutex.Unlock()

	if l.inmemPrecommittedTxID == l.committedTxID {
		// everything already synced
		return nil
	}

	for i := range l.vLogs {
		vLog, err := l.fetchVLog(i + 1)
		if err != nil {
			return err
		}
		defer l.releaseVLog(i + 1)

		err = vLog.Flush()
		if err != nil {
			return err
		}

		err = vLog.Sync()
		if err != nil {
			return err
		}
	}

	err := l.txLog.Flush()
	if err != nil {
		return err
	}

	err = l.txLog.Sync()
	if err != nil {
		return err
	}

	err = l.durablePrecommitWHub.DoneUpto(l.inmemPrecommittedTxID)
	if err != nil {
		return err
	}

	commitAllowedUpToTxID := l.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - l.committedTxID)

	if txsCountToBeCommitted == 0 {
		return nil
	}

	// will overwrite partially written and uncommitted data
	err = l.cLog.SetOffset(int64(l.committedTxID) * int64(l.cLogEntrySize))
	if err != nil {
		return err
	}

	var commitUpToTxID uint64
	var commitUpToTxAlh [sha256.Size]byte

	for i := 0; i < txsCountToBeCommitted; i++ {
		txID, alh, txOff, txSize, err := l.cLogBuf.readAhead(i)
		if err != nil {
			return err
		}

		cb := make([]byte, l.cLogEntrySize)
		binary.BigEndian.PutUint64(cb, uint64(txOff))
		binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

		if l.cLogEntrySize == cLogEntrySizeV2 {
			copy(cb[offsetSize+lszSize:], alh[:])
		}

		_, _, err = l.cLog.Append(cb)
		if err != nil {
			return err
		}

		commitUpToTxID = txID
		commitUpToTxAlh = alh
	}

	// added as a safety fuse but this situation should NOT happen
	if commitUpToTxID != commitAllowedUpToTxID {
		return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d", ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
	}

	err = l.cLog.Flush()
	if err != nil {
		return err
	}

	err = l.cLog.Sync()
	if err != nil {
		return err
	}

	err = l.cLogBuf.advanceReader(txsCountToBeCommitted)
	if err != nil {
		return err
	}

	l.committedTxID = commitUpToTxID
	l.committedAlh = commitUpToTxAlh

	l.doneCommitUpTo(commitUpToTxID, txsCountToBeCommitted)

	return nil
}

func (l *Ledger) IsClosed() bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.closed
}

func (l *Ledger) Close() error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return ErrAlreadyClosed
	}

	l.closed = true
	merr := multierr.NewMultiErr()

	for i := range l.vLogs {
		vLog, err := l.fetchVLog(i + 1)
		merr.Append(err)

		err = vLog.Close()
		merr.Append(err)

		err = l.releaseVLog(i + 1)
		merr.Append(err)
	}

	err := l.store.indexerManager.CloseLedgerIndexing(l.ID())
	merr.Append(err)

	err = l.inmemPrecommitWHub.Close()
	merr.Append(err)

	err = l.durablePrecommitWHub.Close()
	merr.Append(err)

	err = l.commitWHub.Close()
	merr.Append(err)

	err = l.txLog.Close()
	merr.Append(err)

	err = l.cLog.Close()
	merr.Append(err)

	err = l.aht.Close()
	merr.Append(err)

	used, _, _ := l.txPool.Stats()
	if used > 0 {
		merr.Append(errors.New("not all tx holders were released"))
	}
	return merr.Reduce()
}

func (l *Ledger) wrapAppendableErr(err error, action string) error {
	if errors.Is(err, singleapp.ErrAlreadyClosed) || errors.Is(err, multiapp.ErrAlreadyClosed) {
		l.logger.Warningf("Got '%v' while '%s'", err, action)
		return ErrAlreadyClosed
	}

	return err
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a <= b {
		return b
	}
	return a
}

func minUint64(a, b uint64) uint64 {
	if a >= b {
		return b
	}
	return a
}

// readTxOffsetAt reads the value-log offset of a specific entry (index) in a transaction (txID)
// txID is the transaction ID
// index is the index of the entry in the transaction
// allowPrecommitted indicates if a precommitted transaction can be read
func (l *Ledger) readTxOffsetAt(txID uint64, allowPrecommitted bool, index int) (*TxEntry, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.closed {
		return nil, ErrAlreadyClosed
	}

	r, err := l.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return nil, err
	}

	tdr := &txDataReader{r: r}

	hdr, err := tdr.readHeader(l.maxTxEntries)
	if err != nil {
		return nil, err
	}

	if hdr.NEntries < index {
		return nil, ErrTxEntryIndexOutOfRange
	}

	e := &TxEntry{k: make([]byte, l.maxKeyLen)}

	for i := 0; i < index; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, err
		}
	}

	return e, nil
}

// TruncateUptoTx deletes the value-log file up to transactions
// that are strictly below the specified minTxID.
func (l *Ledger) TruncateUptoTx(minTxID uint64) error {
	/*
		When values are appended to the value log file, they are not
		inserted in strict monotic order of time. Depending on the
		maxConcurrency value, there could be n transactions trying
		to insert into the log at the same time. This could lead to
		the situation where a transaction (n+1) could be inserted
		in the value log before transaction (n)
						  discard point
								|
								v
				--------+-------+--------+----------
						|       |        |
					tn+1:vx   tn:vx  tn-1:vx
						|                |
						+----------------+
						max concurrency
							range
		If the log is truncated upto tn, it could lead to removal of
		data for tn+1. To avoid this overlap, we first go back from
		the discard point to fetch offsets across vlogs for safe delete
		points, and then check for transactions further than the
		discard point to figure out the least offset from the range
		which can safely be deleted. This offset, tn+1 in the above
		scenario is the safest point to discard upto to avoid deletion
		of values for any future transaction.
	*/

	l.logger.Infof("running truncation up to transaction '%d'", minTxID)

	if l.embeddedValues {
		l.logger.Infof("truncation with embedded values does not delete any data")
		return nil
	}

	// tombstones maintain the minimum offset for each value log file that can be safely deleted.
	tombstones := make(map[byte]int64)

	readFirstEntryOffset := func(id uint64) (*TxEntry, error) {
		return l.readTxOffsetAt(id, false, 1)
	}

	back := func(txID uint64) error {
		firstTxEntry, err := readFirstEntryOffset(txID)
		if err != nil {
			return err
		}

		// Iterate over past transactions and store the minimum offset for each value log file.
		vLogID, off := decodeOffset(firstTxEntry.VOff())
		if _, ok := tombstones[vLogID]; !ok {
			tombstones[vLogID] = off
		}
		return nil
	}

	front := func(txID uint64) error {
		firstTxEntry, err := readFirstEntryOffset(txID)
		if err != nil {
			return err
		}

		// Check if any future transaction offset lies before past transaction(s)
		// If so, then update the offset to the minimum offset for that value log file.
		vLogID, off := decodeOffset(firstTxEntry.VOff())
		if val, ok := tombstones[vLogID]; ok {
			if off < val {
				tombstones[vLogID] = off
			}
		}
		return nil
	}

	// Walk back to get offsets across vlogs which can be deleted safely.
	// This way, we can calculate the minimum offset for each value log file.
	{
		var i uint64 = minTxID
		for i > 0 && len(tombstones) != l.MaxIOConcurrency() {
			err := back(i)
			// if there is an error reading a transaction, stop the traversal and return the error.
			if err != nil && !errors.Is(err, ErrTxEntryIndexOutOfRange) /* tx has entries*/ {
				l.logger.Errorf("failed to fetch transaction %d {traversal=back, err = %v}", i, err)
				return err
			}
			i--
		}
	}

	// Walk front to check if any future transaction offset lies before past transaction(s)
	{
		// Check for transactions upto the last committed transaction to avoid deletion of values for any future transaction.
		maxTxID := l.LastCommittedTxID()
		l.logger.Infof("running truncation check between transaction '%d' and '%d'", minTxID, maxTxID)

		// TODO: add more integration tests
		// Iterate over all future transactions to check if any offset lies before past transaction(s) offset.
		for j := minTxID; j <= maxTxID; j++ {
			err := front(j)
			if err != nil && !errors.Is(err, ErrTxEntryIndexOutOfRange) /* tx has entries*/ {
				l.logger.Errorf("failed to fetch transaction %d {traversal=front, err = %v}", j, err)
				return err
			}
		}
	}

	// Delete offset from different value logs
	merr := multierr.NewMultiErr()
	{
		for vLogID, offset := range tombstones {
			vlog, err := l.fetchVLog(vLogID)
			if err != nil {
				merr.Append(err)
				continue
			}
			defer l.releaseVLog(vLogID)

			l.logger.Infof("truncating vlog '%d' at offset '%d'", vLogID, offset)
			err = vlog.DiscardUpto(offset)
			merr.Append(err)
		}
	}

	return merr.Reduce()
}

func (l *Ledger) ID() LedgerID {
	return l.id
}

func (l *Ledger) Path() string {
	return l.path
}

func (l *Ledger) Name() string {
	return filepath.Base(l.path)
}

func (l *Ledger) Options() *Options {
	return l.opts
}

func digest(s []byte) [sha256.Size]byte {
	var a [sha256.Size]byte
	copy(a[:], s)
	return a
}
