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
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded"
	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"

	"github.com/codenotary/immudb/pkg/logger"
)

var ErrIllegalArguments = embedded.ErrIllegalArguments
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrAlreadyClosed = embedded.ErrAlreadyClosed
var ErrUnexpectedLinkingError = errors.New("internal inconsistency between linear and binary linking")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrWriteOnlyTx = errors.New("write-only transaction")
var ErrTxReadConflict = errors.New("tx read conflict")
var ErrTxAlreadyCommitted = errors.New("tx already committed")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrNullKey = errors.New("null key")
var ErrorMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrPreconditionFailed = errors.New("precondition failed")
var ErrDuplicatedKey = errors.New("duplicated key")
var ErrMaxActiveTransactionsLimitExceeded = errors.New("max active transactions limit exceeded")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrorCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedTxDataMaxTxEntriesExceeded = fmt.Errorf("%w: maximum number of TX entries exceeded", ErrorCorruptedTxData)
var ErrCorruptedTxDataUnknownHeaderVersion = fmt.Errorf("%w: unknown TX header version", ErrorCorruptedTxData)
var ErrCorruptedTxDataMaxKeyLenExceeded = fmt.Errorf("%w: maximum key length exceeded", ErrorCorruptedTxData)
var ErrCorruptedTxDataDuplicateKey = fmt.Errorf("%w: duplicate key in a single TX", ErrorCorruptedTxData)
var ErrCorruptedData = errors.New("data is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrCorruptedIndex = errors.New("corrupted index")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")
var ErrCorruptedAHtree = errors.New("appendable hash tree is corrupted")
var ErrKeyNotFound = tbtree.ErrKeyNotFound // TODO: define error in store layer
var ErrExpiredEntry = fmt.Errorf("%w: expired entry", ErrKeyNotFound)
var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrTxNotFound = errors.New("tx not found")
var ErrNoMoreEntries = tbtree.ErrNoMoreEntries       // TODO: define error in store layer
var ErrIllegalState = tbtree.ErrIllegalState         // TODO: define error in store layer
var ErrOffsetOutOfRange = tbtree.ErrOffsetOutOfRange // TODO: define error in store layer
var ErrUnexpectedError = errors.New("unexpected error")
var ErrUnsupportedTxVersion = errors.New("unsupported tx version")
var ErrNewerVersionOrCorruptedData = errors.New("tx created with a newer version or data is corrupted")
var ErrTxPoolExhausted = errors.New("transaction pool exhausted")

var ErrInvalidPrecondition = errors.New("invalid precondition")
var ErrInvalidPreconditionTooMany = fmt.Errorf("%w: too many preconditions", ErrInvalidPrecondition)
var ErrInvalidPreconditionNull = fmt.Errorf("%w: null", ErrInvalidPrecondition)
var ErrInvalidPreconditionNullKey = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrNullKey)
var ErrInvalidPreconditionMaxKeyLenExceeded = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrorMaxKeyLenExceeded)
var ErrInvalidPreconditionInvalidTxID = fmt.Errorf("%w: invalid transaction ID", ErrInvalidPrecondition)

var ErrSourceTxNewerThanTargetTx = errors.New("source tx is newer than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("max linear proof length limit exceeded")

var ErrCompactionUnsupported = errors.New("compaction is unsupported when remote storage is used")

var ErrMetadataUnsupported = errors.New(
	"metadata is unsupported when in 1.1 compatibility mode, " +
		"do not use metadata-related features such as expiration and logical deletion",
)

var ErrUnsupportedTxHeaderVersion = errors.New("missing tx header serialization method")

const MaxKeyLen = 1024 // assumed to be not lower than hash size
const MaxParallelIO = 127

const cLogEntrySize = offsetSize + lszSize // tx offset & size

const txIDSize = 8
const tsSize = 8
const lszSize = 4
const sszSize = 2
const offsetSize = 8

const Version = 1
const MaxTxHeaderVersion = 1

const (
	metaVersion      = "VERSION"
	metaMaxTxEntries = "MAX_TX_ENTRIES"
	metaMaxKeyLen    = "MAX_KEY_LEN"
	metaMaxValueLen  = "MAX_VALUE_LEN"
	metaFileSize     = "FILE_SIZE"
)

const indexDirname = "index"
const ahtDirname = "aht"

type ImmuStore struct {
	path string

	logger           logger.Logger
	lastNotification time.Time
	notifyMutex      sync.Mutex

	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	txLog      appendable.Appendable
	txLogCache *cache.LRUCache

	cLog appendable.Appendable

	cLogBuf *precommitBuffer

	committedTxID uint64
	committedAlh  [sha256.Size]byte

	preCommittedTxID      uint64
	preCommittedAlh       [sha256.Size]byte
	preCommittedTxLogSize int64

	commitStateRWMutex sync.RWMutex

	readOnly              bool
	synced                bool
	syncFrequency         time.Duration
	maxActiveTransactions int
	maxWaitees            int
	maxConcurrency        int
	maxIOConcurrency      int
	maxTxEntries          int
	maxKeyLen             int
	maxValueLen           int
	maxLinearProofLen     int

	maxTxSize int

	writeTxHeaderVersion int

	timeFunc TimeFunc

	useExternalCommitAllowance bool
	commitAllowedUpToTxID      uint64

	txPool TxPool

	waiteesMutex sync.Mutex
	waiteesCount int // current number of go-routines waiting for a tx to be indexed or committed

	_txbs []byte       // pre-allocated buffer to support tx serialization
	_kvs  []*tbtree.KV //pre-allocated for indexing

	aht      *ahtree.AHtree
	blBuffer chan ([sha256.Size]byte)
	blErr    error

	precommitWHub *watchers.WatchersHub
	commitWHub    *watchers.WatchersHub

	indexer *indexer

	closed bool
	blDone chan (struct{})

	mutex sync.Mutex

	compactionDisabled bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIllegalArguments, err)
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		err := os.Mkdir(path, opts.FileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaVersion, Version)
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
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.TxLogMaxOpenedFiles)
	txLog, err := appFactory(path, "tx", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open transaction log: %w", err)
	}

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.CommitLogMaxOpenedFiles)
	cLog, err := appFactory(path, "commit", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open commit log: %w", err)

	}

	vLogs := make([]appendable.Appendable, opts.MaxIOConcurrency)
	appendableOpts.WithFileExt("val")
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

	return OpenWith(path, vLogs, txLog, cLog, opts)
}

func OpenWith(path string, vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
	if len(vLogs) == 0 || txLog == nil || cLog == nil {
		return nil, ErrIllegalArguments
	}

	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrIllegalArguments, err)
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	fileSize, ok := metadata.GetInt(metaFileSize)
	if !ok {
		return nil, fmt.Errorf("corrupted commit log metadata (filesize): %w", ErrCorruptedCLog)
	}

	maxTxEntries, ok := metadata.GetInt(metaMaxTxEntries)
	if !ok {
		return nil, fmt.Errorf("corrupted commit log metadata (max tx entries): %w", ErrCorruptedCLog)
	}

	maxKeyLen, ok := metadata.GetInt(metaMaxKeyLen)
	if !ok {
		return nil, fmt.Errorf("corrupted commit log metadata (max key len): %w", ErrCorruptedCLog)
	}

	maxValueLen, ok := metadata.GetInt(metaMaxValueLen)
	if !ok {
		return nil, fmt.Errorf("corrupted commit log metadata (max value len): %w", ErrCorruptedCLog)

	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, fmt.Errorf("corrupted commit log: could not get size: %w", err)
	}

	rem := cLogSize % cLogEntrySize
	if rem > 0 {
		cLogSize -= rem
		err = cLog.SetOffset(cLogSize)
		if err != nil {
			return nil, fmt.Errorf("corrupted commit log: could not set offset: %w", err)
		}
	}

	var committedTxLogSize int64
	var committedTxOffset int64
	var committedTxSize int

	var committedTxID uint64

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)
		_, err := cLog.ReadAt(b, cLogSize-cLogEntrySize)
		if err != nil {
			return nil, fmt.Errorf("corrupted commit log: could not read the last commit: %w", err)
		}

		committedTxOffset = int64(binary.BigEndian.Uint64(b))
		committedTxSize = int(binary.BigEndian.Uint32(b[txIDSize:]))
		committedTxLogSize = committedTxOffset + int64(committedTxSize)
		committedTxID = uint64(cLogSize) / cLogEntrySize

		txLogFileSize, err := txLog.Size()
		if err != nil {
			return nil, fmt.Errorf("corrupted transaction log: could not get size: %w", err)
		}

		if txLogFileSize < committedTxLogSize {
			return nil, fmt.Errorf("corrupted transaction log: size is too small: %w", ErrorCorruptedTxData)
		}
	}

	txPool, err := newTxPool(txPoolOptions{
		poolSize:     opts.MaxConcurrency + 1, // one extra tx pre-allocation for indexing thread
		maxTxEntries: maxTxEntries,
		maxKeyLen:    maxKeyLen,
		preallocated: true,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid configuration, couldn't initialize transaction holder pool")
	}

	maxTxSize := maxTxSize(maxTxEntries, maxKeyLen, maxTxMetadataLen, maxKVMetadataLen)
	txbs := make([]byte, maxTxSize)

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx, _ := txPool.Alloc()

		err = tx.readFrom(txReader)
		if err != nil {
			txPool.Release(tx)
			return nil, fmt.Errorf("corrupted transaction log: could not read the last transaction: %w", err)
		}

		txPool.Release(tx)

		committedAlh = tx.header.Alh()
	}

	vLogsMap := make(map[byte]*refVLog, len(vLogs))
	vLogUnlockedList := list.New()

	for i, vLog := range vLogs {
		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
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

	kvs := make([]*tbtree.KV, maxTxEntries)
	for i := range kvs {
		// vLen + vOff + vHash + txmdLen + txmd + kvmdLen + kvmd
		elen := lszSize + offsetSize + sha256.Size + sszSize + maxTxMetadataLen + sszSize + maxKVMetadataLen
		kvs[i] = &tbtree.KV{K: make([]byte, maxKeyLen), V: make([]byte, elen)}
	}

	var blBuffer chan ([sha256.Size]byte)
	if opts.MaxLinearProofLen > 0 {
		blBuffer = make(chan [sha256.Size]byte, opts.MaxLinearProofLen)
	}

	txLogCache, err := cache.NewLRUCache(opts.TxLogCacheSize) // TODO: optionally it could include up to opts.MaxActiveTransactions upon start
	if err != nil {
		return nil, err
	}

	store := &ImmuStore{
		path:             path,
		logger:           opts.logger,
		txLog:            txLog,
		txLogCache:       txLogCache,
		vLogs:            vLogsMap,
		vLogUnlockedList: vLogUnlockedList,
		vLogsCond:        sync.NewCond(&sync.Mutex{}),

		cLog: cLog,

		cLogBuf: newPrecommitBuffer(opts.MaxActiveTransactions), // TODO: parse and insert pre-committed txs when using external commit allowance

		committedTxID: committedTxID,
		committedAlh:  committedAlh,

		preCommittedTxID:      committedTxID,
		preCommittedAlh:       committedAlh,
		preCommittedTxLogSize: committedTxLogSize,

		readOnly:              opts.ReadOnly,
		synced:                opts.Synced,
		syncFrequency:         opts.SyncFrequency,
		maxActiveTransactions: opts.MaxActiveTransactions,
		maxWaitees:            opts.MaxWaitees,
		maxConcurrency:        opts.MaxConcurrency,
		maxIOConcurrency:      opts.MaxIOConcurrency,
		maxTxEntries:          maxTxEntries,
		maxKeyLen:             maxKeyLen,
		maxValueLen:           maxInt(maxValueLen, opts.MaxValueLen),
		maxLinearProofLen:     opts.MaxLinearProofLen,

		maxTxSize: maxTxSize,

		writeTxHeaderVersion: opts.WriteTxHeaderVersion,

		timeFunc: opts.TimeFunc,

		useExternalCommitAllowance: opts.UseExternalCommitAllowance,
		commitAllowedUpToTxID:      committedTxID,

		aht:      aht,
		blBuffer: blBuffer,

		precommitWHub: watchers.New(0, 1),                                            // syncer (TODO: indexer may wait here instead)
		commitWHub:    watchers.New(0, 1+opts.MaxActiveTransactions+opts.MaxWaitees), // including indexer

		txPool: txPool,
		_kvs:   kvs,
		_txbs:  txbs,

		compactionDisabled: opts.CompactionDisabled,
	}

	if store.aht.Size() > committedTxID {
		err = store.aht.ResetSize(committedTxID)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("corrupted commit log: can not truncate aht tree: %w", err)
		}
	}

	if store.aht.Size() == store.committedTxID {
		store.logger.Infof("Binary Linking up to date at '%s'", store.path)
	} else {
		err = store.syncBinaryLinking()
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("binary linking failed: %w", err)
		}
	}

	if store.blBuffer != nil {
		store.blDone = make(chan struct{})
		go store.binaryLinking()
	}

	err = store.precommitWHub.DoneUpto(committedTxID)
	if err != nil {
		return nil, err
	}

	err = store.commitWHub.DoneUpto(committedTxID)
	if err != nil {
		return nil, err
	}

	indexOpts := tbtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithLogger(opts.logger).
		WithFileSize(fileSize).
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

	err = indexOpts.Validate()
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("%w: invalid index options", err)
	}

	if opts.appFactory != nil {
		indexOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(store.path, filepath.Join(indexDirname, subPath), appOpts)
		})
	}

	indexPath := filepath.Join(store.path, indexDirname)

	store.indexer, err = newIndexer(indexPath, store, indexOpts, opts.MaxWaitees)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("could not open indexer: %w", err)
	}

	if store.indexer.Ts() > committedTxID {
		store.Close()
		return nil, fmt.Errorf("corrupted commit log: index size is too large: %w", ErrCorruptedCLog)

		// TODO: if indexing is done on pre-committed txs, the index may be rollback to a previous snapshot where it was already synced
		// NOTE: compaction should preserve snapshot which are not synced... so to ensure rollback can be achieved
	}

	if store.synced {
		go func() {
			for {
				committedTxID := store.LastCommittedTxID()

				// passive wait for one new transaction at least
				store.precommitWHub.WaitFor(committedTxID+1, nil)

				// TODO: waiting on earlier stages of transaction processing may also be possible
				prevLatestPrecommitedTx := committedTxID + 1

				// TODO: parametrize concurrency evaluation
				for i := 0; i < 4; i++ {
					// give some time for more transactions to be precommitted
					time.Sleep(store.syncFrequency / 4)

					latestPrecommitedTx := store.LastPreCommittedTxID()

					if prevLatestPrecommitedTx == latestPrecommitedTx {
						// avoid waiting if there are no new transactions
						break
					}

					prevLatestPrecommitedTx = latestPrecommitedTx
				}

				// ensure durability
				err := store.sync()
				if err == ErrAlreadyClosed || err == multiapp.ErrAlreadyClosed || err == singleapp.ErrAlreadyClosed {
					return
				}
				if err != nil {
					store.notify(Error, true, "%w: while syncing transactions", err)
				}
			}
		}()
	}

	return store, nil
}

type NotificationType = int

const NotificationWindow = 60 * time.Second
const (
	Info NotificationType = iota
	Warn
	Error
)

func (s *ImmuStore) notify(nType NotificationType, mandatory bool, formattedMessage string, args ...interface{}) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if mandatory || time.Since(s.lastNotification) > NotificationWindow {
		switch nType {
		case Info:
			{
				s.logger.Infof(formattedMessage, args...)
			}
		case Warn:
			{
				s.logger.Warningf(formattedMessage, args...)
			}
		case Error:
			{
				s.logger.Errorf(formattedMessage, args...)
			}
		}
		s.lastNotification = time.Now()
	}
}

func (s *ImmuStore) IndexInfo() uint64 {
	return s.indexer.Ts()
}

func (s *ImmuStore) ExistKeyWith(prefix []byte, neq []byte) (bool, error) {
	return s.indexer.ExistKeyWith(prefix, neq)
}

func (s *ImmuStore) Get(key []byte) (valRef ValueRef, err error) {
	return s.GetWith(key, IgnoreExpired, IgnoreDeleted)
}

func (s *ImmuStore) GetWith(key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
	indexedVal, tx, hc, err := s.indexer.Get(key)
	if err != nil {
		return nil, err
	}

	valRef, err = s.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	for _, filter := range filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, now)
		if err != nil {
			return nil, err
		}
	}

	return valRef, nil
}

func (s *ImmuStore) History(key []byte, offset uint64, descOrder bool, limit int) (txs []uint64, hCount uint64, err error) {
	return s.indexer.History(key, offset, descOrder, limit)
}

func (s *ImmuStore) UseTimeFunc(timeFunc TimeFunc) error {
	if timeFunc == nil {
		return ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.timeFunc = timeFunc

	return nil
}

func (s *ImmuStore) NewTxHolderPool(poolSize int, preallocated bool) (TxPool, error) {
	return newTxPool(txPoolOptions{
		poolSize:     poolSize,
		maxTxEntries: s.maxTxEntries,
		maxKeyLen:    s.maxKeyLen,
		preallocated: preallocated,
	})
}

func (s *ImmuStore) Snapshot() (*Snapshot, error) {
	snap, err := s.indexer.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:   s,
		snap: snap,
		ts:   time.Now(),
	}, nil
}

func (s *ImmuStore) SnapshotSince(tx uint64) (*Snapshot, error) {
	snap, err := s.indexer.SnapshotSince(tx)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:   s,
		snap: snap,
		ts:   time.Now(),
	}, nil
}

func (s *ImmuStore) binaryLinking() {
	for {
		select {
		case alh := <-s.blBuffer:
			{
				_, _, err := s.aht.Append(alh[:])
				if err != nil {
					s.SetBlErr(err)
					s.logger.Errorf("Binary linking at '%s' stopped due to error: %v", s.path, err)
					return
				}
			}
		case <-s.blDone:
			{
				return
			}
		}
	}
}

func (s *ImmuStore) SetBlErr(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.blErr = err
}

func (s *ImmuStore) Alh() (uint64, [sha256.Size]byte) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID, s.committedAlh
}

func (s *ImmuStore) PreAlh() (uint64, [sha256.Size]byte) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.preCommittedTxID, s.preCommittedAlh
}

func (s *ImmuStore) BlInfo() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	alhSize := s.aht.Size()
	committedTxID := s.LastCommittedTxID()

	// only expose fully committed (durable) information
	if alhSize < committedTxID {
		return alhSize, s.blErr
	}

	return committedTxID, s.blErr
}

func (s *ImmuStore) syncBinaryLinking() error {
	s.logger.Infof("Syncing Binary Linking at '%s'...", s.path)

	tx, err := s.fetchAllocTx()
	if err != nil {
		return err
	}
	defer s.releaseAllocTx(tx)

	txReader, err := s.NewTxReader(s.aht.Size()+1, false, tx)
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		alh := tx.header.Alh()
		s.aht.Append(alh[:])

		if tx.header.ID%1000 == 0 {
			s.logger.Infof("Binary linking at '%s' in progress: processing tx: %d", s.path, tx.header.ID)
		}
	}

	s.logger.Infof("Binary Linking up to date at '%s'", s.path)

	return nil
}

func (s *ImmuStore) WaitForTx(txID uint64, cancellation <-chan struct{}) error {
	s.waiteesMutex.Lock()

	if s.waiteesCount == s.maxWaitees {
		s.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	s.waiteesCount++

	s.waiteesMutex.Unlock()

	defer func() {
		s.waiteesMutex.Lock()
		s.waiteesCount--
		s.waiteesMutex.Unlock()
	}()

	err := s.commitWHub.WaitFor(txID, cancellation)
	if err == watchers.ErrAlreadyClosed {
		return ErrAlreadyClosed
	}
	return err
}

func (s *ImmuStore) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	s.waiteesMutex.Lock()

	if s.waiteesCount == s.maxWaitees {
		s.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	s.waiteesCount++

	s.waiteesMutex.Unlock()

	defer func() {
		s.waiteesMutex.Lock()
		s.waiteesCount--
		s.waiteesMutex.Unlock()
	}()

	// note: this wait is only needed if precommitted transactions are indexed
	err := s.commitWHub.WaitFor(txID, cancellation)
	if err == watchers.ErrAlreadyClosed {
		return ErrAlreadyClosed
	}
	if err != nil {
		return err
	}

	return s.indexer.WaitForIndexingUpto(txID, cancellation)
}

func (s *ImmuStore) CompactIndex() error {
	if s.compactionDisabled {
		return ErrCompactionUnsupported
	}
	return s.indexer.CompactIndex()
}

func (s *ImmuStore) FlushIndex(cleanupPercentage float32, synced bool) error {
	return s.indexer.FlushIndex(cleanupPercentage, synced)
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

func (s *ImmuStore) ReadOnly() bool {
	return s.readOnly
}

func (s *ImmuStore) Synced() bool {
	return s.synced
}

func (s *ImmuStore) MaxActiveTransactions() int {
	return s.maxActiveTransactions
}

func (s *ImmuStore) MaxConcurrency() int {
	return s.maxConcurrency
}

func (s *ImmuStore) MaxIOConcurrency() int {
	return s.maxIOConcurrency
}

func (s *ImmuStore) MaxTxEntries() int {
	return s.maxTxEntries
}

func (s *ImmuStore) MaxKeyLen() int {
	return s.maxKeyLen
}

func (s *ImmuStore) MaxValueLen() int {
	return s.maxValueLen
}

func (s *ImmuStore) MaxLinearProofLen() int {
	return s.maxLinearProofLen
}

func (s *ImmuStore) TxCount() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID
}

func (s *ImmuStore) fetchAllocTx() (*Tx, error) {
	tx, err := s.txPool.Alloc()
	if errors.Is(err, ErrTxPoolExhausted) {
		return nil, ErrMaxConcurrencyLimitExceeded
	}
	return tx, nil
}

func (s *ImmuStore) releaseAllocTx(tx *Tx) {
	s.txPool.Release(tx)
}

func encodeOffset(offset int64, vLogID byte) int64 {
	return int64(vLogID)<<56 | offset
}

func decodeOffset(offset int64) (byte, int64) {
	return byte(offset >> 56), offset & ^(0xff << 55)
}

func (s *ImmuStore) fetchAnyVLog() (vLodID byte, vLog appendable.Appendable) {
	s.vLogsCond.L.Lock()
	defer s.vLogsCond.L.Unlock()

	for s.vLogUnlockedList.Len() == 0 {
		s.vLogsCond.Wait()
	}

	vLogID := s.vLogUnlockedList.Remove(s.vLogUnlockedList.Front()).(byte) + 1
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	return vLogID, s.vLogs[vLogID-1].vLog
}

func (s *ImmuStore) fetchVLog(vLogID byte) appendable.Appendable {
	s.vLogsCond.L.Lock()
	defer s.vLogsCond.L.Unlock()

	for s.vLogs[vLogID-1].unlockedRef == nil {
		s.vLogsCond.Wait()
	}

	s.vLogUnlockedList.Remove(s.vLogs[vLogID-1].unlockedRef)
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	return s.vLogs[vLogID-1].vLog
}

func (s *ImmuStore) releaseVLog(vLogID byte) {
	s.vLogsCond.L.Lock()
	s.vLogs[vLogID-1].unlockedRef = s.vLogUnlockedList.PushBack(vLogID - 1) // unlocked
	s.vLogsCond.L.Unlock()
	s.vLogsCond.Signal()
}

type appendableResult struct {
	offsets []int64
	err     error
}

func (s *ImmuStore) appendData(entries []*EntrySpec, donec chan<- appendableResult) {
	offsets := make([]int64, len(entries))

	vLogID, vLog := s.fetchAnyVLog()
	defer s.releaseVLog(vLogID)

	for i := 0; i < len(offsets); i++ {
		if len(entries[i].Value) == 0 {
			continue
		}

		voff, _, err := vLog.Append(entries[i].Value)
		if err != nil {
			donec <- appendableResult{nil, err}
			return
		}
		offsets[i] = encodeOffset(voff, vLogID)
	}

	donec <- appendableResult{offsets, nil}
}

func (s *ImmuStore) NewWriteOnlyTx() (*OngoingTx, error) {
	return newWriteOnlyTx(s)
}

func (s *ImmuStore) NewTx() (*OngoingTx, error) {
	return newReadWriteTx(s)
}

func (s *ImmuStore) commit(otx *OngoingTx, expectedHeader *TxHeader, waitForIndexing bool) (*TxHeader, error) {
	hdr, err := s.precommit(otx, expectedHeader, waitForIndexing)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = s.commitWHub.WaitFor(hdr.ID, nil)
	if err == watchers.ErrAlreadyClosed {
		return hdr, ErrAlreadyClosed
	}
	if err != nil {
		return hdr, err
	}

	if waitForIndexing {
		err = s.WaitForIndexingUpto(hdr.ID, nil)
		if err != nil {
			return hdr, err
		}
	}

	return hdr, nil
}

func (s *ImmuStore) precommit(otx *OngoingTx, expectedHeader *TxHeader, waitForIndexing bool) (*TxHeader, error) {
	if otx == nil {
		return nil, ErrIllegalArguments
	}

	err := s.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = s.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	if otx.hasPreconditions() {
		// First check is performed on the currently committed transaction.
		// It may happen that between now and the commit of this change there are
		// more transactions happening. This check though will prevent acquiring
		// the write lock and putting garbage into appendables if we can already
		// determine that preconditions are not met.

		// A corner case is if the DB fails to meet preconditions now but would fulfill
		// those during final commit phase - but in such case, we are allowed to reason
		// about the DB state in any point in time between both checks thus it is still
		// valid to fail precondition check.

		err = s.WaitForIndexingUpto(s.LastPreCommittedTxID(), nil)
		if err != nil {
			return nil, err
		}

		err = otx.checkPreconditions(s)
		if err != nil {
			return nil, err
		}
	}

	// early check to reduce amount of garbage when tx is not finally committed
	s.mutex.Lock()

	if s.closed {
		s.mutex.Unlock()
		return nil, ErrAlreadyClosed
	}

	if !otx.IsWriteOnly() && otx.snap.Ts() <= s.LastPreCommittedTxID() {
		s.mutex.Unlock()
		return nil, ErrTxReadConflict
	}

	s.mutex.Unlock()

	var version int

	if expectedHeader == nil {
		version = s.writeTxHeaderVersion
	} else {
		version = expectedHeader.Version

		// TxHeader is validated against current store

		currPrecomittedTxID, currPrecommittedAlh := s.PreAlh()

		if currPrecomittedTxID >= expectedHeader.ID {
			return nil, ErrTxAlreadyCommitted
		}

		var blRoot [sha256.Size]byte

		if expectedHeader.BlTxID >= expectedHeader.ID {
			return nil, ErrIllegalArguments
		}

		if expectedHeader.BlTxID > 0 {
			blRoot, err = s.aht.RootAt(expectedHeader.BlTxID)
			if err != nil && err != ahtree.ErrEmptyTree {
				return nil, err
			}
		}

		if otx.metadata != nil {
			if !otx.metadata.Equal(expectedHeader.Metadata) {
				return nil, ErrIllegalArguments
			}
		} else if expectedHeader.Metadata != nil {
			if !expectedHeader.Metadata.Equal(otx.metadata) {
				return nil, ErrIllegalArguments
			}
		}

		if currPrecomittedTxID != expectedHeader.ID-1 ||
			currPrecommittedAlh != expectedHeader.PrevAlh ||
			blRoot != expectedHeader.BlRoot ||
			len(otx.entries) != expectedHeader.NEntries {
			return nil, ErrIllegalArguments
		}
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	appendableCh := make(chan appendableResult)
	go s.appendData(otx.entries, appendableCh)

	tx.header.Version = version
	tx.header.Metadata = otx.metadata

	tx.header.NEntries = len(otx.entries)

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-appendableCh // wait for data to be written
		return nil, err
	}

	// TxHeader is validated against current store
	if expectedHeader != nil && tx.header.Eh != expectedHeader.Eh {
		<-appendableCh // wait for data to be written
		return nil, ErrIllegalArguments
	}

	r := <-appendableCh // wait for data to be written
	err = r.err
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	precommittedTxID := s.LastPreCommittedTxID()

	var ts int64
	var blTxID uint64
	if expectedHeader == nil {
		ts = s.timeFunc().Unix()
		blTxID = s.aht.Size()
	} else {
		ts = expectedHeader.Ts
		blTxID = expectedHeader.BlTxID

		// currTxID and currAlh were already checked before,
		// but we have to add an additional check once the commit mutex
		// is locked to ensure that those constraints are still valid
		// in case of simultaneous writers
		if precommittedTxID != expectedHeader.ID-1 {
			return nil, ErrTxAlreadyCommitted
		}
	}

	if !otx.IsWriteOnly() && otx.snap.Ts() <= precommittedTxID {
		return nil, ErrTxReadConflict
	}

	if otx.hasPreconditions() {
		// Preconditions must be executed with up-to-date tree
		err = s.WaitForIndexingUpto(precommittedTxID, nil)
		if err != nil {
			return nil, err
		}

		err = otx.checkPreconditions(s)
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < tx.header.NEntries; i++ {
		tx.entries[i].vOff = r.offsets[i]
	}

	err = s.performPreCommit(tx, ts, blTxID)
	if err != nil {
		return nil, err
	}

	return tx.Header(), err
}

func (s *ImmuStore) LastPreCommittedTxID() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.preCommittedTxID
}

func (s *ImmuStore) LastCommittedTxID() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID
}

func (s *ImmuStore) performPreCommit(tx *Tx, ts int64, blTxID uint64) error {
	if s.blErr != nil {
		return s.blErr
	}

	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	// limit the maximum number of pre-committed transactions
	if s.synced && s.committedTxID+uint64(s.maxActiveTransactions) <= s.preCommittedTxID {
		return ErrMaxActiveTransactionsLimitExceeded
	}

	// will overwrite partially written and uncommitted data
	err := s.txLog.SetOffset(s.preCommittedTxLogSize)
	if err != nil {
		return fmt.Errorf("commit log: could not set offset: %w", err)
	}

	tx.header.ID = s.preCommittedTxID + 1
	tx.header.Ts = ts

	tx.header.BlTxID = blTxID

	if blTxID > 0 {
		blRoot, err := s.aht.RootAt(blTxID)
		if err != nil && err != ahtree.ErrEmptyTree {
			return err
		}
		tx.header.BlRoot = blRoot
	}

	if tx.header.ID <= tx.header.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.header.PrevAlh = s.preCommittedAlh

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.ID))
	txSize += txIDSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.Ts))
	txSize += tsSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.BlTxID))
	txSize += txIDSize
	copy(s._txbs[txSize:], tx.header.BlRoot[:])
	txSize += sha256.Size
	copy(s._txbs[txSize:], tx.header.PrevAlh[:])
	txSize += sha256.Size

	binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(tx.header.Version))
	txSize += sszSize

	switch tx.header.Version {
	case 0:
		{
			binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(tx.header.NEntries))
			txSize += sszSize
		}
	case 1:
		{
			var txmdbs []byte

			if tx.header.Metadata != nil {
				txmdbs = tx.header.Metadata.Bytes()
			}

			binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(len(txmdbs)))
			txSize += sszSize

			copy(s._txbs[txSize:], txmdbs)
			txSize += len(txmdbs)

			binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(tx.header.NEntries))
			txSize += lszSize
		}
	default:
		{
			panic(fmt.Errorf("missing tx serialization method for version %d", tx.header.Version))
		}
	}

	for i := 0; i < tx.header.NEntries; i++ {
		txe := tx.entries[i]

		// tx serialization using pre-allocated buffer
		// md is stored before key to ensure backward compatibility
		var kvmdbs []byte

		if txe.md != nil {
			kvmdbs = txe.md.Bytes()
		}

		binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(len(kvmdbs)))
		txSize += sszSize
		copy(s._txbs[txSize:], kvmdbs)
		txSize += len(kvmdbs)
		binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(txe.kLen))
		txSize += sszSize
		copy(s._txbs[txSize:], txe.k[:txe.kLen])
		txSize += txe.kLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(txe.vLen))
		txSize += lszSize
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.vOff))
		txSize += offsetSize
		copy(s._txbs[txSize:], txe.hVal[:])
		txSize += sha256.Size
	}

	// tx serialization using pre-allocated buffer
	alh := tx.header.Alh()

	copy(s._txbs[txSize:], alh[:])
	txSize += sha256.Size

	txbs := make([]byte, txSize)
	copy(txbs, s._txbs[:txSize])

	txOff, _, err := s.txLog.Append(txbs)
	if err != nil {
		return err
	}

	_, _, err = s.txLogCache.Put(tx.header.ID, txbs)
	if err != nil {
		return err
	}

	if s.blBuffer == nil {
		err = s.aht.ResetSize(s.preCommittedTxID)
		if err != nil {
			return err
		}
		_, _, err := s.aht.Append(alh[:])
		if err != nil {
			return err
		}
	} else {
		s.blBuffer <- alh
	}

	s.preCommittedTxID++
	s.preCommittedAlh = alh
	s.preCommittedTxLogSize += int64(txSize)

	s.precommitWHub.DoneUpto(s.preCommittedTxID)

	err = s.cLogBuf.put(s.preCommittedTxID, alh, txOff, txSize)
	if err != nil {
		return err
	}

	if !s.synced {
		return s.mayCommit()
	}

	return nil
}

func (s *ImmuStore) EnableExternalCommitAllowance() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.useExternalCommitAllowance = true

	return nil
}

func (s *ImmuStore) AllowCommitUpto(txID uint64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	if !s.useExternalCommitAllowance {
		return fmt.Errorf("%w: the external commit allowance mode is not enabled", ErrIllegalState)
	}

	if txID < s.commitAllowedUpToTxID {
		// once a commit is allowed, it cannot be revoked
		return nil
	}

	if s.preCommittedTxID < txID {
		// commit allowances apply only to pre-committed transactions
		s.commitAllowedUpToTxID = s.preCommittedTxID
	} else {
		s.commitAllowedUpToTxID = txID
	}

	if !s.synced {
		return s.mayCommit()
	}

	return nil
}

func (s *ImmuStore) commitAllowedUpTo() uint64 {
	if !s.useExternalCommitAllowance {
		return s.preCommittedTxID
	}

	return s.commitAllowedUpToTxID
}

func (s *ImmuStore) mayCommit() error {
	commitAllowedUpToTxID := s.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - s.committedTxID)

	if txsCountToBeCommitted > 0 {
		// will overwrite partially written and uncommitted data
		err := s.cLog.SetOffset(int64(s.committedTxID * cLogEntrySize))
		if err != nil {
			return err
		}

		var commitUpToTxID uint64
		var commitUpToTxAlh [sha256.Size]byte

		for i := 0; i < txsCountToBeCommitted; i++ {
			txID, alh, txOff, txSize, err := s.cLogBuf.readAhead(i)
			if err != nil {
				return err
			}

			var cb [cLogEntrySize]byte
			binary.BigEndian.PutUint64(cb[:], uint64(txOff))
			binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

			_, _, err = s.cLog.Append(cb[:])
			if err != nil {
				return err
			}

			commitUpToTxID = txID
			commitUpToTxAlh = alh
		}

		if commitUpToTxID != commitAllowedUpToTxID {
			// added as a safety fuse but this situation should NOT happen
			return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d",
				ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
		}

		err = s.cLog.Flush()
		if err != nil {
			return err
		}

		err = s.cLogBuf.advanceReader(txsCountToBeCommitted)
		if err != nil {
			return err
		}

		s.committedTxID = commitUpToTxID
		s.committedAlh = commitUpToTxAlh

		s.commitWHub.DoneUpto(commitUpToTxID)
	}

	return nil
}

func (s *ImmuStore) CommitWith(callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error), waitForIndexing bool) (*TxHeader, error) {
	hdr, err := s.preCommitWith(callback)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = s.commitWHub.WaitFor(hdr.ID, nil)
	if err == watchers.ErrAlreadyClosed {
		return hdr, ErrAlreadyClosed
	}
	if err != nil {
		return hdr, err
	}

	if waitForIndexing {
		err = s.WaitForIndexingUpto(hdr.ID, nil)
		if err != nil {
			return hdr, err
		}
	}

	return hdr, nil
}

type KeyIndex interface {
	Get(key []byte) (valRef ValueRef, err error)
	GetWith(key []byte, filters ...FilterFn) (valRef ValueRef, err error)
}

type unsafeIndex struct {
	st *ImmuStore
}

func (index *unsafeIndex) Get(key []byte) (ValueRef, error) {
	return index.GetWith(key, IgnoreDeleted)
}

func (index *unsafeIndex) GetWith(key []byte, filters ...FilterFn) (ValueRef, error) {
	return index.st.GetWith(key, filters...)
}

func (s *ImmuStore) preCommitWith(callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error)) (*TxHeader, error) {
	if callback == nil {
		return nil, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	otx, err := s.NewWriteOnlyTx()
	if err != nil {
		return nil, err
	}
	defer otx.Cancel()

	s.indexer.Pause()
	defer s.indexer.Resume()

	LastPreCommittedTxID := s.LastPreCommittedTxID()

	otx.entries, otx.preconditions, err = callback(LastPreCommittedTxID+1, &unsafeIndex{st: s})
	if err != nil {
		return nil, err
	}

	err = s.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = s.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	if otx.hasPreconditions() {
		s.indexer.Resume()

		// Preconditions must be executed with up-to-date tree
		err = s.WaitForIndexingUpto(LastPreCommittedTxID, nil)
		if err != nil {
			return nil, err
		}

		err = otx.checkPreconditions(s)
		if err != nil {
			return nil, err
		}

		s.indexer.Pause()
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	appendableCh := make(chan appendableResult)
	go s.appendData(otx.entries, appendableCh)

	tx.header.Version = s.writeTxHeaderVersion
	tx.header.NEntries = len(otx.entries)

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-appendableCh // wait for data to be written
		return nil, err
	}

	r := <-appendableCh // wait for data to be written
	err = r.err
	if err != nil {
		return nil, err
	}

	for i := 0; i < tx.header.NEntries; i++ {
		tx.entries[i].vOff = r.offsets[i]
	}

	err = s.performPreCommit(tx, s.timeFunc().Unix(), s.aht.Size())
	if err != nil {
		return nil, err
	}

	return tx.Header(), nil
}

type DualProof struct {
	SourceTxHeader     *TxHeader
	TargetTxHeader     *TxHeader
	InclusionProof     [][sha256.Size]byte
	ConsistencyProof   [][sha256.Size]byte
	TargetBlTxAlh      [sha256.Size]byte
	LastInclusionProof [][sha256.Size]byte
	LinearProof        *LinearProof
}

// DualProof combines linear cryptographic linking i.e. transactions include the linear accumulative hash up to the previous one,
// with binary cryptographic linking generated by appending the linear accumulative hash values into an incremental hash tree, whose
// root is also included as part of each transaction and thus considered when calculating the linear accumulative hash.
// The objective of this proof is the same as the linear proof, that is, generate data for the calculation of the accumulative
// hash value of the target transaction from the linear accumulative hash value up to source transaction.
func (s *ImmuStore) DualProof(sourceTxHdr, targetTxHdr *TxHeader) (proof *DualProof, err error) {
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
		binInclusionProof, err := s.aht.InclusionProof(sourceTxHdr.ID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.InclusionProof = binInclusionProof
	}

	if sourceTxHdr.BlTxID > targetTxHdr.BlTxID {
		return nil, fmt.Errorf("%w: binary linking mismatch at tx %d", ErrorCorruptedTxData, sourceTxHdr.ID)
	}

	if sourceTxHdr.BlTxID > 0 {
		// first root sourceTx.BlRoot, second one targetTx.BlRoot
		binConsistencyProof, err := s.aht.ConsistencyProof(sourceTxHdr.BlTxID, targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof = binConsistencyProof
	}

	if targetTxHdr.BlTxID > 0 {
		targetBlTxHdr, err := s.ReadTxHeader(targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTxHdr.Alh()

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := s.aht.InclusionProof(targetTxHdr.BlTxID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.LastInclusionProof = binLastInclusionProof
	}

	lproof, err := s.LinearProof(maxUint64(sourceTxHdr.ID, targetTxHdr.BlTxID), targetTxHdr.ID)
	if err != nil {
		return nil, err
	}
	proof.LinearProof = lproof

	return
}

type LinearProof struct {
	SourceTxID uint64
	TargetTxID uint64
	Terms      [][sha256.Size]byte
}

// LinearProof returns a list of hashes to calculate Alh@targetTxID from Alh@sourceTxID
func (s *ImmuStore) LinearProof(sourceTxID, targetTxID uint64) (*LinearProof, error) {
	if sourceTxID == 0 || sourceTxID > targetTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if s.maxLinearProofLen > 0 && int(targetTxID-sourceTxID+1) > s.maxLinearProofLen {
		return nil, ErrLinearProofMaxLenExceeded
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	r, err := s.NewTxReader(sourceTxID, false, tx)
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

func (s *ImmuStore) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off := (txID - 1) * cLogEntrySize

	var cb [cLogEntrySize]byte

	_, err := s.cLog.ReadAt(cb[:], int64(off))
	if err == multiapp.ErrAlreadyClosed || err == singleapp.ErrAlreadyClosed {
		return 0, 0, ErrAlreadyClosed
	}
	if err == io.EOF {
		// A partially readable commit record must be discarded -
		// - it is a result of incomplete commit log write
		// and will be overwritten on the next commit
		return 0, 0, ErrTxNotFound
	}
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(cb[:]))
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

func (s *ImmuStore) ExportTx(txID uint64, tx *Tx) ([]byte, error) {
	err := s.ReadTx(txID, tx)
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

	valBs := make([]byte, s.maxValueLen)

	for _, e := range tx.Entries() {
		_, err = s.readValueAt(valBs[:e.vLen], e.vOff, e.hVal)
		if err != nil {
			return nil, err
		}

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

		// vLen
		binary.BigEndian.PutUint32(blen[:], uint32(e.vLen))
		_, err = buf.Write(blen[:])
		if err != nil {
			return nil, err
		}

		// val
		_, err = buf.Write(valBs[:e.vLen])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (s *ImmuStore) ReplicateTx(exportedTx []byte, waitForIndexing bool) (*TxHeader, error) {
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

	txSpec, err := s.NewWriteOnlyTx()
	if err != nil {
		return nil, err
	}

	txSpec.metadata = hdr.Metadata

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

		vLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
		i += lszSize

		if len(exportedTx) < i+vLen {
			return nil, ErrIllegalArguments
		}

		err = txSpec.Set(key, md, exportedTx[i:i+vLen])
		if err != nil {
			return nil, err
		}

		i += vLen
	}

	if i != len(exportedTx) {
		return nil, ErrIllegalArguments
	}

	//return s.commit(txSpec, hdr, waitForIndexing)
	return s.precommit(txSpec, hdr, waitForIndexing) //TODO: temporary POC for a non-blocking replicator but it's not waiting durable pre-commit this manner
}

func (s *ImmuStore) FirstTxSince(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := s.LastCommittedTxID()

	for left < right {
		middle := left + (right-left)/2

		header, err := s.ReadTxHeader(middle)
		if err != nil {
			return nil, err
		}

		if header.Ts < ts.Unix() {
			left = middle + 1
		} else {
			right = middle
		}
	}

	header, err := s.ReadTxHeader(left)
	if err != nil {
		return nil, err
	}

	if header.Ts < ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (s *ImmuStore) LastTxUntil(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := s.LastCommittedTxID()

	for left < right {
		middle := left + ((right-left)+1)/2

		header, err := s.ReadTxHeader(middle)
		if err != nil {
			return nil, err
		}

		if header.Ts > ts.Unix() {
			right = middle - 1
		} else {
			left = middle
		}
	}

	header, err := s.ReadTxHeader(left)
	if err != nil {
		return nil, err
	}

	if header.Ts > ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (s *ImmuStore) appendableReaderForTx(txID uint64) (*appendable.Reader, error) {
	cacheMiss := false

	txbs, err := s.txLogCache.Get(txID)
	if err != nil {
		if errors.Is(err, cache.ErrKeyNotFound) {
			cacheMiss = true
		} else {
			return nil, err
		}
	}

	// TODO: it must resolve txOff and txSize for pre-committed txs using cLogBuf instead of cLog
	txOff, txSize, err := s.txOffsetAndSize(txID)
	if err != nil {
		return nil, err
	}

	var txr io.ReaderAt

	if cacheMiss {
		txr = s.txLog
	} else {
		txr = &slicedReaderAt{bs: txbs.([]byte), off: txOff}
	}

	return appendable.NewReaderFrom(txr, txOff, txSize), nil
}

func (s *ImmuStore) ReadTx(txID uint64, tx *Tx) error {
	r, err := s.appendableReaderForTx(txID)
	if err != nil {
		return err
	}

	err = tx.readFrom(r)
	if err == io.EOF {
		return fmt.Errorf("%w: unexpected EOF while reading tx %d", ErrorCorruptedTxData, txID)
	}

	return err
}

func (s *ImmuStore) ReadTxHeader(txID uint64) (*TxHeader, error) {
	r, err := s.appendableReaderForTx(txID)
	if err != nil {
		return nil, err
	}

	tdr := &txDataReader{r: r}

	header, err := tdr.readHeader(s.maxTxEntries)
	if err != nil {
		return nil, err
	}

	e := &TxEntry{k: make([]byte, s.maxKeyLen)}

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

func (s *ImmuStore) ReadTxEntry(txID uint64, key []byte) (*TxEntry, *TxHeader, error) {

	var ret *TxEntry

	r, err := s.appendableReaderForTx(txID)
	if err != nil {
		return nil, nil, err
	}

	tdr := &txDataReader{r: r}

	header, err := tdr.readHeader(s.maxTxEntries)
	if err != nil {
		return nil, nil, err
	}

	e := &TxEntry{k: make([]byte, s.maxKeyLen)}

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
			e = &TxEntry{k: make([]byte, s.maxKeyLen)}
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
func (s *ImmuStore) ReadValue(entry *TxEntry) ([]byte, error) {
	if entry == nil || !entry.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && !entry.md.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && entry.md.ExpiredAt(time.Now()) {
		return nil, ErrExpiredEntry
	}

	b := make([]byte, entry.vLen)

	_, err := s.readValueAt(b, entry.vOff, entry.hVal)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *ImmuStore) readValueAt(b []byte, off int64, hvalue [sha256.Size]byte) (int, error) {
	vLogID, offset := decodeOffset(off)

	if vLogID > 0 {
		vLog := s.fetchVLog(vLogID)
		defer s.releaseVLog(vLogID)

		n, err := vLog.ReadAt(b, offset)
		if err == multiapp.ErrAlreadyClosed || err == singleapp.ErrAlreadyClosed {
			return n, ErrAlreadyClosed
		}
		if err != nil {
			return n, err
		}
	}

	if hvalue != sha256.Sum256(b) {
		return len(b), ErrCorruptedData
	}

	return len(b), nil
}

func (s *ImmuStore) validateEntries(entries []*EntrySpec) error {
	if len(entries) == 0 {
		return ErrorNoEntriesProvided
	}
	if len(entries) > s.maxTxEntries {
		return ErrorMaxTxEntriesLimitExceeded
	}

	m := make(map[string]struct{}, len(entries))

	for _, kv := range entries {
		if kv.Key == nil {
			return ErrNullKey
		}

		if len(kv.Key) > s.maxKeyLen {
			return ErrorMaxKeyLenExceeded
		}
		if len(kv.Value) > s.maxValueLen {
			return ErrorMaxValueLenExceeded
		}

		b64k := base64.StdEncoding.EncodeToString(kv.Key)
		if _, ok := m[b64k]; ok {
			return ErrDuplicatedKey
		}
		m[b64k] = struct{}{}
	}
	return nil
}

func (s *ImmuStore) validatePreconditions(preconditions []Precondition) error {

	if len(preconditions) > s.maxTxEntries {
		return ErrInvalidPreconditionTooMany
	}

	for _, c := range preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}

		err := c.Validate(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ImmuStore) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	return s.sync()
}

func (s *ImmuStore) sync() error {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	if s.preCommittedTxID == s.committedTxID {
		// everything already synced
		return nil
	}

	for i := range s.vLogs {
		vLog := s.fetchVLog(i + 1)
		defer s.releaseVLog(i + 1)

		err := vLog.Flush()
		if err != nil {
			return err
		}

		err = vLog.Sync()
		if err != nil {
			return err
		}
	}

	err := s.txLog.Flush()
	if err != nil {
		return err
	}

	err = s.txLog.Sync()
	if err != nil {
		return err
	}

	commitAllowedUpToTxID := s.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - s.committedTxID)

	// will overwrite partially written and uncommitted data
	err = s.cLog.SetOffset(int64(s.committedTxID * cLogEntrySize))
	if err != nil {
		return err
	}

	var commitUpToTxID uint64
	var commitUpToTxAlh [sha256.Size]byte

	for i := 0; i < txsCountToBeCommitted; i++ {
		txID, alh, txOff, txSize, err := s.cLogBuf.readAhead(i)
		if err != nil {
			return err
		}

		var cb [cLogEntrySize]byte
		binary.BigEndian.PutUint64(cb[:], uint64(txOff))
		binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

		_, _, err = s.cLog.Append(cb[:])
		if err != nil {
			return err
		}

		commitUpToTxID = txID
		commitUpToTxAlh = alh
	}

	if commitUpToTxID != commitAllowedUpToTxID {
		// added as a safety fuse but this situation should NOT happen
		return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d",
			ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	err = s.cLog.Sync()
	if err != nil {
		return err
	}

	err = s.cLogBuf.advanceReader(txsCountToBeCommitted)
	if err != nil {
		return err
	}

	s.committedTxID = commitUpToTxID
	s.committedAlh = commitUpToTxAlh

	s.commitWHub.DoneUpto(commitUpToTxID)

	return nil
}

func (s *ImmuStore) IsClosed() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.closed
}

func (s *ImmuStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.closed = true

	merr := multierr.NewMultiErr()

	for i := range s.vLogs {
		vLog := s.fetchVLog(i + 1)

		err := vLog.Close()
		merr.Append(err)

		s.releaseVLog(i + 1)
	}

	if s.blBuffer != nil && s.blErr == nil && s.blDone != nil {
		s.logger.Infof("Stopping Binary Linking at '%s'...", s.path)
		s.blDone <- struct{}{}
		s.logger.Infof("Binary linking gracefully stopped at '%s'", s.path)
		close(s.blBuffer)
	}

	err := s.precommitWHub.Close()
	merr.Append(err)

	err = s.commitWHub.Close()
	merr.Append(err)

	if s.indexer != nil {
		err = s.indexer.Close()
		merr.Append(err)
	}

	err = s.txLog.Close()
	merr.Append(err)

	err = s.cLog.Close()
	merr.Append(err)

	err = s.aht.Close()
	merr.Append(err)

	used, _, _ := s.txPool.Stats()
	if used > 0 {
		merr.Append(errors.New("not all tx holders were released"))
	}

	return merr.Reduce()
}

func (s *ImmuStore) wrapAppendableErr(err error, action string) error {
	if err == singleapp.ErrAlreadyClosed || err == multiapp.ErrAlreadyClosed {
		s.logger.Warningf("Got '%v' while '%s'", err, action)
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

func maxInt(a, b int) int {
	if a <= b {
		return b
	}
	return a
}

func maxUint64(a, b uint64) uint64 {
	if a <= b {
		return b
	}
	return a
}
