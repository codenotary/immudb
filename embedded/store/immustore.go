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

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"

	"github.com/codenotary/immudb/pkg/logger"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("store already closed")
var ErrUnexpectedLinkingError = errors.New("Internal inconsistency between linear and binary linking")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrNullKey = errors.New("null key")
var ErrorMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrDuplicatedKey = errors.New("duplicated key")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrorCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedData = errors.New("data is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")
var ErrCorruptedAHtree = errors.New("appendable hash tree is corrupted")
var ErrKeyNotFound = tbtree.ErrKeyNotFound
var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrTxNotFound = errors.New("tx not found")
var ErrNoMoreEntries = tbtree.ErrNoMoreEntries
var ErrIllegalState = tbtree.ErrIllegalState
var ErrOffsetOutOfRange = tbtree.ErrOffsetOutOfRange
var ErrUnexpectedError = errors.New("unexpected error")

var ErrSourceTxNewerThanTargetTx = errors.New("source tx is newer than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("max linear proof length limit exceeded")

var ErrCompactionUnsupported = errors.New("comapction is unsupported when remote storage is used")

const MaxKeyLen = 1024 // assumed to be not lower than hash size

const MaxParallelIO = 127

const cLogEntrySize = offsetSize + szSize // tx offset & size

const txIDSize = 8
const tsSize = 8
const szSize = 4
const offsetSize = 8

const linkedLeafSize = txIDSize + tsSize + txIDSize + 3*sha256.Size

const Version = 1

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

	log              logger.Logger
	lastNotification time.Time
	notifyMutex      sync.Mutex

	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	txLog appendable.Appendable
	cLog  appendable.Appendable

	txLogCache *cache.LRUCache

	committedTxID      uint64
	committedAlh       [sha256.Size]byte
	committedTxLogSize int64
	commitStateRWMutex sync.RWMutex

	readOnly          bool
	synced            bool
	maxConcurrency    int
	maxIOConcurrency  int
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	maxLinearProofLen int

	maxTxSize int

	_txs     *list.List // pre-allocated txs
	_txsLock sync.Mutex

	_txbs []byte // pre-allocated buffer to support tx serialization

	_kvs []*tbtree.KV //pre-allocated for indexing

	aht      *ahtree.AHtree
	blBuffer chan ([sha256.Size]byte)
	blErr    error

	wHub *watchers.WatchersHub

	indexer *indexer

	closed bool
	done   chan (struct{})

	mutex sync.Mutex

	compactionDisabled bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

type KV struct {
	Key    []byte
	Value  []byte
	Unique bool
}

func (kv *KV) Digest() [sha256.Size]byte {
	b := make([]byte, len(kv.Key)+sha256.Size)

	copy(b[:], kv.Key)

	hvalue := sha256.Sum256(kv.Value)
	copy(b[len(kv.Key):], hvalue[:])

	return sha256.Sum256(b)
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, opts.FileMode)
			if err != nil {
				return nil, err
			}
		} else {
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
		WithSynced(opts.Synced).
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
		return nil, err
	}

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.CommitLogMaxOpenedFiles)
	cLog, err := appFactory(path, "commit", appendableOpts)
	if err != nil {
		return nil, err
	}

	vLogs := make([]appendable.Appendable, opts.MaxIOConcurrency)
	for i := 0; i < opts.MaxIOConcurrency; i++ {
		appendableOpts.WithSynced(false)
		appendableOpts.WithFileExt("val")
		appendableOpts.WithCompressionFormat(opts.CompressionFormat)
		appendableOpts.WithCompresionLevel(opts.CompressionLevel)
		appendableOpts.WithMaxOpenedFiles(opts.VLogMaxOpenedFiles)
		vLog, err := appFactory(path, fmt.Sprintf("val_%d", i), appendableOpts)
		if err != nil {
			return nil, err
		}
		vLogs[i] = vLog
	}

	return OpenWith(path, vLogs, txLog, cLog, opts)
}

func OpenWith(path string, vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) || len(vLogs) == 0 || txLog == nil || cLog == nil {
		return nil, ErrIllegalArguments
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	fileSize, ok := metadata.GetInt(metaFileSize)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxTxEntries, ok := metadata.GetInt(metaMaxTxEntries)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxKeyLen, ok := metadata.GetInt(metaMaxKeyLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxValueLen, ok := metadata.GetInt(metaMaxValueLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	if cLogSize%cLogEntrySize > 0 {
		return nil, ErrCorruptedCLog
	}

	var committedTxLogSize int64
	var committedTxOffset int64
	var committedTxSize int

	var committedTxID uint64

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)
		_, err := cLog.ReadAt(b, cLogSize-cLogEntrySize)
		if err != nil {
			return nil, err
		}
		committedTxOffset = int64(binary.BigEndian.Uint64(b))
		committedTxSize = int(binary.BigEndian.Uint32(b[txIDSize:]))
		committedTxLogSize = committedTxOffset + int64(committedTxSize)
		committedTxID = uint64(cLogSize) / cLogEntrySize
	}

	txLogFileSize, err := txLog.Size()
	if err != nil {
		return nil, err
	}

	if txLogFileSize < committedTxLogSize {
		return nil, ErrorCorruptedTxData
	}

	maxTxSize := maxTxSize(maxTxEntries, maxKeyLen)

	txs := list.New()

	// one extra tx pre-allocation for indexing thread
	for i := 0; i < opts.MaxConcurrency+1; i++ {
		txs.PushBack(NewTx(maxTxEntries, maxKeyLen))
	}

	txbs := make([]byte, maxTxSize)

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx := txs.Front().Value.(*Tx)
		err = tx.readFrom(txReader)
		if err != nil {
			return nil, err
		}

		committedAlh = tx.Alh
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
		WithSynced(opts.Synced) // built from derived data, but temporarily to reduce chances of data inconsistencies

	if opts.appFactory != nil {
		ahtOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(path, filepath.Join(ahtDirname, subPath), appOpts)
		})
	}

	aht, err := ahtree.Open(ahtPath, ahtOpts)
	if err != nil {
		return nil, err
	}

	kvs := make([]*tbtree.KV, maxTxEntries)
	for i := range kvs {
		kvs[i] = &tbtree.KV{K: make([]byte, maxKeyLen), V: make([]byte, sha256.Size+szSize+offsetSize)}
	}

	var blBuffer chan ([sha256.Size]byte)
	if opts.MaxLinearProofLen > 0 {
		blBuffer = make(chan [sha256.Size]byte, opts.MaxLinearProofLen)
	}

	txLogCache, err := cache.NewLRUCache(opts.TxLogCacheSize)
	if err != nil {
		return nil, err
	}

	store := &ImmuStore{
		path:               path,
		log:                opts.log,
		txLog:              txLog,
		txLogCache:         txLogCache,
		vLogs:              vLogsMap,
		vLogUnlockedList:   vLogUnlockedList,
		vLogsCond:          sync.NewCond(&sync.Mutex{}),
		cLog:               cLog,
		committedTxLogSize: committedTxLogSize,
		committedTxID:      committedTxID,
		committedAlh:       committedAlh,

		readOnly:          opts.ReadOnly,
		synced:            opts.Synced,
		maxConcurrency:    opts.MaxConcurrency,
		maxIOConcurrency:  opts.MaxIOConcurrency,
		maxTxEntries:      maxTxEntries,
		maxKeyLen:         maxKeyLen,
		maxValueLen:       maxInt(maxValueLen, opts.MaxValueLen),
		maxLinearProofLen: opts.MaxLinearProofLen,

		maxTxSize: maxTxSize,

		aht:      aht,
		blBuffer: blBuffer,

		wHub: watchers.New(0, 1+opts.MaxWaitees),

		_kvs:  kvs,
		_txs:  txs,
		_txbs: txbs,

		done: make(chan struct{}),

		compactionDisabled: opts.CompactionDisabled,
	}

	err = store.wHub.DoneUpto(committedTxID)
	if err != nil {
		return nil, err
	}

	indexOpts := tbtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithLog(opts.log).
		WithFileSize(fileSize).
		WithSynced(opts.Synced). // built from derived data, but temporarily modified to reduce chances of data inconsistencies until a better solution is implemented
		WithCacheSize(opts.IndexOpts.CacheSize).
		WithFlushThld(opts.IndexOpts.FlushThld).
		WithMaxActiveSnapshots(opts.IndexOpts.MaxActiveSnapshots).
		WithMaxNodeSize(opts.IndexOpts.MaxNodeSize).
		WithRenewSnapRootAfter(opts.IndexOpts.RenewSnapRootAfter).
		WithCompactionThld(opts.IndexOpts.CompactionThld).
		WithDelayDuringCompaction(opts.IndexOpts.DelayDuringCompaction)

	if opts.appFactory != nil {
		indexOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(store.path, filepath.Join(indexDirname, subPath), appOpts)
		})
	}

	indexPath := filepath.Join(store.path, indexDirname)

	store.indexer, err = newIndexer(indexPath, store, indexOpts, opts.MaxWaitees)
	if err != nil {
		return nil, err
	}

	if store.aht.Size() > store.committedTxID || store.indexer.Ts() > store.committedTxID {
		store.Close()
		return nil, ErrCorruptedCLog
	}

	err = store.syncBinaryLinking()
	if err != nil {
		store.Close()
		return nil, err
	}

	if store.blBuffer != nil {
		go store.binaryLinking()
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
				s.log.Infof(formattedMessage, args...)
			}
		case Warn:
			{
				s.log.Warningf(formattedMessage, args...)
			}
		case Error:
			{
				s.log.Errorf(formattedMessage, args...)
			}
		}
		s.lastNotification = time.Now()
	}
}

func (s *ImmuStore) IndexInfo() uint64 {
	return s.indexer.Ts()
}

func (s *ImmuStore) ExistKeyWith(prefix []byte, neq []byte, smaller bool) (bool, error) {
	return s.indexer.ExistKeyWith(prefix, neq, smaller)
}

func (s *ImmuStore) Get(key []byte) (value []byte, tx uint64, hc uint64, err error) {
	indexedVal, tx, hc, err := s.indexer.Get(key)
	if err != nil {
		return nil, 0, 0, err
	}

	valRef, err := s.valueRefFrom(indexedVal)
	if err != nil {
		return nil, 0, 0, err
	}

	val, err := valRef.Resolve()
	if err != nil {
		return nil, 0, 0, err
	}

	return val, tx, hc, err
}

func (s *ImmuStore) History(key []byte, offset uint64, descOrder bool, limit int) (txs []uint64, err error) {
	return s.indexer.History(key, offset, descOrder, limit)
}

func (s *ImmuStore) NewTx() *Tx {
	return NewTx(s.maxTxEntries, s.maxKeyLen)
}

func (s *ImmuStore) Snapshot() (*Snapshot, error) {
	snap, err := s.indexer.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:   s,
		snap: snap,
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
					s.log.Errorf("Binary linking at '%s' stopped due to error: %v", s.path, err)
					return
				}
			}
		case <-s.done:
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
	txID, txAlh, _ := s.commitState()
	return txID, txAlh
}

func (s *ImmuStore) BlInfo() (uint64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.aht.Size(), s.blErr
}

func (s *ImmuStore) syncBinaryLinking() error {
	if s.aht.Size() == s.committedTxID {
		s.log.Infof("Binary Linking up to date at '%s'", s.path)
		return nil
	}

	s.log.Infof("Syncing Binary Linking at '%s'...", s.path)

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

		alh := tx.Alh
		s.aht.Append(alh[:])
	}

	s.log.Infof("Binary Linking up to date at '%s'", s.path)

	return nil
}

func (s *ImmuStore) WaitForIndexingUpto(txID uint64, cancellation <-chan struct{}) error {
	return s.indexer.WaitForIndexingUpto(txID, cancellation)
}

func (s *ImmuStore) CompactIndex() error {
	if s.compactionDisabled {
		return ErrCompactionUnsupported
	}
	return s.indexer.CompactIndex()
}

func maxTxSize(maxTxEntries, maxKeyLen int) int {
	return txIDSize /*txID*/ +
		tsSize /*ts*/ +
		txIDSize /*blTxID*/ +
		sha256.Size /*blRoot*/ +
		sha256.Size /*prevAlh*/ +
		szSize /*|entries|*/ +
		maxTxEntries*(szSize /*kLen*/ +maxKeyLen /*key*/ +szSize /*vLen*/ +offsetSize /*vOff*/ +sha256.Size /*hValue*/) +
		sha256.Size /*eH*/ +
		sha256.Size /*txH*/
}

func (s *ImmuStore) ReadOnly() bool {
	return s.readOnly
}

func (s *ImmuStore) Synced() bool {
	return s.synced
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
	committedTxID, _, _ := s.commitState()
	return committedTxID
}

func (s *ImmuStore) fetchAllocTx() (*Tx, error) {
	s._txsLock.Lock()
	defer s._txsLock.Unlock()

	if s._txs.Len() == 0 {
		return nil, ErrMaxConcurrencyLimitExceeded
	}

	return s._txs.Remove(s._txs.Front()).(*Tx), nil
}

func (s *ImmuStore) releaseAllocTx(tx *Tx) {
	s._txsLock.Lock()
	defer s._txsLock.Unlock()

	s._txs.PushBack(tx)
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

func (s *ImmuStore) fetchVLog(vLogID byte, checkClosed bool) (vLog appendable.Appendable, err error) {
	s.vLogsCond.L.Lock()
	defer s.vLogsCond.L.Unlock()

	for s.vLogs[vLogID-1].unlockedRef == nil {
		if checkClosed {
			s.mutex.Lock()
			if s.closed {
				err = ErrAlreadyClosed
			}
			s.mutex.Unlock()
		}

		if err != nil {
			return nil, err
		}

		s.vLogsCond.Wait()
	}

	s.vLogUnlockedList.Remove(s.vLogs[vLogID-1].unlockedRef)
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	return s.vLogs[vLogID-1].vLog, nil
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

func (s *ImmuStore) appendData(entries []*KV, donec chan<- appendableResult) {
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

	err := vLog.Flush()
	if err != nil {
		donec <- appendableResult{nil, err}
		return
	}

	if s.synced {
		err = vLog.Sync()
		if err != nil {
			donec <- appendableResult{nil, err}
			return
		}
	}

	donec <- appendableResult{offsets, nil}
}

func (s *ImmuStore) Commit(entries []*KV, waitForIndexing bool) (*TxMetadata, error) {
	return s.commitAt(entries, waitForIndexing, time.Now().Unix(), s.aht.Size())
}

func (s *ImmuStore) commitAt(entries []*KV, waitForIndexing bool, ts int64, blTxID uint64) (*TxMetadata, error) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil, ErrAlreadyClosed
	}
	s.mutex.Unlock()

	err := s.validateEntries(entries)
	if err != nil {
		return nil, err
	}

	appendableCh := make(chan appendableResult)
	go s.appendData(entries, appendableCh)

	tx, err := s.fetchAllocTx()
	if err != nil {
		<-appendableCh // wait for data to be written
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	tx.nentries = len(entries)

	for i, e := range entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
		txe.unique = e.Unique
	}

	tx.BuildHashTree()

	r := <-appendableCh // wait for data to be written
	err = r.err
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()

	if s.closed {
		s.mutex.Unlock()
		return nil, ErrAlreadyClosed
	}

	err = s.commit(tx, r.offsets, ts, blTxID)
	if err != nil {
		s.mutex.Unlock()
		return nil, err
	}

	s.mutex.Unlock()

	if waitForIndexing {
		err = s.WaitForIndexingUpto(tx.ID, nil)
		if err != nil {
			return tx.Metadata(), err
		}
	}

	return tx.Metadata(), nil
}

func (s *ImmuStore) commit(tx *Tx, offsets []int64, ts int64, blTxID uint64) error {
	if s.blErr != nil {
		return s.blErr
	}

	// will overwrite partially written and uncommitted data
	committedTxID, committedAlh, committedTxLogSize := s.commitState()

	s.txLog.SetOffset(committedTxLogSize)

	tx.ID = committedTxID + 1
	tx.Ts = ts

	tx.BlTxID = blTxID

	if blTxID > 0 {
		blRoot, err := s.aht.RootAt(blTxID)
		if err != nil && err != ahtree.ErrEmptyTree {
			return err
		}
		tx.BlRoot = blRoot
	}

	if tx.ID <= tx.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.PrevAlh = committedAlh

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.ID))
	txSize += txIDSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.Ts))
	txSize += tsSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.BlTxID))
	txSize += txIDSize
	copy(s._txbs[txSize:], tx.BlRoot[:])
	txSize += sha256.Size
	copy(s._txbs[txSize:], tx.PrevAlh[:])
	txSize += sha256.Size
	binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(tx.nentries))
	txSize += szSize

	for i := 0; i < tx.nentries; i++ {
		txe := tx.entries[i]
		txe.vOff = offsets[i]

		if txe.unique {
			if tx.ID > 1 {
				err := s.WaitForIndexingUpto(tx.ID-1, nil)
				if err != nil {
					return err
				}

				_, _, _, err = s.indexer.Get(txe.Key())
				if err == nil {
					return ErrKeyAlreadyExists
				}
				if err != ErrKeyNotFound {
					return err
				}
			}
		}

		// tx serialization using pre-allocated buffer
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(txe.kLen))
		txSize += szSize
		copy(s._txbs[txSize:], txe.k[:txe.kLen])
		txSize += txe.kLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(txe.vLen))
		txSize += szSize
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.vOff))
		txSize += offsetSize
		copy(s._txbs[txSize:], txe.hVal[:])
		txSize += sha256.Size
	}

	tx.CalcAlh()

	// tx serialization using pre-allocated buffer
	copy(s._txbs[txSize:], tx.Alh[:])
	txSize += sha256.Size

	txbs := make([]byte, txSize)
	copy(txbs, s._txbs[:txSize])

	txOff, _, err := s.txLog.Append(txbs)
	if err != nil {
		return err
	}

	_, _, err = s.txLogCache.Put(tx.ID, txbs)
	if err != nil {
		return err
	}

	err = s.txLog.Flush()
	if err != nil {
		return err
	}

	if s.blBuffer == nil {
		_, _, err := s.aht.Append(tx.Alh[:])
		if err != nil {
			return err
		}
	} else {
		s.blBuffer <- tx.Alh
	}

	var cb [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cb[:], uint64(txOff))
	binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))
	_, _, err = s.cLog.Append(cb[:])
	if err != nil {
		return err
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	committedTxID = s.advanceCommitState(tx.Alh, int64(txSize))
	s.wHub.DoneUpto(committedTxID)

	return nil
}

func (s *ImmuStore) advanceCommitState(txAlh [sha256.Size]byte, txSize int64) uint64 {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	s.committedTxID++
	s.committedAlh = txAlh
	s.committedTxLogSize += txSize

	return s.committedTxID
}

func (s *ImmuStore) commitState() (txID uint64, txAlh [sha256.Size]byte, clogSize int64) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID, s.committedAlh, s.committedTxLogSize
}

func (s *ImmuStore) CommitWith(callback func(txID uint64, index KeyIndex) ([]*KV, error), waitForIndexing bool) (*TxMetadata, error) {
	md, err := s.commitWith(callback)
	if err != nil {
		return nil, err
	}

	if waitForIndexing {
		err = s.WaitForIndexingUpto(md.ID, nil)
		if err != nil {
			return md, err
		}
	}

	return md, err
}

type KeyIndex interface {
	Get(key []byte) (value []byte, tx uint64, hc uint64, err error)
}

type unsafeIndex struct {
	st *ImmuStore
}

func (index *unsafeIndex) Get(key []byte) (value []byte, tx uint64, hc uint64, err error) {
	return index.st.Get(key)
}

func (s *ImmuStore) commitWith(callback func(txID uint64, index KeyIndex) ([]*KV, error)) (*TxMetadata, error) {
	if callback == nil {
		return nil, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	s.indexer.Pause()
	defer s.indexer.Resume()

	committedTxID, _, _ := s.commitState()
	txID := committedTxID + 1

	entries, err := callback(txID, &unsafeIndex{st: s})
	if err != nil {
		return nil, err
	}

	err = s.validateEntries(entries)
	if err != nil {
		return nil, err
	}

	appendableCh := make(chan appendableResult)
	go s.appendData(entries, appendableCh)

	tx, err := s.fetchAllocTx()
	if err != nil {
		<-appendableCh // wait for data to be writen
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	tx.nentries = len(entries)

	for i, e := range entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
		txe.unique = e.Unique
	}

	tx.BuildHashTree()

	r := <-appendableCh // wait for data to be writen
	err = r.err
	if err != nil {
		return nil, err
	}

	err = s.commit(tx, r.offsets, time.Now().Unix(), s.aht.Size())
	if err != nil {
		return nil, err
	}

	return tx.Metadata(), nil
}

type DualProof struct {
	SourceTxMetadata   *TxMetadata
	TargetTxMetadata   *TxMetadata
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
func (s *ImmuStore) DualProof(sourceTx, targetTx *Tx) (proof *DualProof, err error) {
	if sourceTx == nil || targetTx == nil {
		return nil, ErrIllegalArguments
	}

	if sourceTx.ID > targetTx.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	proof = &DualProof{
		SourceTxMetadata: sourceTx.Metadata(),
		TargetTxMetadata: targetTx.Metadata(),
	}

	if sourceTx.ID < targetTx.BlTxID {
		binInclusionProof, err := s.aht.InclusionProof(sourceTx.ID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.InclusionProof = binInclusionProof
	}

	if sourceTx.BlTxID > targetTx.BlTxID {
		return nil, ErrorCorruptedTxData
	}

	if sourceTx.BlTxID > 0 {
		binConsistencyProof, err := s.aht.ConsistencyProof(sourceTx.BlTxID, targetTx.BlTxID) // first root sourceTx.BlRoot, second one targetTx.BlRoot
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof = binConsistencyProof
	}

	var targetBlTx *Tx

	if targetTx.BlTxID > 0 {
		targetBlTx, err = s.fetchAllocTx()
		if err != nil {
			return nil, err
		}

		err = s.ReadTx(targetTx.BlTxID, targetBlTx)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTx.Alh

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := s.aht.InclusionProof(targetTx.BlTxID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.LastInclusionProof = binLastInclusionProof
	}

	if targetBlTx != nil {
		s.releaseAllocTx(targetBlTx)
	}

	lproof, err := s.LinearProof(maxUint64(sourceTx.ID, targetTx.BlTxID), targetTx.ID)
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

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	proof := make([][sha256.Size]byte, targetTxID-sourceTxID+1)
	proof[0] = tx.Alh

	for i := 1; i < len(proof); i++ {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		proof[i] = tx.InnerHash
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

	n, err := s.cLog.ReadAt(cb[:], int64(off))
	if err == multiapp.ErrAlreadyClosed || err == singleapp.ErrAlreadyClosed {
		return 0, 0, ErrAlreadyClosed
	}
	if err == io.EOF && n == 0 {
		return 0, 0, ErrTxNotFound
	}
	if err == io.EOF && n > 0 {
		return 0, n, ErrCorruptedCLog
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

	mdBs := tx.Metadata().serialize()

	var buf bytes.Buffer

	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(mdBs)))
	_, err = buf.Write(b[:])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(mdBs)
	if err != nil {
		return nil, err
	}

	valBs := make([]byte, s.maxValueLen)

	for _, e := range tx.Entries() {
		_, err = s.ReadValueAt(valBs[:e.vLen], e.vOff, e.hVal)
		if err != nil {
			return nil, err
		}

		var lenBs [4]byte

		// kLen
		binary.BigEndian.PutUint32(lenBs[:], uint32(e.kLen))
		_, err = buf.Write(lenBs[:])
		if err != nil {
			return nil, err
		}

		// vLen
		binary.BigEndian.PutUint32(lenBs[:], uint32(e.vLen))
		_, err = buf.Write(lenBs[:])
		if err != nil {
			return nil, err
		}

		// key
		_, err = buf.Write(e.Key())
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

func (s *ImmuStore) ReplicateTx(exportedTx []byte) (*TxMetadata, error) {
	if len(exportedTx) < 4 {
		return nil, ErrIllegalArguments
	}

	i := 0

	mdLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
	i += 4

	if len(exportedTx[i:]) < mdLen {
		return nil, ErrIllegalArguments
	}

	md := &TxMetadata{}
	err := md.readFrom(exportedTx[i : i+mdLen])
	if err != nil {
		return nil, err
	}
	i += mdLen

	entries := make([]*KV, md.NEntries)

	for ei := range entries {
		if len(exportedTx[i:]) < 8 {
			return nil, ErrIllegalArguments
		}

		kLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
		i += 4

		vLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
		i += 4

		if len(exportedTx[i:]) < kLen+vLen {
			return nil, ErrIllegalArguments
		}

		entries[ei] = &KV{
			Key:   exportedTx[i : i+kLen],
			Value: exportedTx[i+kLen : i+kLen+vLen],
		}

		i += kLen + vLen
	}

	if i != len(exportedTx) {
		return nil, ErrIllegalArguments
	}

	return s.replicateCommit(md, entries)
}

func (s *ImmuStore) replicateCommit(md *TxMetadata, entries []*KV) (*TxMetadata, error) {
	//TODO: validate metadata and entry hashes against metadata ...

	return s.commitAt(entries, false, md.Ts, md.BlTxID)
}

func (s *ImmuStore) ReadTx(txID uint64, tx *Tx) error {
	cacheMiss := false

	txbs, err := s.txLogCache.Get(txID)
	if err != nil && err != cache.ErrKeyNotFound {
		return err
	}
	if err == cache.ErrKeyNotFound {
		cacheMiss = true
	}

	txOff, txSize, err := s.txOffsetAndSize(txID)
	if err != nil {
		return err
	}

	var txr io.ReaderAt

	if cacheMiss {
		txr = s.txLog
	} else {
		txr = &slicedReaderAt{bs: txbs.([]byte), off: txOff}
	}

	r := appendable.NewReaderFrom(txr, txOff, txSize)

	err = tx.readFrom(r)
	if err == io.EOF {
		return ErrorCorruptedTxData
	}

	return err
}

func (s *ImmuStore) ReadValue(tx *Tx, key []byte) ([]byte, error) {
	for _, e := range tx.Entries() {
		if bytes.Equal(e.key(), key) {
			v := make([]byte, e.vLen)
			_, err := s.ReadValueAt(v, e.vOff, e.hVal)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (s *ImmuStore) ReadValueAt(b []byte, off int64, hvalue [sha256.Size]byte) (int, error) {
	vLogID, offset := decodeOffset(off)

	if vLogID > 0 {
		vLog, err := s.fetchVLog(vLogID, true)
		if err != nil {
			return 0, err
		}
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

func (s *ImmuStore) validateEntries(entries []*KV) error {
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

func (s *ImmuStore) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	for i := range s.vLogs {
		vLog, _ := s.fetchVLog(i+1, false)
		defer s.releaseVLog(i + 1)

		err := vLog.Sync()
		if err != nil {
			return err
		}
	}

	err := s.txLog.Sync()
	if err != nil {
		return err
	}

	err = s.cLog.Sync()
	if err != nil {
		return err
	}

	return s.indexer.Sync()
}

func (s *ImmuStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.closed = true

	errors := make([]error, 0)

	for i := range s.vLogs {
		vLog, _ := s.fetchVLog(i+1, false)

		err := vLog.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	s.vLogsCond.Broadcast()

	if s.blBuffer != nil && s.blErr == nil {
		s.log.Infof("Stopping Binary Linking at '%s'...", s.path)
		s.done <- struct{}{}
		s.log.Infof("Binary linking gracefully stopped at '%s'", s.path)
		close(s.blBuffer)
	}

	s.wHub.Close()

	iErr := s.indexer.Close()
	if iErr != nil {
		errors = append(errors, iErr)
	}

	txErr := s.txLog.Close()
	if txErr != nil {
		errors = append(errors, txErr)
	}

	cErr := s.cLog.Close()
	if cErr != nil {
		errors = append(errors, cErr)
	}

	tErr := s.aht.Close()
	if tErr != nil {
		errors = append(errors, tErr)
	}

	if len(errors) > 0 {
		return &multierr.MultiErr{Errors: errors}
	}

	return nil
}

func (s *ImmuStore) wrapAppendableErr(err error, action string) error {
	if err == singleapp.ErrAlreadyClosed || err == multiapp.ErrAlreadyClosed {
		s.log.Warningf("Got '%v' while '%s'", err, action)
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
