/*
Copyright 2019-2020 vChain, Inc.

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

	"codenotary.io/immudb-v2/ahtree"
	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/cbuffer"
	"codenotary.io/immudb-v2/multierr"
	"codenotary.io/immudb-v2/tbtree"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrUnexpectedLinkingError = errors.New("Internal inconsistency between linear and binary linking")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrNullKeyOrValue = errors.New("null key or value")
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
var ErrKeyNotFound = errors.New("key not found")

var ErrSourceTxNewerThanTargetTx = errors.New("source tx is newer than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("max linear proof length limit exceeded")

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

type ImmuStore struct {
	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	txLog appendable.Appendable
	cLog  appendable.Appendable

	committedTxID      uint64
	committedAlh       [sha256.Size]byte
	committedTxLogSize int64

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

	blBuffer *cbuffer.CHBuffer
	blErr    error
	blMutex  sync.Mutex
	aht      *ahtree.AHtree

	index        *tbtree.TBtree
	indexerErr   error
	indexerMutex sync.Mutex

	mutex sync.Mutex

	closed bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

type KV struct {
	Key   []byte
	Value []byte
}

func (kv *KV) Digest() [sha256.Size]byte {
	b := make([]byte, 1+len(kv.Key)+sha256.Size)

	b[0] = LeafPrefix

	copy(b[1:], kv.Key)

	hvalue := sha256.Sum256(kv.Value)
	copy(b[1+len(kv.Key):], hvalue[:])

	return sha256.Sum256(b)
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, opts.fileMode)
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
	metadata.PutInt(metaMaxTxEntries, opts.maxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.maxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.maxValueLen)
	metadata.PutInt(metaFileSize, opts.fileSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithSynced(opts.synced).
		WithFileSize(opts.fileSize).
		WithFileMode(opts.fileMode).
		WithMetadata(metadata.Bytes())

	vLogs := make([]appendable.Appendable, opts.maxIOConcurrency)
	for i := 0; i < opts.maxIOConcurrency; i++ {
		appendableOpts.WithFileExt("val")
		appendableOpts.WithCompressionFormat(opts.compressionFormat)
		appendableOpts.WithCompresionLevel(opts.compressionLevel)
		appendableOpts.WithMaxOpenedFiles(opts.vLogMaxOpenedFiles)
		vLogPath := filepath.Join(path, fmt.Sprintf("val_%d", i))
		vLog, err := multiapp.Open(vLogPath, appendableOpts)
		if err != nil {
			return nil, err
		}
		vLogs[i] = vLog
	}

	appendableOpts.WithFileExt("tx")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.txLogMaxOpenedFiles)
	txLogPath := filepath.Join(path, "tx")
	txLog, err := multiapp.Open(txLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.commitLogMaxOpenedFiles)
	cLogPath := filepath.Join(path, "commit")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return nil, err
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

	for i := 0; i < opts.maxConcurrency; i++ {
		tx := newTx(maxTxEntries, maxKeyLen)
		txs.PushBack(tx)
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

		committedAlh = tx.Alh()
	}

	vLogsMap := make(map[byte]*refVLog, len(vLogs))
	vLogUnlockedList := list.New()

	for i, vLog := range vLogs {
		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
	}

	indexPath := filepath.Join(path, "index")

	indexOpts := tbtree.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithFileMode(opts.fileMode).
		WithFileSize(fileSize).
		WithSynced(false). // index is built from derived data
		WithCacheSize(opts.indexOpts.cacheSize).
		WithFlushThld(opts.indexOpts.flushThld).
		WithMaxActiveSnapshots(opts.indexOpts.maxActiveSnapshots).
		WithMaxNodeSize(opts.indexOpts.maxNodeSize).
		WithRenewSnapRootAfter(opts.indexOpts.renewSnapRootAfter)

	index, err := tbtree.Open(indexPath, indexOpts)
	if err != nil {
		return nil, err
	}

	ahtPath := filepath.Join(path, "aht")

	ahtOpts := ahtree.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithFileMode(opts.fileMode).
		WithFileSize(fileSize).
		WithSynced(false) // built from derived data

	aht, err := ahtree.Open(ahtPath, ahtOpts)
	if err != nil {
		return nil, err
	}

	kvs := make([]*tbtree.KV, maxTxEntries)
	for i := range kvs {
		kvs[i] = &tbtree.KV{K: make([]byte, maxKeyLen), V: make([]byte, sha256.Size+szSize+offsetSize)}
	}

	var blBuffer *cbuffer.CHBuffer
	if opts.maxLinearProofLen > 0 {
		blBuffer = cbuffer.New(opts.maxLinearProofLen)
	}

	store := &ImmuStore{
		txLog:              txLog,
		vLogs:              vLogsMap,
		vLogUnlockedList:   vLogUnlockedList,
		vLogsCond:          sync.NewCond(&sync.Mutex{}),
		cLog:               cLog,
		committedTxLogSize: committedTxLogSize,
		committedTxID:      committedTxID,
		committedAlh:       committedAlh,

		readOnly:          opts.readOnly,
		synced:            opts.synced,
		maxConcurrency:    opts.maxConcurrency,
		maxIOConcurrency:  opts.maxIOConcurrency,
		maxTxEntries:      maxTxEntries,
		maxKeyLen:         maxKeyLen,
		maxValueLen:       maxValueLen,
		maxLinearProofLen: opts.maxLinearProofLen,

		maxTxSize: maxTxSize,

		index:    index,
		blBuffer: blBuffer,
		aht:      aht,

		_kvs:  kvs,
		_txs:  txs,
		_txbs: txbs,
	}

	err = store.syncBinaryLinking()
	if err != nil {
		return nil, err
	}

	if store.blBuffer != nil {
		go store.binaryLinking()
	}

	go store.indexer()

	return store, nil
}

func (s *ImmuStore) NewTx() *Tx {
	return newTx(s.maxTxEntries, s.maxKeyLen)
}

func (s *ImmuStore) Snapshot() (*tbtree.Snapshot, error) {
	return s.index.Snapshot()
}

func (s *ImmuStore) binaryLinking() {
	for {
		alh, err := s.blBuffer.Get()

		if err == cbuffer.ErrBufferIsEmpty {
			time.Sleep(time.Duration(10) * time.Millisecond)
			continue
		}

		if err == nil {
			_, _, err = s.aht.Append(alh[:])
			if err == ErrAlreadyClosed {
				return
			}
		}

		if err != nil {
			s.SetBlErr(err)
			return
		}
	}
}

func (s *ImmuStore) SetBlErr(err error) {
	s.blMutex.Lock()
	defer s.blMutex.Unlock()

	s.blErr = err
}

func (s *ImmuStore) BlInfo() (uint64, error) {
	s.blMutex.Lock()
	defer s.blMutex.Unlock()

	return s.aht.Size(), s.blErr
}

func (s *ImmuStore) syncBinaryLinking() error {
	if s.aht.Size() > s.committedTxID {
		return ErrUnexpectedLinkingError
	}

	if s.aht.Size() == s.committedTxID {
		return nil
	}

	txReader, err := s.NewTxReader(s.aht.Size()+1, s.maxTxSize)
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		alh := tx.Alh()
		s.aht.Append(alh[:])
	}

	return nil
}

func (s *ImmuStore) indexer() {
	for {
		time.Sleep(time.Duration(100) * time.Millisecond) // TODO: use sync for waking up (ideally, would index from rotated file)

		err := s.doIndexing()

		if err != nil && err != io.EOF {
			if err != ErrAlreadyClosed {
				s.indexerMutex.Lock()
				s.indexerErr = err
				s.indexerMutex.Unlock()
			}
			return
		}
	}
}

func (s *ImmuStore) IndexInfo() (uint64, error) {
	s.indexerMutex.Lock()
	defer s.indexerMutex.Unlock()

	return s.index.Ts(), s.indexerErr
}

func (s *ImmuStore) doIndexing() error {
	txID := s.index.Ts() + 1

	txReader, err := s.NewTxReader(txID, s.maxTxSize)
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		txEntries := tx.Entries()

		for i, e := range txEntries {
			var b [szSize + offsetSize + sha256.Size]byte
			binary.BigEndian.PutUint32(b[:], uint32(e.ValueLen))
			binary.BigEndian.PutUint64(b[szSize:], uint64(e.VOff))
			copy(b[szSize+offsetSize:], e.HValue[:])

			s._kvs[i].K = e.Key()
			s._kvs[i].V = b[:]
		}

		err = s.index.BulkInsert(s._kvs[:len(txEntries)])
		if err != nil {
			return err
		}
	}

	return err
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.committedTxID
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

func (s *ImmuStore) fetchAnyVLog() (vLodID byte, vLog appendable.Appendable, err error) {
	s.vLogsCond.L.Lock()

	for s.vLogUnlockedList.Len() == 0 {
		s.mutex.Lock()
		if s.closed {
			err = ErrAlreadyClosed
		}
		s.mutex.Unlock()

		if err != nil {
			return 0, nil, err
		}

		s.vLogsCond.Wait()
	}

	vLogID := s.vLogUnlockedList.Remove(s.vLogUnlockedList.Front()).(byte)

	s.vLogs[vLogID].unlockedRef = nil // locked

	s.vLogsCond.L.Unlock()

	return vLogID, s.vLogs[vLogID].vLog, nil
}

func (s *ImmuStore) fetchVLog(vLogID byte, checkClosed bool) (vLog appendable.Appendable, err error) {
	s.vLogsCond.L.Lock()

	for s.vLogs[vLogID].unlockedRef == nil {
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

	s.vLogUnlockedList.Remove(s.vLogs[vLogID].unlockedRef)
	s.vLogs[vLogID].unlockedRef = nil // locked

	s.vLogsCond.L.Unlock()

	return s.vLogs[vLogID].vLog, nil
}

func (s *ImmuStore) releaseVLog(vLogID byte) {
	s.vLogsCond.L.Lock()
	s.vLogs[vLogID].unlockedRef = s.vLogUnlockedList.PushBack(vLogID) // unlocked
	s.vLogsCond.L.Unlock()
	s.vLogsCond.Signal()
}

type appendableResult struct {
	offsets []int64
	err     error
}

func (s *ImmuStore) appendData(entries []*KV, donec chan<- appendableResult) {
	offsets := make([]int64, len(entries))

	vLogID, vLog, err := s.fetchAnyVLog()
	if err != nil {
		donec <- appendableResult{nil, err}
		return
	}

	defer s.releaseVLog(vLogID)

	for i := 0; i < len(offsets); i++ {
		voff, _, err := vLog.Append(entries[i].Value)
		if err != nil {
			donec <- appendableResult{nil, err}
			return
		}
		offsets[i] = encodeOffset(voff, vLogID)
	}

	err = vLog.Flush()
	if err != nil {
		donec <- appendableResult{nil, err}
	}

	donec <- appendableResult{offsets, nil}
}

func (s *ImmuStore) Commit(entries []*KV) (id uint64, ts int64, alh [sha256.Size]byte, txh [sha256.Size]byte, err error) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		err = ErrAlreadyClosed
		return
	}
	s.mutex.Unlock()

	err = s.validateEntries(entries)
	if err != nil {
		return
	}

	appendableCh := make(chan appendableResult)
	go s.appendData(entries, appendableCh)

	tx, err := s.fetchAllocTx()
	if err != nil {
		return
	}
	defer s.releaseAllocTx(tx)

	tx.nentries = len(entries)

	for i, e := range entries {
		txe := tx.entries[i]
		txe.keyLen = len(e.Key)
		copy(txe.key, e.Key)
		txe.ValueLen = len(e.Value)
		txe.HValue = sha256.Sum256(e.Value)

		tx.htree[0][i] = txe.digest()
	}

	tx.buildHashTree()

	r := <-appendableCh // wait for data to be writen
	err = r.err
	if err != nil {
		return
	}

	err = s.commit(tx, r.offsets)
	if err != nil {
		return
	}

	return tx.ID, tx.Ts, tx.PrevAlh, tx.TxH, nil
}

func (s *ImmuStore) commit(tx *Tx, offsets []int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	// will overrite partially written and uncommitted data
	s.txLog.SetOffset(s.committedTxLogSize)

	tx.ID = s.committedTxID + 1
	tx.Ts = time.Now().Unix()

	blTxID, blRoot, err := s.aht.Root()
	if err != nil && err != ahtree.ErrEmptyTree {
		return err
	}

	tx.BlTxID = blTxID
	tx.BlRoot = blRoot

	if tx.ID <= tx.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.PrevAlh = s.committedAlh

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
		e := tx.entries[i]

		txe := tx.entries[i]
		txe.VOff = offsets[i]

		// tx serialization using pre-allocated buffer
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.keyLen))
		txSize += szSize
		copy(s._txbs[txSize:], e.key[:e.keyLen])
		txSize += e.keyLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.ValueLen))
		txSize += szSize
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.VOff))
		txSize += offsetSize
		copy(s._txbs[txSize:], txe.HValue[:])
		txSize += sha256.Size
	}

	var b [txIDSize + tsSize + txIDSize + 2*sha256.Size + szSize + sha256.Size]byte
	bi := 0

	binary.BigEndian.PutUint64(b[:], tx.ID)
	bi += txIDSize
	binary.BigEndian.PutUint64(b[bi:], uint64(tx.Ts))
	bi += tsSize
	binary.BigEndian.PutUint64(b[bi:], tx.BlTxID)
	bi += txIDSize
	copy(b[bi:], tx.BlRoot[:])
	bi += sha256.Size
	copy(b[bi:], tx.PrevAlh[:])
	bi += sha256.Size
	binary.BigEndian.PutUint32(b[bi:], uint32(len(tx.entries)))
	bi += szSize
	copy(b[bi:], tx.Eh[:])

	tx.TxH = sha256.Sum256(b[:])

	// tx serialization using pre-allocated buffer
	copy(s._txbs[txSize:], tx.TxH[:])
	txSize += sha256.Size

	txOff, _, err := s.txLog.Append(s._txbs[:txSize])
	if err != nil {
		return err
	}

	err = s.txLog.Flush()
	if err != nil {
		return err
	}

	alh := tx.Alh()

	if s.blBuffer == nil {
		_, _, err := s.aht.Append(alh[:])
		if err != nil {
			return err
		}
	} else {
		err = s.blBuffer.Put(alh)
		if err != nil {
			if err == cbuffer.ErrBufferIsFull {
				return ErrLinearProofMaxLenExceeded
			}
			return err
		}
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

	s.committedTxID++
	s.committedAlh = alh
	s.committedTxLogSize += int64(txSize)

	return nil
}

type TxMetadata struct {
	ID      uint64
	PrevAlh [sha256.Size]byte
	TxH     [sha256.Size]byte
	BlTxID  uint64
	BlRoot  [sha256.Size]byte
}

type DualProof struct {
	SourceTxMetadata         TxMetadata
	TargetTxMetadata         TxMetadata
	BinaryInclusionProof     [][sha256.Size]byte
	BinaryConsistencyProof   [][sha256.Size]byte
	TargetBlTxAlh            [sha256.Size]byte
	BinaryLastInclusionProof [][sha256.Size]byte
	LinearProof              *LinearProof
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
		SourceTxMetadata: TxMetadata{
			ID:      sourceTx.ID,
			PrevAlh: sourceTx.PrevAlh,
			TxH:     sourceTx.TxH,
			BlTxID:  sourceTx.BlTxID,
			BlRoot:  sourceTx.BlRoot,
		},
		TargetTxMetadata: TxMetadata{
			ID:      targetTx.ID,
			PrevAlh: targetTx.PrevAlh,
			TxH:     targetTx.TxH,
			BlTxID:  targetTx.BlTxID,
			BlRoot:  targetTx.BlRoot,
		},
	}

	if sourceTx.ID < targetTx.BlTxID {
		binInclusionProof, err := s.aht.InclusionProof(sourceTx.ID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.BinaryInclusionProof = binInclusionProof
	}

	if sourceTx.BlTxID > 0 {
		binConsistencyProof, err := s.aht.ConsistencyProof(sourceTx.BlTxID, targetTx.BlTxID) // first root sourceTx.BlRoot, second one targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.BinaryConsistencyProof = binConsistencyProof
	}

	if targetTx.BlTxID > 0 {
		targetBlTx := s.NewTx()
		err = s.ReadTx(targetTx.BlTxID, targetBlTx)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTx.Alh()

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := s.aht.InclusionProof(targetTx.BlTxID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.BinaryLastInclusionProof = binLastInclusionProof
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
	Proof      [][sha256.Size]byte
}

// LinearProof returns a list of hashes to calculate Alh@targetTxID from Alh@sourceTxID
func (s *ImmuStore) LinearProof(sourceTxID, targetTxID uint64) (*LinearProof, error) {
	if sourceTxID == 0 || sourceTxID > targetTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if s.maxLinearProofLen > 0 && int(targetTxID-sourceTxID+1) > s.maxLinearProofLen {
		return nil, ErrLinearProofMaxLenExceeded
	}

	r, err := s.NewTxReader(sourceTxID, s.maxTxSize)

	tx, err := r.Read()
	if err != nil {
		return nil, err
	}

	proof := make([][sha256.Size]byte, targetTxID-sourceTxID+1)
	proof[0] = tx.Alh()

	for i := 1; i < len(proof); i++ {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		var b [txIDSize + 2*sha256.Size]byte
		binary.BigEndian.PutUint64(b[:], tx.BlTxID)
		copy(b[txIDSize:], tx.BlRoot[:])
		copy(b[txIDSize+sha256.Size:], tx.TxH[:])

		proof[i] = sha256.Sum256(b[:]) // hash(blTxID + blRoot + txH)
	}

	return &LinearProof{
		SourceTxID: sourceTxID,
		TargetTxID: targetTxID,
		Proof:      proof,
	}, nil
}

func (s *ImmuStore) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off := (txID - 1) * cLogEntrySize

	var cb [cLogEntrySize]byte

	_, err := s.cLog.ReadAt(cb[:], int64(off))
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(cb[:]))
	txSize := int(binary.BigEndian.Uint32(cb[offsetSize:]))

	if txOffset > s.committedTxLogSize || txSize > s.maxTxSize {
		return 0, 0, ErrorCorruptedTxData
	}

	return txOffset, txSize, nil
}

func (s *ImmuStore) ReadTx(txID uint64, tx *Tx) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	txOff, txSize, err := s.txOffsetAndSize(txID)
	if err != nil {
		return err
	}

	txReader := appendable.NewReaderFrom(s.txLog, txOff, txSize)

	return tx.readFrom(txReader)
}

func (s *ImmuStore) ReadValue(tx *Tx, key []byte) ([]byte, error) {
	for _, e := range tx.Entries() {
		if bytes.Equal(e.Key(), key) {
			v := make([]byte, e.ValueLen)
			_, err := s.ReadValueAt(v, e.VOff, e.HValue)
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

	vLog, err := s.fetchVLog(vLogID, true)
	if err != nil {
		return 0, err
	}
	defer s.releaseVLog(vLogID)

	n, err := vLog.ReadAt(b, offset)
	if err != nil {
		return n, err
	}

	if hvalue != sha256.Sum256(b) {
		return n, ErrCorruptedData
	}

	return n, nil
}

type TxReader struct {
	r           *appendable.Reader
	_tx         *Tx
	alreadyRead bool
	txID        uint64
	alh         [sha256.Size]byte
}

func (s *ImmuStore) NewTxReader(txID uint64, bufSize int) (*TxReader, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	syncedReader := &syncedReader{wr: s.txLog, maxSize: s.committedTxLogSize, mutex: &s.mutex}

	txOff, _, err := s.txOffsetAndSize(txID)
	if err != nil {
		return nil, err
	}

	r := appendable.NewReaderFrom(syncedReader, txOff, bufSize)

	tx := s.NewTx()

	return &TxReader{r: r, _tx: tx}, nil
}

func (txr *TxReader) Read() (*Tx, error) {
	err := txr._tx.readFrom(txr.r)
	if err != nil {
		return nil, err
	}

	if txr.alreadyRead && (txr.txID != txr._tx.ID-1 || txr.alh != txr._tx.PrevAlh) {
		return nil, ErrorCorruptedTxData
	}

	txr.alreadyRead = true
	txr.txID = txr._tx.ID
	txr.alh = txr._tx.Alh()

	return txr._tx, nil
}

type syncedReader struct {
	wr      io.ReaderAt
	maxSize int64
	mutex   *sync.Mutex
}

func (r *syncedReader) ReadAt(bs []byte, off int64) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(bs) == 0 {
		return 0, nil
	}

	available := minInt(len(bs), int(r.maxSize-off))

	if r.maxSize < off || available == 0 {
		return 0, io.EOF
	}

	return r.wr.ReadAt(bs[:available], off)
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
		if kv.Key == nil || kv.Value == nil {
			return ErrNullKeyOrValue
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

	for vLogID := range s.vLogs {
		vLog, _ := s.fetchVLog(vLogID, false)
		err := vLog.Sync()
		if err != nil {
			return err
		}
		s.releaseVLog(vLogID)
	}

	err := s.txLog.Sync()
	if err != nil {
		return err
	}

	err = s.cLog.Sync()
	if err != nil {
		return err
	}

	return s.index.Sync()
}

func (s *ImmuStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.closed = true

	errors := make([]error, 0)

	for vLogID := range s.vLogs {
		vLog, _ := s.fetchVLog(vLogID, false)

		err := vLog.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	s.vLogsCond.Broadcast()

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

	iErr := s.index.Close()
	if iErr != nil {
		errors = append(errors, iErr)
	}

	if len(errors) > 0 {
		return &multierr.MultiErr{Errors: errors}
	}

	return nil
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
