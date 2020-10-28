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

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/multierr"
	"codenotary.io/immudb-v2/tbtree"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrorMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrDuplicatedKey = errors.New("duplicated key")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrorCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedData = errors.New("data is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")

var ErrKeyNotFound = errors.New("key not found")

var ErrTrustedTxNotOlderThanTargetTx = errors.New("trusted tx is not older than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("max linear proof length limit exceeded")

const DefaultMaxConcurrency = 100
const DefaultMaxIOConcurrency = 1
const DefaultMaxTxEntries = 1 << 16 // 65536
const DefaultMaxKeyLen = 256
const DefaultMaxValueLen = 1 << 20 // 1 Mb
const DefaultFileMode = 0755
const DefaultMaxLinearProofLen = 1 << 10

const MaxKeyLen = 1024 // assumed to be not lower than hash size

const MaxParallelIO = 127

const cLogEntrySize = 12 // tx offset & size

const verifyOnIndexing = false

const Version = 1

const (
	MetaVersion      = "VERSION"
	MetaMaxTxEntries = "MAX_TX_ENTRIES"
	MetaMaxKeyLen    = "MAX_KEY_LEN"
	MetaMaxValueLen  = "MAX_VALUE_LEN"
	MetaFileSize     = "FILE_SIZE"
)

type ImmuStore struct {
	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	txLog appendable.Appendable
	cLog  appendable.Appendable

	committedTxID      uint64
	committedAlh       [32]byte
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
	hash := sha256.New()

	hash.Write(kv.Key)
	hvalue := sha256.Sum256(kv.Value)
	hash.Write(hvalue[:])

	var eh [sha256.Size]byte
	copy(eh[:], hash.Sum(nil))
	return eh
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
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxTxEntries, opts.maxTxEntries)
	metadata.PutInt(MetaMaxKeyLen, opts.maxKeyLen)
	metadata.PutInt(MetaMaxValueLen, opts.maxValueLen)
	metadata.PutInt(MetaFileSize, opts.fileSize)

	appendableOpts := multiapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileSize(opts.fileSize).
		SetFileMode(opts.fileMode).
		SetMetadata(metadata.Bytes())

	vLogs := make([]appendable.Appendable, opts.maxIOConcurrency)
	for i := 0; i < opts.maxIOConcurrency; i++ {
		appendableOpts.SetFileExt("val")
		appendableOpts.SetCompressionFormat(opts.compressionFormat)
		appendableOpts.SetCompresionLevel(opts.compressionLevel)
		appendableOpts.SetMaxOpenedFiles(opts.vLogMaxOpenedFiles)
		vLogPath := filepath.Join(path, fmt.Sprintf("val_%d", i))
		vLog, err := multiapp.Open(vLogPath, appendableOpts)
		if err != nil {
			return nil, err
		}
		vLogs[i] = vLog
	}

	appendableOpts.SetFileExt("tx")
	appendableOpts.SetCompressionFormat(appendable.NoCompression)
	appendableOpts.SetMaxOpenedFiles(opts.txLogMaxOpenedFiles)
	txLogPath := filepath.Join(path, "tx")
	txLog, err := multiapp.Open(txLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.SetFileExt("txi")
	appendableOpts.SetCompressionFormat(appendable.NoCompression)
	appendableOpts.SetMaxOpenedFiles(opts.commitLogMaxOpenedFiles)
	cLogPath := filepath.Join(path, "commit")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(vLogs, txLog, cLog, opts)
}

func OpenWith(vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	fileSize, ok := metadata.GetInt(MetaFileSize)
	if !ok {
		return nil, ErrCorruptedCLog
	}
	maxTxEntries, ok := metadata.GetInt(MetaMaxTxEntries)
	if !ok {
		return nil, ErrCorruptedCLog
	}
	maxKeyLen, ok := metadata.GetInt(MetaMaxKeyLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}
	maxValueLen, ok := metadata.GetInt(MetaMaxValueLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	mapp, ok := txLog.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
	}

	mapp, ok = cLog.(*multiapp.MultiFileAppendable)
	if ok {
		mapp.SetFileSize(fileSize)
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
		committedTxSize = int(binary.BigEndian.Uint32(b[8:]))
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
		mapp, ok := vLog.(*multiapp.MultiFileAppendable)
		if ok {
			mapp.SetFileSize(fileSize)
		}

		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
	}

	indexPath := filepath.Join("data", "index")

	indexOpts := tbtree.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetFileMode(opts.fileMode).
		SetFileSize(fileSize).
		SetSynced(false). // index is built from derived data
		SetCacheSize(opts.indexOpts.cacheSize).
		SetFlushThld(opts.indexOpts.flushThld).
		SetMaxActiveSnapshots(opts.indexOpts.maxActiveSnapshots).
		SetMaxNodeSize(opts.indexOpts.maxNodeSize).
		SetRenewSnapRootAfter(opts.indexOpts.renewSnapRootAfter)

	index, err := tbtree.Open(indexPath, indexOpts)
	if err != nil {
		return nil, err
	}

	kvs := make([]*tbtree.KV, maxTxEntries)
	for i := range kvs {
		kvs[i] = &tbtree.KV{K: make([]byte, maxKeyLen), V: make([]byte, sha256.Size+4+8)}
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
		readOnly:           opts.readOnly,
		synced:             opts.synced,
		maxTxEntries:       maxTxEntries,
		maxKeyLen:          maxKeyLen,
		maxValueLen:        maxValueLen,
		maxLinearProofLen:  opts.maxLinearProofLen,
		maxTxSize:          maxTxSize,
		index:              index,
		_kvs:               kvs,
		_txs:               txs,
		_txbs:              txbs,
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
			if verifyOnIndexing {
				path := tx.Proof(i)
				be := make([]byte, s.maxValueLen)
				_, err = s.ReadValueAt(be[:txEntries[i].ValueLen], txEntries[i].VOff, txEntries[i].HValue)
				if err != nil {
					return err
				}
				kv := &KV{Key: txEntries[i].Key(), Value: be[:txEntries[i].ValueLen]}
				verifies := path.VerifyInclusion(uint64(len(txEntries)-1), uint64(i), tx.Eh, kv.Digest())
				if !verifies {
					return ErrorCorruptedTxData
				}
			}

			var b [4 + 8 + sha256.Size]byte
			binary.BigEndian.PutUint32(b[:], uint32(e.ValueLen))
			binary.BigEndian.PutUint64(b[4:], uint64(e.VOff))
			copy(b[4+8:], e.HValue[:])

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
	return 2*8 + 2*sha256.Size + 4 + maxTxEntries*(4+maxKeyLen+4+sha256.Size) + 4
}

func (s *ImmuStore) MaxValueLen() int {
	return s.maxValueLen
}

func (s *ImmuStore) MaxKeyLen() int {
	return s.maxKeyLen
}

func (s *ImmuStore) MaxConcurrency() int {
	return s.maxConcurrency
}

func (s *ImmuStore) MaxLinearProofLen() int {
	return s.maxLinearProofLen
}

func (s *ImmuStore) MaxTxEntries() int {
	return s.maxTxEntries
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

	return tx.ID, tx.Ts, tx.PrevAlh, tx.Txh, nil
}

func (s *ImmuStore) commit(tx *Tx, offsets []int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if onPrivateSection {
		panic("commiting")
	}
	onPrivateSection = true
	defer func() { onPrivateSection = false }()

	if s.closed {
		return ErrAlreadyClosed
	}

	// will overrite partially written and uncommitted data
	s.txLog.SetOffset(s.committedTxLogSize)

	tx.ID = s.committedTxID + 1
	tx.Ts = time.Now().Unix()
	tx.PrevAlh = s.committedAlh

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.ID))
	txSize += 8
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.Ts))
	txSize += 8
	copy(s._txbs[txSize:], tx.PrevAlh[:])
	txSize += sha256.Size
	binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(tx.nentries))
	txSize += 4

	for i := 0; i < tx.nentries; i++ {
		e := tx.entries[i]

		txe := tx.entries[i]
		txe.VOff = offsets[i]

		// tx serialization using pre-allocated buffer
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.keyLen))
		txSize += 4
		copy(s._txbs[txSize:], e.key[:e.keyLen])
		txSize += e.keyLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.ValueLen))
		txSize += 4
		copy(s._txbs[txSize:], txe.HValue[:])
		txSize += sha256.Size
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.VOff))
		txSize += 8
	}

	var b [52]byte
	binary.BigEndian.PutUint64(b[:], tx.ID)
	binary.BigEndian.PutUint64(b[8:], uint64(tx.Ts))
	binary.BigEndian.PutUint32(b[16:], uint32(len(tx.entries)))
	copy(b[20:], tx.Eh[:])
	tx.Txh = sha256.Sum256(b[:])

	// tx serialization using pre-allocated buffer
	copy(s._txbs[txSize:], tx.Txh[:])
	txSize += sha256.Size

	txOff, _, err := s.txLog.Append(s._txbs[:txSize])
	if err != nil {
		return err
	}

	err = s.txLog.Flush()
	if err != nil {
		return err
	}

	var cb [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cb[:], uint64(txOff))
	binary.BigEndian.PutUint32(cb[8:], uint32(txSize))
	_, _, err = s.cLog.Append(cb[:])
	if err != nil {
		return err
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	s.committedTxID++
	s.committedAlh = tx.Alh()
	s.committedTxLogSize += int64(txSize)

	return nil
}

func (s *ImmuStore) LinearProof(trustedTxID, txID uint64) (path [][sha256.Size]byte, err error) {
	if trustedTxID >= txID {
		return nil, ErrTrustedTxNotOlderThanTargetTx
	}

	if int(txID-trustedTxID) > s.maxLinearProofLen {
		return nil, ErrLinearProofMaxLenExceeded
	}

	r, err := s.NewTxReader(trustedTxID, s.maxTxSize)

	tx, err := r.Read()
	if err != nil {
		return nil, err
	}

	path = make([][sha256.Size]byte, 1+(txID-1-trustedTxID)*2)
	path[0] = tx.Alh()

	for {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		if tx.ID == txID {
			return path, nil
		}

		path = append(path, tx.PrevAlh, tx.Txh)
	}
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
	txSize := int(binary.BigEndian.Uint32(cb[8:]))

	if txOffset > s.committedTxLogSize {
		return 0, 0, ErrorCorruptedTxData
	}

	if txSize > s.maxTxSize {
		return 0, 0, ErrTxSizeGreaterThanMaxTxSize
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

var onPrivateSection = false

func (r *syncedReader) ReadAt(bs []byte, off int64) (int, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if onPrivateSection {
		panic("commiting")
	}
	onPrivateSection = true
	defer func() { onPrivateSection = false }()

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

	for vLogID := range s.vLogs {
		vLog, _ := s.fetchVLog(vLogID, false)

		err := vLog.Close()
		if err != nil {
			return err
		}
	}
	s.vLogsCond.Broadcast()

	s.closed = true

	errors := make([]error, 0)

	txErr := s.txLog.Close()
	if txErr != nil {
		errors = append(errors, txErr)
	}

	cErr := s.cLog.Close()
	if cErr != nil {
		errors = append(errors, cErr)
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
