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
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codenotary.io/immudb-v2/appendable"
	"codenotary.io/immudb-v2/appendable/multiapp"
	"codenotary.io/immudb-v2/tbtree"
	"github.com/codenotary/merkletree"
)

var ErrIllegalArgument = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrorMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrorCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrCorruptedVLog = errors.New("value log is corrupted")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")

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

const bufLenTxh = 2*8 + 4 + sha256.Size

const cLogEntrySize = 12 // tx offset & size

const verifyOnIndexing = false

type Tx struct {
	ID       uint64
	Ts       int64
	PrevAlh  [sha256.Size]byte
	nentries int
	entries  []*Txe
	Txh      [sha256.Size]byte
	htree    [][][sha256.Size]byte
	Eh       [sha256.Size]byte
}

func NewTx(nentries int, maxKeyLen int) *Tx {
	entries := make([]*Txe, nentries)
	for i := 0; i < nentries; i++ {
		entries[i] = &Txe{key: make([]byte, maxKeyLen)}
	}

	w := 1
	for w < nentries {
		w = w << 1
	}

	layers := bits.Len64(uint64(nentries-1)) + 1
	htree := make([][][sha256.Size]byte, layers)
	for l := 0; l < layers; l++ {
		htree[l] = make([][sha256.Size]byte, w>>l)
	}

	return &Tx{
		ID:      0,
		entries: entries,
		htree:   htree,
	}
}

const NodePrefix = byte(1)

func (tx *Tx) buildHashTree() {
	l := 0
	w := tx.nentries

	p := [sha256.Size*2 + 1]byte{NodePrefix}

	for w > 1 {
		wn := 0

		for i := 0; i+1 < w; i += 2 {
			copy(p[1:sha256.Size+1], tx.htree[l][i][:])
			copy(p[sha256.Size+1:], tx.htree[l][i+1][:])
			tx.htree[l+1][wn] = sha256.Sum256(p[:])
			wn++
		}

		if w%2 == 1 {
			tx.htree[l+1][wn] = tx.htree[l][w-1]
			wn++
		}

		l++
		w = wn
	}

	tx.Eh = tx.htree[l][0]
}

func (tx *Tx) Width() uint64 {
	return uint64(tx.nentries)
}

func (tx *Tx) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	tx.htree[layer][index] = value
}

func (tx *Tx) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return &tx.htree[layer][index]
}

func (tx *Tx) Entries() []*Txe {
	return tx.entries[:tx.nentries]
}

func (tx *Tx) Alh() [sha256.Size]byte {
	bs := make([]byte, 2*sha256.Size)
	copy(bs, tx.PrevAlh[:])
	copy(bs[sha256.Size:], tx.Txh[:])
	return sha256.Sum256(bs)
}

func (tx *Tx) Proof(kindex int) merkletree.Path {
	return merkletree.InclusionProof(tx, uint64(tx.nentries-1), uint64(kindex))
}

func (tx *Tx) readFrom(r io.Reader, b []byte) error {
	id, err := ReadUint64(r, b)
	if err != nil {
		return err
	}
	tx.ID = id

	ts, err := ReadUint64(r, b)
	if err != nil {
		return err
	}
	tx.Ts = int64(ts)

	_, err = r.Read(tx.PrevAlh[:])
	if err != nil {
		return err
	}

	nentries, err := ReadUint32(r, b)
	if err != nil {
		return err
	}
	tx.nentries = int(nentries)

	for i := 0; i < int(nentries); i++ {
		klen, err := ReadUint32(r, b)
		if err != nil {
			return err
		}
		tx.entries[i].keyLen = int(klen)

		_, err = r.Read(tx.entries[i].key[:klen])
		if err != nil {
			return err
		}

		vlen, err := ReadUint32(r, b)
		if err != nil {
			return err
		}
		tx.entries[i].ValueLen = int(vlen)

		_, err = r.Read(tx.entries[i].HValue[:])
		if err != nil {
			return err
		}

		voff, err := ReadUint64(r, b)
		if err != nil {
			return err
		}
		tx.entries[i].VOff = int64(voff)

		tx.htree[0][i] = tx.entries[i].digest()
	}

	_, err = r.Read(tx.Txh[:])

	tx.buildHashTree()

	binary.BigEndian.PutUint64(b, tx.ID)
	binary.BigEndian.PutUint64(b[8:], uint64(tx.Ts))
	binary.BigEndian.PutUint32(b[16:], uint32(len(tx.entries)))
	copy(b[20:], tx.Eh[:])

	if tx.Txh != sha256.Sum256(b[:bufLenTxh]) {
		return ErrorCorruptedTxData
	}

	return nil
}

func ReadUint64(r io.Reader, b []byte) (uint64, error) {
	_, err := r.Read(b[:8])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func ReadUint32(r io.Reader, b []byte) (uint32, error) {
	_, err := r.Read(b[:4])
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

type Txe struct {
	keyLen   int
	key      []byte
	ValueLen int
	HValue   [sha256.Size]byte
	VOff     int64
}

func (e *Txe) Key() []byte {
	return e.key[:e.keyLen]
}

func (e *Txe) digest() [sha256.Size]byte {
	hash := sha256.New()

	hash.Write(e.Key())
	hash.Write(e.HValue[:])

	var eh [sha256.Size]byte
	copy(eh[:], hash.Sum(nil))
	return eh
}

const (
	MetaMaxTxEntries = "MAX_TX_ENTRIES"
	MetaMaxKeyLen    = "MAX_KEY_LEN"
	MetaMaxValueLen  = "MAX_VALUE_LEN"
	MetaFileSize     = "FILE_SIZE"
)

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode

	maxConcurrency    int
	maxIOConcurrency  int
	maxLinearProofLen int

	vLogMaxOpenedFiles      int
	txLogMaxOpenedFiles     int
	commitLogMaxOpenedFiles int

	// options below are only set during initialization and stored as metadata
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	fileSize          int
	compressionFormat int
	compressionLevel  int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly: false,
		synced:   true,
		fileMode: DefaultFileMode,

		maxConcurrency:    DefaultMaxConcurrency,
		maxIOConcurrency:  DefaultMaxIOConcurrency,
		maxLinearProofLen: DefaultMaxLinearProofLen,

		vLogMaxOpenedFiles:      10,
		txLogMaxOpenedFiles:     10,
		commitLogMaxOpenedFiles: 1,

		// options below are only set during initialization and stored as metadata
		maxTxEntries:      DefaultMaxTxEntries,
		maxKeyLen:         DefaultMaxKeyLen,
		maxValueLen:       DefaultMaxValueLen,
		fileSize:          multiapp.DefaultFileSize,
		compressionFormat: appendable.DefaultCompressionFormat,
		compressionLevel:  appendable.DefaultCompressionLevel,
	}
}

func (opt *Options) SetReadOnly(readOnly bool) *Options {
	opt.readOnly = readOnly
	return opt
}

func (opt *Options) SetSynced(synced bool) *Options {
	opt.synced = synced
	return opt
}

func (opt *Options) SetFileMode(fileMode os.FileMode) *Options {
	opt.fileMode = fileMode
	return opt
}

func (opt *Options) SetConcurrency(maxConcurrency int) *Options {
	opt.maxConcurrency = maxConcurrency
	return opt
}

func (opt *Options) SetIOConcurrency(maxIOConcurrency int) *Options {
	opt.maxIOConcurrency = maxIOConcurrency
	return opt
}

func (opt *Options) SetMaxTxEntries(maxTxEntries int) *Options {
	opt.maxTxEntries = maxTxEntries
	return opt
}

func (opt *Options) SetMaxKeyLen(maxKeyLen int) *Options {
	opt.maxKeyLen = maxKeyLen
	return opt
}

func (opt *Options) SetMaxValueLen(maxValueLen int) *Options {
	opt.maxValueLen = maxValueLen
	return opt
}

func (opt *Options) SetMaxLinearProofLen(maxLinearProofLen int) *Options {
	opt.maxLinearProofLen = maxLinearProofLen
	return opt
}

func (opt *Options) SetFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

func (opt *Options) SetVLogMaxOpenedFiles(vLogMaxOpenedFiles int) *Options {
	opt.vLogMaxOpenedFiles = vLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetTxLogMaxOpenedFiles(txLogMaxOpenedFiles int) *Options {
	opt.txLogMaxOpenedFiles = txLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetCommitLogMaxOpenedFiles(commitLogMaxOpenedFiles int) *Options {
	opt.commitLogMaxOpenedFiles = commitLogMaxOpenedFiles
	return opt
}

func (opt *Options) SetCompressionFormat(compressionFormat int) *Options {
	opt.compressionFormat = compressionFormat
	return opt
}

func (opt *Options) SetCompresionLevel(compressionLevel int) *Options {
	opt.compressionLevel = compressionLevel
	return opt
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

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
	_b    []byte // pre-allocated general purpose buffer

	_kvs []*tbtree.KV //pre-allocated for indexing

	index        *tbtree.TBtree
	indexerErr   error
	indexerMutex sync.Mutex

	mutex sync.Mutex

	closed bool
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
	if opts == nil || opts.maxKeyLen > MaxKeyLen ||
		opts.maxConcurrency < 1 ||
		opts.maxIOConcurrency < 1 ||
		opts.maxIOConcurrency > MaxParallelIO {
		return nil, ErrIllegalArgument
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
	metadata.PutInt(MetaFileSize, opts.fileSize)
	metadata.PutInt(MetaMaxTxEntries, opts.maxTxEntries)
	metadata.PutInt(MetaMaxKeyLen, opts.maxKeyLen)
	metadata.PutInt(MetaMaxValueLen, opts.maxValueLen)

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

	appendableOpts.SetFileExt("idb")
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
	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	if cLogSize%cLogEntrySize > 0 {
		return nil, ErrCorruptedCLog
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
		tx := NewTx(maxTxEntries, maxKeyLen)
		txs.PushBack(tx)
	}

	txbs := make([]byte, maxTxSize)

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx := txs.Front().Value.(*Tx)
		err = tx.readFrom(txReader, make([]byte, maxInt(MaxKeyLen, bufLenTxh)))
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

	indexPath := filepath.Join("data", "index.idx") //TODO change for appendable

	index, err := tbtree.Open(indexPath, tbtree.DefaultOptions())
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
		_b:                 make([]byte, bufLenTxh),
	}

	go store.indexer()

	return store, nil
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

func (s *ImmuStore) indexerStatus() error {
	s.indexerMutex.Lock()
	defer s.indexerMutex.Unlock()
	return s.indexerErr
}

func (s *ImmuStore) doIndexing() error {
	txID := s.index.Ts() + 1

	txOff, _, err := s.TxOffsetAndSize(txID)
	if err != nil {
		return err
	}

	txReader, err := s.NewTxReader(txOff, s.maxTxSize)
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
				_, err = s.ReadValueAt(be[:txEntries[i].ValueLen], txEntries[i].VOff)
				if err != nil {
					return err
				}
				kv := &KV{Key: txEntries[i].Key(), Value: be[:txEntries[i].ValueLen]}
				verifies := path.VerifyInclusion(uint64(len(txEntries)-1), uint64(i), tx.Eh, kv.Digest())
				if !verifies {
					return ErrorCorruptedTxData
				}
			}

			var b [4 + 8]byte
			binary.BigEndian.PutUint32(b[:], uint32(e.ValueLen))
			binary.BigEndian.PutUint64(b[4:], uint64(e.VOff))

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

	binary.BigEndian.PutUint64(s._b, tx.ID)
	binary.BigEndian.PutUint64(s._b[8:], uint64(tx.Ts))
	binary.BigEndian.PutUint32(s._b[16:], uint32(len(tx.entries)))
	copy(s._b[20:], tx.Eh[:])
	tx.Txh = sha256.Sum256(s._b[:bufLenTxh])

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

	binary.BigEndian.PutUint64(s._b, uint64(txOff))
	binary.BigEndian.PutUint32(s._b[8:], uint32(txSize))
	_, _, err = s.cLog.Append(s._b[:cLogEntrySize])
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

	trustedTxOffset, _, err := s.TxOffsetAndSize(trustedTxID)
	if err != nil {
		return nil, err
	}

	r, err := s.NewTxReader(trustedTxOffset, s.maxTxSize)

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

func (s *ImmuStore) TxOffsetAndSize(txID uint64) (int64, int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return 0, 0, ErrAlreadyClosed
	}

	return s.txOffsetAndSize(txID)
}

func (s *ImmuStore) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArgument
	}

	off := (txID - 1) * cLogEntrySize

	_, err := s.cLog.ReadAt(s._b[:cLogEntrySize], int64(off))
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(s._b))
	txSize := int(binary.BigEndian.Uint32(s._b[8:]))

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

	return tx.readFrom(txReader, make([]byte, maxInt(MaxKeyLen, bufLenTxh)))
}

func (s *ImmuStore) ReadValueAt(b []byte, off int64) (int, error) {
	vLogID, offset := decodeOffset(off)

	vLog, err := s.fetchVLog(vLogID, true)
	if err != nil {
		return 0, err
	}
	defer s.releaseVLog(vLogID)

	return vLog.ReadAt(b, offset)
}

type TxReader struct {
	r           io.Reader
	_tx         *Tx
	_b          []byte
	alreadyRead bool
	txID        uint64
	alh         [sha256.Size]byte
}

func (s *ImmuStore) NewTxReader(offset int64, bufSize int) (*TxReader, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	syncedReader := &syncedReader{wr: s.txLog, maxSize: s.committedTxLogSize, mutex: &s.mutex}

	r := appendable.NewReaderFrom(syncedReader, offset, bufSize)

	tx := NewTx(s.maxTxEntries, s.maxKeyLen)
	b := make([]byte, maxInt(MaxKeyLen, bufLenTxh))

	return &TxReader{r: r, _tx: tx, _b: b}, nil
}

func (txr *TxReader) Read() (*Tx, error) {
	err := txr._tx.readFrom(txr.r, txr._b)
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

	for _, kv := range entries {
		if len(kv.Key) > s.maxKeyLen {
			return ErrorMaxKeyLenExceeded
		}
		if len(kv.Value) > s.maxValueLen {
			return ErrorMaxValueLenExceeded
		}
	}
	return nil
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

	err := s.txLog.Close()
	if err != nil {
		return err
	}

	err = s.cLog.Close()
	if err != nil {
		return err
	}

	s.closed = true
	return nil
}

func maxInt(a, b int) int {
	if a <= b {
		return b
	}
	return a
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
