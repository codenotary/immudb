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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sync"
	"time"

	"codenotary.io/immudb-v2/appendable"
	"github.com/codenotary/merkletree"
)

var ErrIllegalArgument = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrorNoEntriesProvided = errors.New("No entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("Max number of entries per tx exceeded")
var ErrorMaxKeyLenExceeded = errors.New("Max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("Max value length exceeded")
var ErrorPathIsNotADirectory = errors.New("Path is not a directory")
var ErrorCorruptedTxData = errors.New("Tx data is corrupted")
var ErrCorruptedCLog = errors.New("Commit log is corrupted")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("Tx size greater than max tx size")

var ErrTrustedTxNotOlderThanTargetTx = errors.New("Trusted tx is not older than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("Max linear proof length limit exceeded")

const DefaultMaxTxEntries = 1 << 16 // 65536
const DefaultMaxKeyLen = 256
const DefaultMaxValueLen = 1 << 20 // 1 Mb
const DefaultFileMode = 0644
const DefaultMaxLinearProofLen = 1 << 10

const MaxKeyLen = 1024 // assumed to be not lower than hash size

const bufLenTxh = 2*8 + 4 + sha256.Size

const cLogEntrySize = 12 // tx offset & size

type Tx struct {
	id       uint64
	ts       int64
	alh      [sha256.Size]byte
	nentries int
	es       []*txe
	txh      [sha256.Size]byte
	htree    *txHashTree
}

func mallocTx(nentries int, maxKeyLen int) *Tx {
	tx := &Tx{
		id:    0,
		es:    make([]*txe, nentries),
		htree: mallocTxHashTree(nentries),
	}

	for i := 0; i < nentries; i++ {
		tx.es[i] = &txe{key: make([]byte, maxKeyLen)}
	}

	return tx
}

func (tx *Tx) Alh() [sha256.Size]byte {
	bs := make([]byte, 2*sha256.Size)
	copy(bs, tx.alh[:])
	copy(bs[sha256.Size:], tx.txh[:])
	return sha256.Sum256(bs)
}

func (tx *Tx) Proof(kindex int) merkletree.Path {
	return merkletree.InclusionProof(tx.htree, uint64(tx.htree.width-1), uint64(kindex))
}

func (tx *Tx) readFrom(r io.Reader, b []byte) error {
	id, err := ReadUint64(r, b)
	if err != nil {
		return err
	}
	tx.id = id

	ts, err := ReadUint64(r, b)
	if err != nil {
		return err
	}
	tx.ts = int64(ts)

	_, err = r.Read(tx.alh[:])
	if err != nil {
		return err
	}

	nentries, err := ReadUint32(r, b)
	if err != nil {
		return err
	}
	tx.nentries = int(nentries)

	tx.htree.width = 0

	for i := 0; i < int(nentries); i++ {
		klen, err := ReadUint32(r, b)
		if err != nil {
			return err
		}
		tx.es[i].keyLen = int(klen)

		_, err = r.Read(tx.es[i].key[:klen])
		if err != nil {
			return err
		}

		vlen, err := ReadUint32(r, b)
		if err != nil {
			return err
		}
		tx.es[i].valueLen = int(vlen)

		_, err = r.Read(tx.es[i].hvalue[:])
		if err != nil {
			return err
		}

		voff, err := ReadUint64(r, b)
		if err != nil {
			return err
		}
		tx.es[i].voff = int64(voff)

		eh := tx.es[i].digest()
		merkletree.AppendHash(tx.htree, &eh)
		tx.htree.width++
	}

	_, err = r.Read(tx.txh[:])

	root := merkletree.Root(tx.htree)

	binary.BigEndian.PutUint64(b, tx.id)
	binary.BigEndian.PutUint64(b[8:], uint64(tx.ts))
	binary.BigEndian.PutUint32(b[16:], uint32(len(tx.es)))
	copy(b[20:], root[:])

	if tx.txh != sha256.Sum256(b[:bufLenTxh]) {
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

type txe struct {
	keyLen   int
	key      []byte
	valueLen int
	hvalue   [sha256.Size]byte
	voff     int64
}

func (e *txe) digest() [sha256.Size]byte {
	hash := sha256.New()

	hash.Write(e.key[:e.keyLen])
	hash.Write(e.hvalue[:])

	var eh [sha256.Size]byte
	copy(eh[:], hash.Sum(nil))
	return eh
}

type txHashTree struct {
	data  [][][sha256.Size]byte
	width int
}

func mallocTxHashTree(maxWidth int) *txHashTree {
	layers := bits.Len64(uint64(maxWidth-1)) + 1

	hts := &txHashTree{
		data:  make([][][sha256.Size]byte, layers),
		width: 0,
	}

	for l := 0; l < layers; l++ {
		hts.data[l] = make([][sha256.Size]byte, maxWidth>>l)
	}

	return hts
}

func (hts *txHashTree) Width() uint64 {
	return uint64(hts.width)
}

func (hts *txHashTree) Set(layer uint8, index uint64, value [sha256.Size]byte) {
	hts.data[layer][index] = value
}

func (hts *txHashTree) Get(layer uint8, index uint64) *[sha256.Size]byte {
	return &hts.data[layer][index]
}

type Options struct {
	readOnly          bool
	synced            bool
	fileMode          os.FileMode
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	maxLinearProofLen int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
		fileMode:          DefaultFileMode,
		maxTxEntries:      DefaultMaxTxEntries,
		maxKeyLen:         DefaultMaxKeyLen,
		maxValueLen:       DefaultMaxValueLen,
		maxLinearProofLen: DefaultMaxLinearProofLen,
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

type ImmuStore struct {
	txLog appendable.Appendable
	vLog  appendable.Appendable
	cLog  appendable.Appendable

	committedTxID      uint64
	committedAlh       [32]byte
	committedTxLogSize int64

	readOnly          bool
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	maxLinearProofLen int

	maxTxSize int
	_tx       *Tx    // pre-allocated tx
	_txbs     []byte // pre-allocated buffer to support tx serialization
	_b        []byte // pre-allocated general purpose buffer

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
	if opts == nil || opts.maxKeyLen > MaxKeyLen {
		return nil, ErrIllegalArgument
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, 0700)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	appendableOpts := appendable.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileMode(opts.fileMode)

	txLogFilename := filepath.Join(path, "immudb.itx")
	txLog, err := appendable.Open(txLogFilename, appendableOpts)
	if err != nil {
		return nil, err
	}

	vLogFilename := filepath.Join(path, "immudb.val")
	vLog, err := appendable.Open(vLogFilename, appendableOpts)
	if err != nil {
		return nil, err
	}

	cLogFilename := filepath.Join(path, "immudb.ctx")
	cLog, err := appendable.Open(cLogFilename, appendableOpts)
	if err != nil {
		return nil, err
	}

	return open(txLog, vLog, cLog, opts)
}

func open(txLog, vLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
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

	// TODO: take into account initial baseoffset for metadata
	if txLogFileSize == 0 {
		// TODO:  write config parameters and other metadata
	} else {
		// TODO:  read from files config parameters (from beginning of file)
	}

	// TODO: based on file config or opts if fresh created
	maxTxSize := maxTxSize(opts.maxTxEntries, opts.maxKeyLen)

	tx := mallocTx(opts.maxTxEntries, opts.maxKeyLen)
	txbs := make([]byte, maxTxSize)

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		err = tx.readFrom(txReader, make([]byte, maxInt(MaxKeyLen, bufLenTxh)))
		if err != nil {
			return nil, err
		}

		committedAlh = tx.Alh()
	}

	return &ImmuStore{
		txLog:              txLog,
		vLog:               vLog,
		cLog:               cLog,
		committedTxLogSize: committedTxLogSize,
		committedTxID:      committedTxID,
		committedAlh:       committedAlh,
		readOnly:           opts.readOnly,
		maxTxEntries:       opts.maxTxEntries,
		maxKeyLen:          opts.maxKeyLen,
		maxValueLen:        opts.maxValueLen,
		maxLinearProofLen:  opts.maxLinearProofLen,
		maxTxSize:          maxTxSize,
		_tx:                tx,
		_txbs:              txbs,
		_b:                 make([]byte, maxInt(4+opts.maxKeyLen+4, bufLenTxh)),
	}, nil
}

func maxTxSize(maxTxEntries, maxKeyLen int) int {
	return 2*8 + 2*sha256.Size + 4 + maxTxEntries*(4+maxKeyLen+4+sha256.Size) + 4
}

func (s *ImmuStore) Commit(entries []*KV) (id uint64, ts int64, alh [sha256.Size]byte, txh [sha256.Size]byte, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		err = ErrAlreadyClosed
		return
	}

	err = s.validateEntries(entries)
	if err != nil {
		return
	}

	// will overrite partially written and uncommitted data
	s.txLog.SetOffset(s.committedTxLogSize)

	s._tx.id = s.committedTxID + 1
	s._tx.ts = time.Now().Unix()
	s._tx.alh = s.committedAlh
	s._tx.nentries = len(entries)
	s._tx.htree.width = 0

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(s._tx.id))
	txSize += 8
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(s._tx.ts))
	txSize += 8
	copy(s._txbs[txSize:], s._tx.alh[:])
	txSize += sha256.Size
	binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(len(entries)))
	txSize += 4

	for i, e := range entries {
		//kv serialization into pre-allocated buffer
		kvSize := 0
		binary.BigEndian.PutUint32(s._b[kvSize:], uint32(len(e.Key)))
		kvSize += 4
		copy(s._b[kvSize:], e.Key)
		kvSize += len(e.Key)
		binary.BigEndian.PutUint32(s._b[kvSize:], uint32(len(e.Value)))
		kvSize += 4

		kvoff, _, aErr := s.vLog.Append(s._b[:kvSize])
		if aErr != nil {
			err = aErr
			return
		}

		_, _, aErr = s.vLog.Append(e.Value)
		if aErr != nil {
			err = aErr
			return
		}

		txe := s._tx.es[i]
		txe.keyLen = len(e.Key)
		txe.key = e.Key
		txe.valueLen = len(e.Value)
		txe.hvalue = sha256.Sum256(e.Value)
		txe.voff = kvoff

		eh := txe.digest()
		merkletree.AppendHash(s._tx.htree, &eh)
		s._tx.htree.width++

		// tx serialization into pre-allocated buffer
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(len(e.Key)))
		txSize += 4
		copy(s._txbs[txSize:], e.Key)
		txSize += len(e.Key)
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(len(e.Value)))
		txSize += 4
		copy(s._txbs[txSize:], txe.hvalue[:])
		txSize += sha256.Size
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.voff))
		txSize += 8
	}

	root := merkletree.Root(s._tx.htree)
	binary.BigEndian.PutUint64(s._b, s._tx.id)
	binary.BigEndian.PutUint64(s._b[8:], uint64(s._tx.ts))
	binary.BigEndian.PutUint32(s._b[16:], uint32(len(s._tx.es)))
	copy(s._b[20:], root[:])
	s._tx.txh = sha256.Sum256(s._b[:bufLenTxh])

	// tx serialization into pre-allocated buffer
	copy(s._txbs[txSize:], s._tx.txh[:])
	txSize += sha256.Size

	txOff, _, err := s.txLog.Append(s._txbs[:txSize])
	if err != nil {
		return
	}

	err = s.vLog.Flush()
	if err != nil {
		return
	}

	err = s.txLog.Flush()
	if err != nil {
		return
	}

	binary.BigEndian.PutUint64(s._b, uint64(txOff))
	binary.BigEndian.PutUint32(s._b[8:], uint32(txSize))
	_, _, err = s.cLog.Append(s._b[:cLogEntrySize])
	if err != nil {
		return
	}

	err = s.cLog.Flush()
	if err != nil {
		return
	}

	s.committedTxID++
	s.committedAlh = s._tx.Alh()
	s.committedTxLogSize += int64(txSize)

	return s._tx.id, s._tx.ts, s._tx.alh, s._tx.txh, nil
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

		if tx.id == txID {
			return path, nil
		}

		path = append(path, tx.alh, tx.txh)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return 0, ErrAlreadyClosed
	}

	return s.vLog.ReadAt(b, off)
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

	tx := mallocTx(s.maxTxEntries, s.maxKeyLen)
	b := make([]byte, maxInt(MaxKeyLen, bufLenTxh))

	return &TxReader{r: r, _tx: tx, _b: b}, nil
}

func (txr *TxReader) Read() (*Tx, error) {
	err := txr._tx.readFrom(txr.r, txr._b)
	if err != nil {
		return nil, err
	}

	if txr.alreadyRead && (txr.txID != txr._tx.id-1 || txr.alh != txr._tx.alh) {
		return nil, ErrorCorruptedTxData
	}

	txr.alreadyRead = true
	txr.txID = txr._tx.id
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

	err := s.vLog.Close()
	if err != nil {
		return err
	}

	err = s.txLog.Close()
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
