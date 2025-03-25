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

package singleapp

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/fileutils"
)

var ErrorPathIsNotADirectory = errors.New("singleapp: path is not a directory")
var ErrIllegalArguments = errors.New("singleapp: illegal arguments")
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrAlreadyClosed = errors.New("singleapp: already closed")
var ErrReadOnly = errors.New("singleapp: read-only mode")
var ErrCorruptedMetadata = errors.New("singleapp: corrupted metadata")
var ErrBufferFull = errors.New("singleapp: buffer full")
var ErrNegativeOffset = errors.New("singleapp: negative offset")

const (
	metaPreallocSize      = "PREALLOC_SIZE"
	metaCompressionFormat = "COMPRESSION_FORMAT"
	metaCompressionLevel  = "COMPRESSION_LEVEL"
	metaWrappedMeta       = "WRAPPED_METADATA"
)

var _ appendable.Appendable = (*AppendableFile)(nil)

type AppendableFile struct {
	f              *os.File
	fileBaseOffset int64
	fileOffset     int64
	seekRequired   bool

	writeBuffer         []byte
	wbufFlushedOffset   int
	wbufUnwrittenOffset int

	readBufferSize int
	readOnly       bool
	retryableSync  bool
	autoSync       bool

	compressionFormat int
	compressionLevel  int

	preallocSize int

	metadata []byte

	closed bool

	mutex sync.Mutex
}

func Open(fileName string, opts *Options) (*AppendableFile, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	var flag int

	if opts.readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}

	_, err = os.Stat(fileName)
	notExist := os.IsNotExist(err)

	if err != nil && ((opts.readOnly && notExist) || (!opts.createIfNotExists && notExist) || !notExist) {
		return nil, err
	}

	f, err := os.OpenFile(fileName, flag, opts.fileMode)
	if err != nil {
		return nil, err
	}

	var metadata []byte
	var compressionFormat int
	var compressionLevel int
	var fileBaseOffset int64

	var preallocSize int

	if notExist {
		m := appendable.NewMetadata(nil)
		m.PutInt(metaPreallocSize, opts.preallocSize)
		m.PutInt(metaCompressionFormat, opts.compressionFormat)
		m.PutInt(metaCompressionLevel, opts.compressionLevel)
		m.Put(metaWrappedMeta, opts.metadata)

		mBs := m.Bytes()
		mLenBs := make([]byte, 4)
		binary.BigEndian.PutUint32(mLenBs, uint32(len(mBs)))

		w := bufio.NewWriter(f)

		_, err := w.Write(mLenBs)
		if err != nil {
			return nil, err
		}

		_, err = w.Write(mBs)
		if err != nil {
			return nil, err
		}

		preallocBs := make([]byte, 4096)
		preallocated := 0

		for preallocated < opts.preallocSize {
			n, err := w.Write(preallocBs[:minInt(len(preallocBs), opts.preallocSize-preallocated)])
			if err != nil {
				return nil, err
			}

			preallocated += n
		}

		err = w.Flush()
		if err != nil {
			return nil, err
		}

		err = f.Sync()
		if err != nil {
			return nil, err
		}

		err = fileutils.SyncDir(filepath.Dir(fileName))
		if err != nil {
			return nil, err
		}

		preallocSize = opts.preallocSize
		compressionFormat = opts.compressionFormat
		compressionLevel = opts.compressionLevel
		metadata = opts.metadata

		fileBaseOffset = int64(4 + len(mBs))
	} else {
		r := bufio.NewReader(f)

		mLenBs := make([]byte, 4)
		_, err := r.Read(mLenBs)
		if err != nil {
			return nil, ErrCorruptedMetadata
		}

		mBs := make([]byte, binary.BigEndian.Uint32(mLenBs))
		_, err = r.Read(mBs)
		if err != nil {
			return nil, ErrCorruptedMetadata
		}

		m := appendable.NewMetadata(mBs)

		preallocSz, ok := m.GetInt(metaCompressionFormat)
		if ok {
			preallocSize = preallocSz
		}

		cf, ok := m.GetInt(metaCompressionFormat)
		if !ok {
			return nil, ErrCorruptedMetadata
		}
		compressionFormat = cf

		cl, ok := m.GetInt(metaCompressionLevel)
		if !ok {
			return nil, ErrCorruptedMetadata
		}
		compressionLevel = cl

		metadata, ok = m.Get(metaWrappedMeta)
		if !ok {
			return nil, ErrCorruptedMetadata
		}

		fileBaseOffset = int64(4 + len(mBs))
	}

	fileOffset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return &AppendableFile{
		f:                 f,
		fileBaseOffset:    fileBaseOffset,
		fileOffset:        fileOffset - fileBaseOffset,
		writeBuffer:       opts.writeBuffer,
		readBufferSize:    opts.readBufferSize,
		compressionFormat: compressionFormat,
		compressionLevel:  compressionLevel,
		preallocSize:      preallocSize,
		metadata:          metadata,
		readOnly:          opts.readOnly,
		retryableSync:     opts.retryableSync,
		autoSync:          opts.autoSync,
		closed:            false,
	}, nil
}

func (aof *AppendableFile) Copy(dstPath string) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	err = aof.flush()
	if err != nil {
		return err
	}

	aof.seekRequired = true

	_, err = aof.f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.Copy(dstFile, aof.f)
	if err != nil {
		return err
	}

	return dstFile.Sync()
}

func (aof *AppendableFile) CompressionFormat() int {
	return aof.compressionFormat
}

func (aof *AppendableFile) CompressionLevel() int {
	return aof.compressionLevel
}

func (aof *AppendableFile) Metadata() []byte {
	return aof.metadata
}

func (aof *AppendableFile) Size() (int64, error) {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return 0, ErrAlreadyClosed
	}

	return aof.offset(), nil
}

func (aof *AppendableFile) Offset() int64 {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	return aof.offset()
}

func (aof *AppendableFile) offset() int64 {
	return aof.fileOffset + int64(aof.wbufUnwrittenOffset-aof.wbufFlushedOffset)
}

func (aof *AppendableFile) SetOffset(newOffset int64) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	if newOffset < 0 {
		return ErrNegativeOffset
	}

	currOffset := aof.offset()

	if newOffset > currOffset {
		return fmt.Errorf("%w: provided offset %d is bigger than current one %d", ErrIllegalArguments, newOffset, currOffset)
	}

	if newOffset == currOffset {
		return nil
	}

	if newOffset >= aof.fileOffset {
		//in-mem change
		aof.wbufUnwrittenOffset -= int(currOffset - newOffset)
		return nil
	}

	aof.fileOffset = newOffset
	aof.seekRequired = true

	// discard in-memory data
	aof.wbufFlushedOffset = 0
	aof.wbufUnwrittenOffset = 0

	return nil
}

func (aof *AppendableFile) DiscardUpto(off int64) error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.offset() < off {
		return fmt.Errorf("%w: discard beyond existent data boundaries", ErrIllegalArguments)
	}

	return nil
}

func (aof *AppendableFile) writer(w io.Writer) (cw io.Writer, err error) {
	switch aof.compressionFormat {
	case appendable.FlateCompression:
		cw, err = flate.NewWriter(w, aof.compressionLevel)
	case appendable.GZipCompression:
		cw, err = gzip.NewWriterLevel(w, aof.compressionLevel)
	case appendable.LZWCompression:
		cw = lzw.NewWriter(w, lzw.MSB, 8)
	case appendable.ZLibCompression:
		cw, err = zlib.NewWriterLevel(w, aof.compressionLevel)
	}
	return
}

func (aof *AppendableFile) reader(r io.Reader) (reader io.ReadCloser, err error) {
	switch aof.compressionFormat {
	case appendable.FlateCompression:
		reader = flate.NewReader(r)
	case appendable.GZipCompression:
		reader, err = gzip.NewReader(r)
	case appendable.LZWCompression:
		reader = lzw.NewReader(r, lzw.MSB, 8)
	case appendable.ZLibCompression:
		reader, err = zlib.NewReader(r)
	}
	return
}

func (aof *AppendableFile) Append(bs []byte) (off int64, n int, err error) {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return 0, 0, ErrAlreadyClosed
	}

	if aof.readOnly {
		return 0, 0, ErrReadOnly
	}

	if len(bs) == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off = aof.offset()

	if aof.compressionFormat == appendable.NoCompression {
		n, err = aof.write(bs)
		return off, n, err
	}

	var b bytes.Buffer

	w, err := aof.writer(&b)
	if err != nil {
		return 0, 0, err
	}

	_, err = w.Write(bs)
	if err != nil {
		return 0, 0, err
	}

	w.(io.Closer).Close()

	bb := b.Bytes()

	bbLenBs := make([]byte, 4)
	binary.BigEndian.PutUint32(bbLenBs, uint32(len(bb)))

	n, err = aof.write(bbLenBs)
	if err != nil {
		return off, n, err
	}

	n, err = aof.write(bb)

	return off, n + 4, err
}

func (aof *AppendableFile) write(bs []byte) (n int, err error) {
	for n < len(bs) {
		available := len(aof.writeBuffer) - aof.wbufUnwrittenOffset

		if available == 0 {
			if aof.retryableSync {
				// Sync must be called to free buffer space
				if !aof.autoSync {
					return n, ErrBufferFull
				}

				// auto-sync is enabled
				err = aof.sync()
				if err != nil {
					return
				}
			} else {
				err = aof.flush()
				if err != nil {
					return
				}
			}

			available = len(aof.writeBuffer)
		}

		writeChunkSize := minInt(len(bs)-n, available)

		copy(aof.writeBuffer[aof.wbufUnwrittenOffset:], bs[n:n+writeChunkSize])
		aof.wbufUnwrittenOffset += writeChunkSize

		n += writeChunkSize
	}

	return
}

func (aof *AppendableFile) readAt(bs []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, ErrNegativeOffset
	}

	if off > aof.offset() {
		return 0, io.EOF
	}

	// boff is the offset to employ when reading from the buffer
	var boff int

	if off < aof.fileOffset {
		n, err = aof.f.ReadAt(bs, aof.fileBaseOffset+off)
	} else {
		boff = int(off - aof.fileOffset)
	}

	pending := len(bs) - n

	if pending > 0 {
		available := (aof.wbufUnwrittenOffset - aof.wbufFlushedOffset) - boff
		readChunkSize := minInt(pending, available)

		if readChunkSize > 0 {
			copy(bs[n:], aof.writeBuffer[aof.wbufFlushedOffset+boff:aof.wbufFlushedOffset+boff+readChunkSize])
			n += readChunkSize
		}

		if readChunkSize == pending {
			err = nil
		} else {
			err = io.EOF
		}
	}

	return
}

func (aof *AppendableFile) ReadAt(bs []byte, off int64) (n int, err error) {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return 0, ErrAlreadyClosed
	}

	if bs == nil {
		return 0, ErrIllegalArguments
	}

	if aof.compressionFormat == appendable.NoCompression {
		return aof.readAt(bs, off)
	}

	clenBs := make([]byte, 4)
	_, err = aof.readAt(clenBs, off)
	if err != nil {
		return 0, err
	}

	cBs := make([]byte, binary.BigEndian.Uint32(clenBs))
	_, err = aof.readAt(cBs, off+4)
	if err != nil {
		return 0, err
	}

	r, err := aof.reader(bytes.NewReader(cBs))
	if err != nil {
		return 0, err
	}
	defer r.Close()

	var buf bytes.Buffer
	buf.ReadFrom(r)
	rbs := buf.Bytes()

	n = minInt(len(rbs), len(bs))

	copy(bs, rbs[:n])

	if n < len(bs) {
		err = io.EOF
	}

	return
}

func (aof *AppendableFile) SwitchToReadOnlyMode() error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	// write buffer must be freed
	err := aof.flush()
	if err != nil {
		return err
	}

	if aof.retryableSync {
		// syncing is required to free the write buffer with retryable sync
		err := aof.sync()
		if err != nil {
			return err
		}
	}

	aof.writeBuffer = nil
	aof.readOnly = true

	return nil
}

func (aof *AppendableFile) Flush() error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	return aof.flush()
}

func (aof *AppendableFile) seekIfRequired() error {
	if !aof.seekRequired {
		return nil
	}

	_, err := aof.f.Seek(aof.fileBaseOffset+aof.fileOffset, io.SeekStart)
	if err != nil {
		return err
	}

	aof.seekRequired = false

	return nil
}

// flush writes buffered data into the underlying file.
// When retryableSync is used, the buffer space is released
// after sync succeeds to prevent data loss under unexpected conditions
func (aof *AppendableFile) flush() error {
	if aof.wbufUnwrittenOffset-aof.wbufFlushedOffset == 0 {
		// nothing to write
		return nil
	}

	// ensure that the file is written at the expected location
	err := aof.seekIfRequired()
	if err != nil {
		return err
	}

	n, err := aof.f.Write(aof.writeBuffer[aof.wbufFlushedOffset:aof.wbufUnwrittenOffset])

	aof.fileOffset += int64(n)
	aof.wbufFlushedOffset += n

	if err != nil {
		return err
	}

	if !aof.retryableSync {
		// free buffer space
		aof.wbufFlushedOffset = 0
		aof.wbufUnwrittenOffset = 0
	}

	return nil
}

func (aof *AppendableFile) Sync() error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	return aof.sync()
}

func (aof *AppendableFile) sync() error {
	err := aof.flush()
	if err != nil {
		return err
	}

	if aof.preallocSize == 0 {
		err = aof.f.Sync()
	} else {
		err = fileutils.Fdatasync(aof.f)
	}

	if !aof.retryableSync {
		return err
	}

	// retryableSync
	// Buffer space is not freed when there is an error during sync

	// prevent data lost when fsync fails
	// buffered data may be re-written in following
	// flushing and syncing calls.

	if err == nil {
		// buffer space is freed
		aof.wbufFlushedOffset = 0
		aof.wbufUnwrittenOffset = 0
	} else {
		aof.fileOffset -= int64(aof.wbufFlushedOffset)
		aof.seekRequired = true

		aof.wbufFlushedOffset = 0
	}

	return err
}

func (aof *AppendableFile) Close() error {
	aof.mutex.Lock()
	defer aof.mutex.Unlock()

	if aof.closed {
		return ErrAlreadyClosed
	}

	if !aof.readOnly {
		err := aof.flush()
		if err != nil {
			return err
		}
	}

	aof.closed = true

	return aof.f.Close()
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
