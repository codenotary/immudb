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
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
)

var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("single-file appendable already closed")
var ErrReadOnly = errors.New("cannot append when opened in read-only mode")
var ErrCorruptedMetadata = errors.New("corrupted metadata")
var ErrBufferFull = errors.New("buffer full")

const (
	metaCompressionFormat = "COMPRESSION_FORMAT"
	metaCompressionLevel  = "COMPRESSION_LEVEL"
	metaWrappedMeta       = "WRAPPED_METADATA"
)

type AppendableFile struct {
	f              *os.File
	fileBaseOffset int64
	fileOffset     int64

	writeBuffer         []byte
	wbufFlushedOffset   int
	wbufUnwrittenOffset int

	readBufferSize int
	readOnly       bool
	retryableSync  bool
	autoSync       bool

	compressionFormat int
	compressionLevel  int

	metadata []byte

	closed bool

	mutex sync.Mutex
}

func Open(fileName string, opts *Options) (*AppendableFile, error) {
	if !opts.Valid() {
		return nil, ErrIllegalArguments
	}

	var flag int

	if opts.readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}

	_, err := os.Stat(fileName)
	notExist := os.IsNotExist(err)

	if err != nil && ((opts.readOnly && notExist) || !notExist) {
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

	if notExist {
		m := appendable.NewMetadata(nil)
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

		err = w.Flush()
		if err != nil {
			return nil, err
		}

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
		writeBuffer:       make([]byte, opts.writeBufferSize),
		readBufferSize:    opts.readBufferSize,
		compressionFormat: compressionFormat,
		compressionLevel:  compressionLevel,
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

	_, err := aof.f.Seek(aof.fileBaseOffset+newOffset, io.SeekStart)
	if err != nil {
		return err
	}

	aof.fileOffset = newOffset

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
			if aof.retryableSync && !aof.autoSync {
				// Sync must be called to free buffer space
				return n, ErrBufferFull
			}

			if aof.retryableSync && aof.autoSync {
				err = aof.sync()
				if err != nil {
					return
				}
			}

			if !aof.retryableSync {
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
	if off < aof.fileOffset {
		n, err = aof.f.ReadAt(bs, aof.fileBaseOffset+off)
	}

	pending := len(bs) - n

	if pending > 0 {
		readChunkSize := minInt(pending, aof.wbufUnwrittenOffset-aof.wbufFlushedOffset)

		if readChunkSize > 0 {
			copy(bs[n:], aof.writeBuffer[aof.wbufFlushedOffset:aof.wbufFlushedOffset+readChunkSize])
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

// flush writes buffered data into the underlying file.
// When retryableSync is used, the buffer space is released
// after sync succeeds to prevent data loss under unexpected conditions
func (aof *AppendableFile) flush() error {
	if aof.wbufUnwrittenOffset-aof.wbufFlushedOffset == 0 {
		// nothing to write
		return nil
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

	err = aof.f.Sync()
	if aof.retryableSync && err != nil {
		// prevent data lost when fsync fails
		// buffered data is going to be attempt to be re-written
		// in following flushing and sync calls.
		// Buffer space is not freed when there is an error during sync
		aof.fileOffset -= int64(aof.wbufFlushedOffset)
		aof.wbufFlushedOffset = 0
	}
	if aof.retryableSync && err == nil {
		// buffer space is freed
		aof.wbufFlushedOffset = 0
		aof.wbufUnwrittenOffset = 0
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
