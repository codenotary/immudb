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
	"io"
	"os"

	"codenotary.io/immudb-v2/appendable"
)

var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")
var ErrCorruptedMetadata = errors.New("corrupted metadata")

const (
	metaCompressionFormat = "COMPRESSION_FORMAT"
	metaCompressionLevel  = "COMPRESSION_LEVEL"
	metaWrappedMeta       = "WRAPPED_METADATA"
)

type AppendableFile struct {
	f *os.File

	compressionFormat int
	compressionLevel  int

	metadata []byte

	readOnly bool
	synced   bool

	closed bool

	w *bufio.Writer

	baseOffset int64
	offset     int64
}

func Open(fileName string, opts *Options) (*AppendableFile, error) {
	if !validOptions(opts) {
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
	var baseOffset int64

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

		baseOffset = int64(4 + len(mBs))
	} else {
		r := bufio.NewReader(f)

		mLenBs := make([]byte, 4)
		_, err := r.Read(mLenBs)
		if err != nil {
			return nil, err
		}

		mBs := make([]byte, binary.BigEndian.Uint32(mLenBs))
		_, err = r.Read(mBs)
		if err != nil {
			return nil, err
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

		baseOffset = int64(4 + len(mBs))
	}

	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var w *bufio.Writer
	if !opts.readOnly {
		w = bufio.NewWriter(f)
	}

	return &AppendableFile{
		f:                 f,
		compressionFormat: compressionFormat,
		compressionLevel:  compressionLevel,
		metadata:          metadata,
		readOnly:          opts.readOnly,
		synced:            opts.synced,
		w:                 w,
		baseOffset:        baseOffset,
		offset:            off - baseOffset,
		closed:            false,
	}, nil
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
	stat, err := aof.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size() - aof.baseOffset, nil
}

func (aof *AppendableFile) Offset() int64 {
	return aof.offset
}

func (aof *AppendableFile) SetOffset(off int64) error {
	_, err := aof.f.Seek(off+aof.baseOffset, io.SeekStart)
	if err != nil {
		return err
	}

	aof.offset = off
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
	if aof.readOnly {
		return 0, 0, ErrReadOnly
	}

	if bs == nil {
		return 0, 0, ErrIllegalArguments
	}

	off = aof.offset

	if aof.compressionFormat == appendable.NoCompression {
		n, err = aof.w.Write(bs)
		aof.offset += int64(n)
		return
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

	n, err = aof.w.Write(bbLenBs)
	if err != nil {
		return
	}

	n, err = aof.w.Write(bb)
	if err != nil {
		return off, 4 + n, err
	}

	n += 4
	aof.offset += int64(n)

	return
}

func (aof *AppendableFile) ReadAt(bs []byte, off int64) (n int, err error) {
	if aof.closed {
		return 0, ErrAlreadyClosed
	}

	if bs == nil {
		return 0, ErrIllegalArguments
	}

	if aof.compressionFormat == appendable.NoCompression {
		return aof.f.ReadAt(bs, off+aof.baseOffset)
	}

	cOff, err := aof.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	defer aof.f.Seek(cOff, io.SeekStart)

	_, err = aof.f.Seek(off+aof.baseOffset, io.SeekStart)
	if err != nil {
		return 0, err
	}

	br := bufio.NewReader(aof.f)

	clenBs := make([]byte, 4)
	_, err = br.Read(clenBs)
	if err != nil {
		return 0, err
	}

	cBs := make([]byte, binary.BigEndian.Uint32(clenBs))
	_, err = io.ReadFull(br, cBs)
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
	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	err := aof.w.Flush()
	if err != nil {
		return err
	}

	if aof.synced {
		return aof.f.Sync()
	}

	return nil
}

func (aof *AppendableFile) Sync() error {
	if aof.closed {
		return ErrAlreadyClosed
	}

	if aof.readOnly {
		return ErrReadOnly
	}

	return aof.f.Sync()
}

func (aof *AppendableFile) Close() error {
	if aof.closed {
		return ErrAlreadyClosed
	}

	if !aof.readOnly {
		err := aof.Flush()
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
