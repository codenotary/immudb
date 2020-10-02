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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"codenotary.io/immudb-v2/appendable"
)

var ErrorPathIsNotADirectory = errors.New("Path is not a directory")
var ErrIllegalArgument = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")

const DefaultFileMode = 0644

const (
	metaCompressionFormat = "COMPRESSION_FORMAT"
	metaCompressionLevel  = "COMPRESSION_LEVEL"
	metaWrappedMeta       = "WRAPPED_METADATA"
)

const metadataLenLen = 4

type AppendableFile struct {
	f *os.File

	metadata    *appendable.Metadata
	metadataLen int

	readOnly bool
	synced   bool

	closed bool

	w *bufio.Writer

	offset int64
}

type Options struct {
	readOnly bool
	synced   bool
	fileMode os.FileMode
	filename string

	compressionFormat int
	compressionLevel  int

	metadata []byte
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:          false,
		synced:            true,
		fileMode:          DefaultFileMode,
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

func (opt *Options) SetFilename(filename string) *Options {
	opt.filename = filename
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

func (opt *Options) SetMetadata(metadata []byte) *Options {
	opt.metadata = metadata
	return opt
}

func Open(path string, opts *Options) (*AppendableFile, error) {
	if opts == nil {
		return nil, ErrIllegalArgument
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) || opts.readOnly {
			return nil, err
		}

		err = os.Mkdir(path, opts.fileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	var flag int

	if opts.readOnly {
		flag = os.O_RDONLY
	} else {
		flag = os.O_CREATE | os.O_RDWR
	}

	name := opts.filename
	if name == "" {
		name = fmt.Sprintf("%d.idb", time.Now().Unix())
	}
	fileName := filepath.Join(path, name)

	finfo, err = os.Stat(fileName)
	notExist := os.IsNotExist(err)

	if err != nil && ((opts.readOnly && notExist) || !notExist) {
		return nil, err
	}

	f, err := os.OpenFile(fileName, flag, opts.fileMode)
	if err != nil {
		return nil, err
	}

	var w *bufio.Writer
	if !opts.readOnly {
		w = bufio.NewWriter(f)
	}

	var metadata *appendable.Metadata
	var metadataLen int

	if notExist {
		metadata = appendable.NewMetadata(nil)
		metadata.PutInt(metaCompressionFormat, int(opts.compressionFormat))
		metadata.PutInt(metaCompressionLevel, int(opts.compressionLevel))
		metadata.Put(metaWrappedMeta, opts.metadata)

		metadataBs := metadata.Bytes()
		metadataLen = len(metadataBs)

		metaLenBs := make([]byte, 4)
		binary.BigEndian.PutUint32(metaLenBs, uint32(metadataLen))
		_, err := w.Write(metaLenBs)
		if err != nil {
			return nil, err
		}

		_, err = w.Write(metadataBs)
		if err != nil {
			return nil, err
		}

		err = w.Flush()
		if err != nil {
			return nil, err
		}
	} else {
		r := bufio.NewReader(f)

		metaLenBs := make([]byte, 4)
		_, err := r.Read(metaLenBs)
		if err != nil {
			return nil, err
		}

		metadataBs := make([]byte, binary.BigEndian.Uint32(metaLenBs))
		_, err = r.Read(metadataBs)
		if err != nil {
			return nil, err
		}

		metadata = appendable.NewMetadata(metadataBs)
		metadataLen = len(metadataBs)
	}

	off, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}

	return &AppendableFile{
		f:           f,
		metadata:    metadata,
		metadataLen: metadataLen,
		readOnly:    opts.readOnly,
		synced:      opts.synced,
		w:           w,
		offset:      off - baseOffset(metadataLen),
		closed:      false,
	}, nil
}

func baseOffset(metadataLen int) int64 {
	return int64(metadataLenLen + metadataLen)
}

func (aof *AppendableFile) Metadata() []byte {
	m, _ := aof.metadata.Get(metaWrappedMeta)
	return m
}

func (aof *AppendableFile) Size() (int64, error) {
	stat, err := aof.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size() - baseOffset(aof.metadataLen), nil
}

func (aof *AppendableFile) Offset() int64 {
	return aof.offset
}

func (aof *AppendableFile) SetOffset(off int64) error {
	_, err := aof.f.Seek(off+baseOffset(aof.metadataLen), os.SEEK_SET)
	if err != nil {
		return err
	}

	aof.offset = off
	return nil
}

func (aof *AppendableFile) Append(bs []byte) (off int64, n int, err error) {
	if aof.readOnly {
		return 0, 0, ErrReadOnly
	}

	if bs == nil {
		return 0, 0, ErrIllegalArgument
	}

	n, err = aof.w.Write(bs)

	off = aof.offset

	aof.offset += int64(n)

	return
}

func (aof *AppendableFile) ReadAt(bs []byte, off int64) (int, error) {
	return aof.f.ReadAt(bs, off+baseOffset(aof.metadataLen))
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
