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
package appendable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var ErrorPathIsNotADirectory = errors.New("Path is not a directory")
var ErrIllegalArgument = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")

const DefaultFileMode = 0644

const metadataLenLen = 4

type AppendableFile struct {
	f *os.File

	metadata []byte

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
	metadata []byte
}

func DefaultOptions() *Options {
	return &Options{
		readOnly: false,
		synced:   true,
		fileMode: DefaultFileMode,
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

		err = os.Mkdir(path, 0700)
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
		name = fmt.Sprintf("%d.aof", time.Now().Unix())
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

	metadata := opts.metadata

	b := make([]byte, 4) // buff for metatada len

	if notExist {
		binary.BigEndian.PutUint32(b, uint32(len(metadata)))
		_, err := w.Write(b)
		if err != nil {
			return nil, err
		}

		if opts.metadata != nil {
			_, err = w.Write(opts.metadata)
			if err != nil {
				return nil, err
			}
		}

		err = w.Flush()
		if err != nil {
			return nil, err
		}
	} else {
		r := bufio.NewReader(f)

		_, err := r.Read(b)
		if err != nil {
			return nil, err
		}

		metadataLen := binary.BigEndian.Uint32(b)
		metadata = make([]byte, metadataLen)

		_, err = r.Read(metadata)
		if err != nil {
			return nil, err
		}
	}

	off, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		return nil, err
	}

	return &AppendableFile{
		f:        f,
		metadata: metadata,
		readOnly: opts.readOnly,
		synced:   opts.synced,
		w:        w,
		offset:   off - metadataLenLen - int64(len(metadata)),
		closed:   false,
	}, nil
}

func (aof *AppendableFile) Metadata() []byte {
	return aof.metadata
}

func (aof *AppendableFile) Size() (int64, error) {
	stat, err := aof.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size() - int64(metadataLenLen+len(aof.metadata)), nil
}

func (aof *AppendableFile) SetOffset(off int64) error {
	_, err := aof.f.Seek(off+int64(metadataLenLen+len(aof.metadata)), os.SEEK_SET)
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

	n, err = aof.w.Write(bs)

	off = aof.offset

	aof.offset += int64(n)

	return
}

func (aof *AppendableFile) ReadAt(bs []byte, off int64) (int, error) {
	return aof.f.ReadAt(bs, off+int64(metadataLenLen+len(aof.metadata)))
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

	return nil
}
