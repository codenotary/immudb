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
package multiapp

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
)

var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")

const (
	metaFileSize    = "FILE_SIZE"
	metaWrappedMeta = "WRAPPED_METADATA"
)

type MultiFileAppendable struct {
	appendables *cache.LRUCache

	currAppID int64
	currApp   *singleapp.AppendableFile

	path     string
	readOnly bool
	synced   bool
	fileMode os.FileMode
	fileSize int
	fileExt  string

	closed bool
}

func Open(path string, opts *Options) (*MultiFileAppendable, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
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

	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var currAppID int64

	m := appendable.NewMetadata(nil)
	m.PutInt(metaFileSize, opts.fileSize)
	m.Put(metaWrappedMeta, opts.metadata)

	appendableOpts := singleapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithSynced(opts.synced).
		WithFileMode(opts.fileMode).
		WithCompressionFormat(opts.compressionFormat).
		WithCompresionLevel(opts.compressionLevel).
		WithMetadata(m.Bytes())

	var filename string

	if len(fis) > 0 {
		filename = fis[len(fis)-1].Name()

		currAppID, err = strconv.ParseInt(strings.TrimSuffix(filename, filepath.Ext(filename)), 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		filename = appendableName(appendableID(0, opts.fileSize), opts.fileExt)
	}

	currApp, err := singleapp.Open(filepath.Join(path, filename), appendableOpts)
	if err != nil {
		return nil, err
	}

	cache, err := cache.NewLRUCache(opts.maxOpenedFiles)
	if err != nil {
		return nil, err
	}

	fileSize, _ := appendable.NewMetadata(currApp.Metadata()).GetInt(metaFileSize)

	return &MultiFileAppendable{
		appendables: cache,
		currAppID:   currAppID,
		currApp:     currApp,
		path:        path,
		readOnly:    opts.readOnly,
		synced:      opts.synced,
		fileMode:    opts.fileMode,
		fileSize:    fileSize,
		fileExt:     opts.fileExt,
		closed:      false,
	}, nil
}

func appendableName(appID int64, ext string) string {
	return fmt.Sprintf("%08d.%s", appID, ext)
}

func appendableID(off int64, fileSize int) int64 {
	return off / int64(fileSize)
}

func (mf *MultiFileAppendable) CompressionFormat() int {
	return mf.currApp.CompressionFormat()
}

func (mf *MultiFileAppendable) CompressionLevel() int {
	return mf.currApp.CompressionLevel()
}

func (mf *MultiFileAppendable) Metadata() []byte {
	bs, _ := appendable.NewMetadata(mf.currApp.Metadata()).Get(metaWrappedMeta)
	return bs
}

func (mf *MultiFileAppendable) Size() (int64, error) {
	if mf.closed {
		return 0, ErrAlreadyClosed
	}
	currSize, err := mf.currApp.Size()
	if err != nil {
		return 0, err
	}

	return mf.currAppID*int64(mf.fileSize) + currSize, nil
}

func (mf *MultiFileAppendable) Append(bs []byte) (off int64, n int, err error) {
	if mf.closed {
		return 0, 0, ErrAlreadyClosed
	}

	if bs == nil {
		return 0, 0, ErrIllegalArguments
	}

	for n < len(bs) {
		available := mf.fileSize - int(mf.currApp.Offset())

		if available <= 0 {
			_, ejectedApp, err := mf.appendables.Put(mf.currAppID, mf.currApp)
			if err != nil {
				return off, n, err
			}

			if ejectedApp != nil {
				err = ejectedApp.(*singleapp.AppendableFile).Close()
				if err != nil {
					return off, n, err
				}
			}

			mf.currAppID++
			currApp, err := mf.openAppendable(appendableName(mf.currAppID, mf.fileExt))
			if err != nil {
				return off, n, err
			}
			currApp.SetOffset(0)

			mf.currApp = currApp

			available = mf.fileSize
		}

		var d int

		if mf.currApp.CompressionFormat() == appendable.NoCompression {
			d = minInt(available, len(bs)-n)
		} else {
			d = len(bs) - n
		}

		offn, _, err := mf.currApp.Append(bs[n : n+d])
		if err != nil {
			return off, n, err
		}

		if n == 0 {
			off = offn + mf.currAppID*int64(mf.fileSize)
		}

		n += d
	}

	return
}

func (mf *MultiFileAppendable) openAppendable(appname string) (*singleapp.AppendableFile, error) {
	appendableOpts := singleapp.DefaultOptions().
		WithReadOnly(mf.readOnly).
		WithSynced(mf.synced).
		WithFileMode(mf.fileMode).
		WithCompressionFormat(mf.currApp.CompressionFormat()).
		WithCompresionLevel(mf.currApp.CompressionLevel()).
		WithMetadata(mf.currApp.Metadata())

	return singleapp.Open(filepath.Join(mf.path, appname), appendableOpts)
}

func (mf *MultiFileAppendable) Offset() int64 {
	return mf.currAppID*int64(mf.fileSize) + mf.currApp.Offset()
}

func (mf *MultiFileAppendable) SetOffset(off int64) error {
	if mf.closed {
		return ErrAlreadyClosed
	}

	appID := appendableID(off, mf.fileSize)

	if mf.currAppID != appID {
		app, err := mf.openAppendable(appendableName(appID, mf.fileExt))
		if err != nil {
			return err
		}

		_, ejectedApp, err := mf.appendables.Put(appID, app)
		if err != nil {
			return err
		}

		if ejectedApp != nil {
			err = ejectedApp.(*singleapp.AppendableFile).Close()
			if err != nil {
				return err
			}
		}

		mf.currAppID = appID
		mf.currApp = app
	}

	return mf.currApp.SetOffset(off % int64(mf.fileSize))
}

func (mf *MultiFileAppendable) ReadAt(bs []byte, off int64) (int, error) {
	r := 0

	for r < len(bs) {
		offr := off + int64(r)

		appID := appendableID(offr, mf.fileSize)

		app, err := mf.appendables.Get(appID)

		if err != nil {
			if err != cache.ErrKeyNotFound {
				return r, err
			}

			app, err = mf.openAppendable(appendableName(appID, mf.fileExt))
			if err != nil {
				return r, err
			}

			_, ejectedApp, err := mf.appendables.Put(appID, app)
			if err != nil {
				return r, err
			}

			if ejectedApp != nil {
				err = ejectedApp.(*singleapp.AppendableFile).Close()
				if err != nil {
					return r, err
				}
			}
		}

		rn, err := app.(*singleapp.AppendableFile).ReadAt(bs[r:], offr%int64(mf.fileSize))
		r += rn

		if err == io.EOF && rn > 0 {
			continue
		}

		if err != nil {
			return r, err
		}
	}

	return r, nil
}

func (mf *MultiFileAppendable) Flush() error {
	if mf.closed {
		return ErrAlreadyClosed
	}

	err := mf.appendables.Apply(func(k interface{}, v interface{}) error {
		return v.(*singleapp.AppendableFile).Flush()
	})
	if err != nil {
		return err
	}

	return mf.currApp.Flush()
}

func (mf *MultiFileAppendable) Sync() error {
	if mf.closed {
		return ErrAlreadyClosed
	}

	err := mf.appendables.Apply(func(k interface{}, v interface{}) error {
		return v.(*singleapp.AppendableFile).Sync()
	})
	if err != nil {
		return err
	}

	return mf.currApp.Sync()
}

func (mf *MultiFileAppendable) Close() error {
	if mf.closed {
		return ErrAlreadyClosed
	}

	mf.closed = true

	err := mf.appendables.Apply(func(k interface{}, v interface{}) error {
		return v.(*singleapp.AppendableFile).Close()
	})
	if err != nil {
		return err
	}

	return mf.currApp.Close()
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
