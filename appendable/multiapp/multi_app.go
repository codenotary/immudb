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

	"codenotary.io/immudb-v2/appendable/singleapp"
	"codenotary.io/immudb-v2/cache"
)

var ErrorPathIsNotADirectory = errors.New("Path is not a directory")
var ErrIllegalArgument = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrReadOnly = errors.New("cannot append when openned in read-only mode")

const DefaultFileSize = 1 << 26 // 64Mb
const DefaultMaxOpenedFiles = 10
const DefaultFileMode = 0755

type Options struct {
	readOnly       bool
	synced         bool
	fileMode       os.FileMode
	fileSize       int
	fileExt        string
	metadata       []byte
	maxOpenedFiles int
}

func DefaultOptions() *Options {
	return &Options{
		readOnly:       false,
		synced:         true,
		fileMode:       DefaultFileMode,
		fileSize:       DefaultFileSize,
		fileExt:        "aof",
		maxOpenedFiles: DefaultMaxOpenedFiles,
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

func (opt *Options) SetMetadata(metadata []byte) *Options {
	opt.metadata = metadata
	return opt
}

func (opt *Options) SetFileSize(fileSize int) *Options {
	opt.fileSize = fileSize
	return opt
}

func (opt *Options) SetFileExt(fileExt string) *Options {
	opt.fileExt = fileExt
	return opt
}

func (opt *Options) SetMaxOpenedFiles(maxOpenedFiles int) *Options {
	opt.maxOpenedFiles = maxOpenedFiles
	return opt
}

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
	if opts == nil || opts.fileSize < 1 || opts.maxOpenedFiles < 1 || opts.fileExt == "" {
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

	fis, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var currAppID int64

	appendableOpts := singleapp.DefaultOptions().
		SetReadOnly(opts.readOnly).
		SetSynced(opts.synced).
		SetFileMode(opts.fileMode)

	if len(fis) > 0 {
		filename := fis[len(fis)-1].Name()
		appendableOpts.SetFilename(filename)
		currAppID, err = strconv.ParseInt(strings.TrimSuffix(filename, filepath.Ext(filename)), 10, 64)
		if err != nil {
			return nil, err
		}
	} else {
		appendableOpts.SetFilename(appendableName(appendableID(0, opts.fileSize), opts.fileExt))
		appendableOpts.SetMetadata(opts.metadata)
	}

	currApp, err := singleapp.Open(path, appendableOpts)
	if err != nil {
		return nil, err
	}

	cache, err := cache.NewLRUCache(opts.maxOpenedFiles)
	if err != nil {
		return nil, err
	}

	return &MultiFileAppendable{
		appendables: cache,
		currAppID:   currAppID,
		currApp:     currApp,
		path:        path,
		readOnly:    opts.readOnly,
		synced:      opts.synced,
		fileMode:    opts.fileMode,
		fileSize:    opts.fileSize,
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

func (mf *MultiFileAppendable) Metadata() []byte {
	return mf.currApp.Metadata()
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
		return 0, 0, ErrIllegalArgument
	}

	for n < len(bs) {
		available := mf.fileSize - int(mf.currApp.Offset())

		if available == 0 {
			_, ejectedApp, err := mf.appendables.Put(mf.currAppID, mf.currApp)
			if err != nil {
				return 0, 0, err
			}

			if ejectedApp != nil {
				err = ejectedApp.(*singleapp.AppendableFile).Close()
				if err != nil {
					return 0, 0, err
				}
			}

			mf.currAppID++
			currApp, err := mf.openAppendable(appendableName(mf.currAppID, mf.fileExt))
			if err != nil {
				return 0, 0, err
			}
			currApp.SetOffset(0)

			mf.currApp = currApp

			available = mf.fileSize
		}

		d := minInt(available, len(bs)-n)

		offn, _, err := mf.currApp.Append(bs[n : n+d])
		if err != nil {
			return 0, 0, err
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
		SetReadOnly(mf.readOnly).
		SetSynced(mf.synced).
		SetFileMode(mf.fileMode).
		SetFilename(appname).
		SetMetadata(mf.currApp.Metadata())

	return singleapp.Open(mf.path, appendableOpts)
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
				return 0, err
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

		if err != nil && err != io.EOF {
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
