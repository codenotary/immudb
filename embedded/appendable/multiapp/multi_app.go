/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package multiapp

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/fileutils"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
)

var ErrorPathIsNotADirectory = errors.New("multiapp: path is not a directory")
var ErrIllegalArguments = errors.New("multiapp: illegal arguments")
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrAlreadyClosed = errors.New("multiapp: already closed")
var ErrReadOnly = errors.New("multiapp: read-only mode")

const (
	metaFileSize    = "FILE_SIZE"
	metaWrappedMeta = "WRAPPED_METADATA"
)

//---------------------------------------------------------

var _ appendable.Appendable = (*MultiFileAppendable)(nil)

type MultiFileAppendableHooks interface {
	// Hook to open underlying appendable.
	// If needsWriteAccess is set to true, this appendable must be a single file appendable
	OpenAppendable(options *singleapp.Options, appname string, needsWriteAccess bool) (appendable.Appendable, error)

	// Hook to open the last underlying appendable that's available
	OpenInitialAppendable(opts *Options, singleAppOpts *singleapp.Options) (app appendable.Appendable, appID int64, err error)
}

type DefaultMultiFileAppendableHooks struct {
	path string
}

func (d *DefaultMultiFileAppendableHooks) OpenInitialAppendable(opts *Options, singleAppOpts *singleapp.Options) (app appendable.Appendable, appID int64, err error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, 0, err
	}

	var filename string

	if len(entries) > 0 {
		filename = entries[len(entries)-1].Name()

		appID, err = strconv.ParseInt(strings.TrimSuffix(filename, filepath.Ext(filename)), 10, 64)
		if err != nil {
			return nil, 0, err
		}
	} else {
		appID = 0
		filename = appendableName(appendableID(0, opts.fileSize), opts.fileExt)
	}

	app, err = d.OpenAppendable(singleAppOpts, filename, true)
	if err != nil {
		return nil, 0, err
	}

	return app, appID, nil
}

func (d *DefaultMultiFileAppendableHooks) OpenAppendable(options *singleapp.Options, appname string, needsWriteAccess bool) (appendable.Appendable, error) {
	return singleapp.Open(filepath.Join(d.path, appname), options)
}

type MultiFileAppendable struct {
	appendables appendableCache

	currAppID int64
	currApp   appendable.Appendable

	path           string
	readOnly       bool
	retryableSync  bool
	autoSync       bool
	fileMode       os.FileMode
	fileSize       int
	fileExt        string
	readBufferSize int
	prealloc       bool

	writeBuffer []byte // shared write-buffer only used by active appendable

	closed bool

	hooks MultiFileAppendableHooks

	mutex sync.Mutex
}

func Open(path string, opts *Options) (*MultiFileAppendable, error) {
	return OpenWithHooks(path, &DefaultMultiFileAppendableHooks{
		path: path,
	}, opts)
}

func OpenWithHooks(path string, hooks MultiFileAppendableHooks, opts *Options) (*MultiFileAppendable, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
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

		err = fileutils.SyncDir(path, filepath.Dir(path))
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	m := appendable.NewMetadata(nil)
	m.PutInt(metaFileSize, opts.fileSize)
	m.Put(metaWrappedMeta, opts.metadata)

	var writeBuffer []byte

	if !opts.readOnly {
		// write buffer is only needed when appendable is not opened in read-only mode
		writeBuffer = make([]byte, opts.GetWriteBufferSize())
	}

	appendableOpts := singleapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithRetryableSync(opts.retryableSync).
		WithAutoSync(opts.autoSync).
		WithFileMode(opts.fileMode).
		WithCompressionFormat(opts.compressionFormat).
		WithCompresionLevel(opts.compressionLevel).
		WithReadBufferSize(opts.readBufferSize).
		WithWriteBuffer(writeBuffer).
		WithMetadata(m.Bytes())

	if opts.prealloc {
		appendableOpts.WithPreallocSize(opts.fileSize)
	}

	currApp, currAppID, err := hooks.OpenInitialAppendable(opts, appendableOpts)
	if err != nil {
		return nil, err
	}

	cache, err := cache.NewCache(opts.maxOpenedFiles)
	if err != nil {
		return nil, err
	}

	fileSize, _ := appendable.NewMetadata(currApp.Metadata()).GetInt(metaFileSize)

	return &MultiFileAppendable{
		appendables:    appendableCache{cache: cache},
		currAppID:      currAppID,
		currApp:        currApp,
		path:           path,
		readOnly:       opts.readOnly,
		retryableSync:  opts.retryableSync,
		autoSync:       opts.autoSync,
		fileMode:       opts.fileMode,
		fileSize:       fileSize,
		fileExt:        opts.fileExt,
		readBufferSize: opts.readBufferSize,
		prealloc:       opts.prealloc,
		writeBuffer:    writeBuffer,
		closed:         false,
		hooks:          hooks,
	}, nil
}

func appendableName(appID int64, ext string) string {
	return fmt.Sprintf("%08d.%s", appID, ext)
}

func appendableID(off int64, fileSize int) int64 {
	return off / int64(fileSize)
}

func (mf *MultiFileAppendable) Copy(dstPath string) error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if !mf.readOnly {
		err := mf.sync()
		if err != nil {
			return err
		}
	}

	err := os.MkdirAll(dstPath, mf.fileMode)
	if err != nil {
		return err
	}

	entries, err := os.ReadDir(mf.path)
	if err != nil {
		return err
	}

	for _, e := range entries {
		_, err = copyFile(path.Join(mf.path, e.Name()), path.Join(dstPath, e.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(srcPath, dstPath string) (int64, error) {
	dstFile, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	return io.Copy(dstFile, srcFile)
}

func (mf *MultiFileAppendable) CompressionFormat() int {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	return mf.currApp.CompressionFormat()
}

func (mf *MultiFileAppendable) CompressionLevel() int {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	return mf.currApp.CompressionLevel()
}

func (mf *MultiFileAppendable) Metadata() []byte {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	bs, _ := appendable.NewMetadata(mf.currApp.Metadata()).Get(metaWrappedMeta)
	return bs
}

func (mf *MultiFileAppendable) Size() (int64, error) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

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
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return 0, 0, ErrAlreadyClosed
	}

	if mf.readOnly {
		return 0, 0, ErrReadOnly
	}

	if len(bs) == 0 {
		return 0, 0, ErrIllegalArguments
	}

	for n < len(bs) {
		available := mf.fileSize - int(mf.currApp.Offset())

		if available <= 0 {
			// by switching to read-only mode, the write buffer is freed
			err = mf.currApp.SwitchToReadOnlyMode()
			if err != nil {
				return off, n, err
			}

			_, ejectedApp, err := mf.appendables.Put(mf.currAppID, mf.currApp)
			if err != nil {
				return off, n, err
			}

			if ejectedApp != nil {
				metricsCacheEvicted.Inc()
				err = ejectedApp.Close()
				if err != nil {
					return off, n, err
				}

			}

			mf.currAppID++
			currApp, err := mf.openAppendable(appendableName(mf.currAppID, mf.fileExt), true, true)
			if err != nil {
				return off, n, err
			}

			mf.currApp = currApp

			err = currApp.SetOffset(0)
			if err != nil {
				return off, n, err
			}

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

func (mf *MultiFileAppendable) openAppendable(appname string, createIfNotExists, activeChunk bool) (appendable.Appendable, error) {
	appendableOpts := singleapp.DefaultOptions().
		WithReadOnly(mf.readOnly).
		WithRetryableSync(mf.retryableSync).
		WithAutoSync(mf.autoSync).
		WithFileMode(mf.fileMode).
		WithCreateIfNotExists(createIfNotExists).
		WithReadBufferSize(mf.readBufferSize).
		WithCompressionFormat(mf.currApp.CompressionFormat()).
		WithCompresionLevel(mf.currApp.CompressionLevel()).
		WithMetadata(mf.currApp.Metadata())

	if mf.prealloc {
		appendableOpts.WithPreallocSize(mf.fileSize)
	}

	if activeChunk && !mf.readOnly {
		appendableOpts.WithWriteBuffer(mf.writeBuffer)
	}

	return mf.hooks.OpenAppendable(appendableOpts, appname, activeChunk)
}

func (mf *MultiFileAppendable) Offset() int64 {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	return mf.offset()
}

func (mf *MultiFileAppendable) offset() int64 {
	return mf.currAppID*int64(mf.fileSize) + mf.currApp.Offset()
}

func (mf *MultiFileAppendable) SetOffset(off int64) error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if mf.readOnly {
		return ErrReadOnly
	}

	currOffset := mf.offset()

	if off > currOffset {
		return fmt.Errorf("%w: provided offset %d is bigger than current one %d", ErrIllegalArguments, off, currOffset)
	}

	if off == currOffset {
		return nil
	}

	appID := appendableID(off, mf.fileSize)

	// given the new offset is lower than the current one, it means
	// either appID ==  mf.currAppID or appID < mf.currAppID must hold

	if mf.currAppID != appID {

		// Head might have moved back, this means that all
		// chunks that follow are no longer valid (will be overwritten anyway).
		// We also must flush / close current chunk since it will be reopened.
		for id := appID; id < mf.currAppID; id++ {
			app, err := mf.appendables.Pop(id)
			if errors.Is(err, cache.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			err = app.Close()
			if err != nil {
				return err
			}
		}

		// close current appendable as it's not present in the cache
		err := mf.currApp.Close()
		if err != nil {
			return err
		}

		app, err := mf.openAppendable(appendableName(appID, mf.fileExt), false, true)
		if err != nil {
			if os.IsNotExist(err) {
				return io.EOF
			}
			return err
		}

		mf.currAppID = appID
		mf.currApp = app
	}

	return mf.currApp.SetOffset(off % int64(mf.fileSize))
}

func (mf *MultiFileAppendable) DiscardUpto(off int64) error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if mf.offset() < off {
		return fmt.Errorf("%w: discard beyond existent data boundaries", ErrIllegalArguments)
	}

	appID := appendableID(off, mf.fileSize)

	var dirSyncNeeded bool

	for i := int64(0); i < appID; i++ {
		if i == mf.currAppID {
			break
		}

		app, err := mf.appendables.Pop(i)
		if err == nil {
			err = app.Close()
			if err != nil {
				return err
			}
		}

		appFile := filepath.Join(mf.path, appendableName(i, mf.fileExt))
		err = os.Remove(appFile)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		dirSyncNeeded = true
	}

	if dirSyncNeeded {
		err := fileutils.SyncDir(mf.path)
		if err != nil {
			return err
		}
	}

	return nil
}

func (mf *MultiFileAppendable) appendableFor(off int64) (appendable.Appendable, error) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return nil, ErrAlreadyClosed
	}

	appID := appendableID(off, mf.fileSize)

	if appID == mf.currAppID {
		metricsCacheHit.Inc()
		return mf.currApp, nil
	}

	app, err := mf.appendables.Get(appID)

	if err != nil {
		if !errors.Is(err, cache.ErrKeyNotFound) {
			return nil, err
		}

		metricsCacheMiss.Inc()

		app, err = mf.openAppendable(appendableName(appID, mf.fileExt), false, false)
		if err != nil {
			return nil, err
		}

		_, ejectedApp, err := mf.appendables.Put(appID, app)
		if err != nil {
			return nil, err
		}

		if ejectedApp != nil {
			metricsCacheEvicted.Inc()
			err = ejectedApp.Close()
			if err != nil {
				return nil, err
			}
		}
	} else {
		metricsCacheHit.Inc()
	}

	return app, nil
}

func (mf *MultiFileAppendable) ReadAt(bs []byte, off int64) (int, error) {
	if len(bs) == 0 {
		return 0, ErrIllegalArguments
	}

	metricsReads.Inc()

	r := 0

	for r < len(bs) {
		offr := off + int64(r)

		app, err := mf.appendableFor(offr)
		if err != nil {
			metricsReadBytes.Add(float64(r))

			if os.IsNotExist(err) {
				return r, io.EOF
			}

			metricsReadErrors.Inc()
			return r, err
		}

		rn, err := app.ReadAt(bs[r:], offr%int64(mf.fileSize))
		r += rn

		if errors.Is(err, io.EOF) {
			if rn > 0 {
				continue
			}

			metricsReadBytes.Add(float64(r))
			return r, err
		}

		if err != nil {
			metricsReadBytes.Add(float64(r))
			metricsReadErrors.Inc()
			return r, err
		}
	}

	metricsReadBytes.Add(float64(r))
	return r, nil
}

func (mf *MultiFileAppendable) SwitchToReadOnlyMode() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if mf.readOnly {
		return ErrReadOnly
	}

	// only current appendable needs to be switched to read-only mode
	err := mf.currApp.SwitchToReadOnlyMode()
	if err != nil {
		return err
	}

	mf.writeBuffer = nil
	mf.readOnly = true

	return nil
}

func (mf *MultiFileAppendable) Flush() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if mf.readOnly {
		return ErrReadOnly
	}

	return mf.currApp.Flush()
}

func (mf *MultiFileAppendable) Sync() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	if mf.readOnly {
		return ErrReadOnly
	}

	return mf.sync()
}

func (mf *MultiFileAppendable) sync() error {
	// sync is only needed in the current appendable:
	// - with retryable sync, non-active appendables were already synced
	// - with non-retryable sync, data may be lost in previous flush or sync calls
	return mf.currApp.Sync()
}

func (mf *MultiFileAppendable) Close() error {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()

	if mf.closed {
		return ErrAlreadyClosed
	}

	mf.closed = true

	err := mf.appendables.Apply(func(k int64, v appendable.Appendable) error {
		return v.Close()
	})
	if err != nil {
		return err
	}

	return mf.currApp.Close()
}

func (mf *MultiFileAppendable) CurrApp() (appendable.Appendable, int64) {
	mf.mutex.Lock()
	defer mf.mutex.Unlock()
	return mf.currApp, mf.currAppID
}

func (mf *MultiFileAppendable) ReplaceCachedChunk(appID int64, app appendable.Appendable) (appendable.Appendable, error) {
	return mf.appendables.Replace(appID, app)
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}
