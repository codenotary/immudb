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

package remoteapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/fileutils"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/prometheus/client_golang/prometheus"
)

type chunkInfo struct {
	state        chunkState
	storageSize  int64              // Used storage size in bytes
	cancelUpload context.CancelFunc // Set to non-nil when the upload is in progress
}

type RemoteStorageAppendable struct {
	*multiapp.MultiFileAppendable
	rStorage   remotestorage.Storage
	path       string
	fileExt    string
	fileMode   os.FileMode
	remotePath string

	mutex             sync.Mutex
	chunkInfos        []chunkInfo // keys are are chunk IDs
	shutdownWaitGroup sync.WaitGroup

	retryMinDelay time.Duration
	retryMaxDelay time.Duration
	retryDelayExp float64
	retryJitter   float64

	mainContext           context.Context
	mainCancelFunc        context.CancelFunc
	uploadThrottler       chan struct{}
	chunkUploadFinished   *sync.Cond
	chunkDownloadFinished *sync.Cond

	statsUpdaterWaitGroup sync.WaitGroup
}

func Open(path string, remotePath string, storage remotestorage.Storage, opts *Options) (*RemoteStorageAppendable, error) {
	if storage == nil ||
		!opts.Valid() {
		return nil, ErrIllegalArguments
	}
	if remotePath != "" && !strings.HasSuffix(remotePath, "/") {
		return nil, ErrIllegalArguments
	}
	if strings.HasPrefix(remotePath, "/") {
		return nil, ErrIllegalArguments
	}
	if strings.Contains(remotePath, "//") {
		return nil, ErrIllegalArguments
	}

	log.Printf("Opening remote storage at %s%s", storage, remotePath)

	mainContext, mainCancelFunc := context.WithCancel(context.Background())

	ret := &RemoteStorageAppendable{
		rStorage:        storage,
		path:            path,
		fileExt:         opts.GetFileExt(),
		fileMode:        opts.GetFileMode(),
		remotePath:      remotePath,
		retryMinDelay:   opts.retryMinDelay,
		retryMaxDelay:   opts.retryMaxDelay,
		retryDelayExp:   opts.retryDelayExp,
		retryJitter:     opts.retryDelayJitter,
		mainContext:     mainContext,
		mainCancelFunc:  mainCancelFunc,
		uploadThrottler: make(chan struct{}, opts.parallelUploads),
	}
	ret.chunkUploadFinished = sync.NewCond(&ret.mutex)
	ret.chunkDownloadFinished = sync.NewCond(&ret.mutex)

	mApp, err := multiapp.OpenWithHooks(path, ret, &opts.Options)
	if err != nil {
		return nil, err
	}

	ret.MultiFileAppendable = mApp

	// Start uploading all chunks that are still stored locally
	ret.mutex.Lock()
	for chunkID, chunkInfo := range ret.chunkInfos {
		if chunkInfo.state == chunkState_Local {
			ret.uploadChunk(int64(chunkID), false)
		}
	}
	ret.mutex.Unlock()

	ret.startStatsUpdater()

	return ret, nil
}

func chunkIdFromName(filename string) (int64, error) {
	return strconv.ParseInt(strings.TrimSuffix(filename, filepath.Ext(filename)), 10, 64)
}

func (r *RemoteStorageAppendable) chunkedProcess(ctx context.Context) *chunkedProcess {
	return &chunkedProcess{
		ctx:           ctx,
		retryMinDelay: r.retryMinDelay,
		retryMaxDelay: r.retryMaxDelay,
		retryDelayExp: r.retryDelayExp,
		retryJitter:   r.retryJitter,
	}
}

func (r *RemoteStorageAppendable) uploadFinished(chunkID int64, state chunkState) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Printf("Uploadnig of chunk %d finished in state: %v", chunkID, state)

	r.chunkInfos[chunkID].state = state
	r.chunkInfos[chunkID].cancelUpload = nil
	r.chunkUploadFinished.Broadcast()
}

func (r *RemoteStorageAppendable) uploadChunk(chunkID int64, dontRemoveFile bool) {

	appName := r.appendableName(chunkID)
	fileName := filepath.Join(r.path, appName)

	r.shutdownWaitGroup.Add(1)
	ctx, cancelFunc := context.WithCancel(r.mainContext)
	r.chunkInfos[chunkID].state = chunkState_Uploading
	r.chunkInfos[chunkID].cancelUpload = cancelFunc

	// Ensure the we've got an up-to-date filesize in chunk info
	fi, err := os.Stat(fileName)
	if err == nil {
		r.chunkInfos[chunkID].storageSize = fi.Size()
	}

	go func() {
		defer r.shutdownWaitGroup.Done()

		// Throttle simultaneous uploads
		select {
		case <-ctx.Done():
			r.uploadFinished(chunkID, chunkState_Local)
			return
		case r.uploadThrottler <- struct{}{}:
		}
		defer func() {
			<-r.uploadThrottler
		}()

		metricsUploadStarted.Inc()
		defer metricsUploadFinished.Inc()

		var newApp appendable.Appendable
		cp := r.chunkedProcess(ctx)

		// Chunk data was already flushed when changing the active appendable

		// Update size stats after flush
		cp.Step(func() error {
			fi, err := os.Stat(fileName)
			if err == nil {
				r.mutex.Lock()
				r.chunkInfos[chunkID].storageSize = fi.Size()
				r.mutex.Unlock()
			}
			return nil
		})

		// Upload the chunk
		cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
			defer prometheus.NewTimer(metricsUploadTime).ObserveDuration()
			err := r.rStorage.Put(ctx, r.remotePath+appName, fileName)
			if err == nil {
				return false, nil
			}

			metricsUploadRetried.Inc()
			return true, nil
		})

		// Wait for the chunk to become ready
		cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
			exists, err := r.rStorage.Exists(ctx, r.remotePath+appName)
			if err == nil && exists {
				return false, nil
			}

			metricsUploadRetried.Inc()
			return true, nil
		})

		// Open new appendable from the remote storage
		cp.RetryableStep(func(retries int, delay time.Duration) (bool, error) {
			app, err := r.openRemoteAppendableReader(appName)
			if err == nil {
				newApp = app
				return false, nil
			}

			metricsUploadRetried.Inc()
			return true, nil
		})

		// Replace the cached instance of appendable for the chunk
		cp.Step(func() error {
			oldApp, err := r.ReplaceCachedChunk(chunkID, newApp)
			// Couldn't replace the cache entry? Can't continue with the cleanup
			if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
				return err
			}
			if err == nil {
				err := oldApp.Close()
				if err != nil && !errors.Is(err, singleapp.ErrAlreadyClosed) {
					return err
				}
			}
			return nil
		})

		// Cleanup the chunk
		cp.Step(func() error {
			if dontRemoveFile {
				return nil
			}

			r.mutex.Lock()
			r.chunkInfos[chunkID].state = chunkState_Cleaning
			r.chunkInfos[chunkID].cancelUpload = nil
			r.mutex.Unlock()

			err := os.Remove(path.Join(r.path, appName))
			if err != nil {
				return nil
			}

			return fileutils.SyncDir(r.path)
		})

		if ctx.Err() != nil {
			// Context has been cancelled
			log.Printf("Uploading chunk %d cancelled", chunkID)
			r.uploadFinished(chunkID, chunkState_Local)
			metricsUploadCancelled.Inc()
			return
		}

		if cp.Err() != nil {
			log.Printf("Uploading chunk %d failed: %v", chunkID, cp.Err())
			r.uploadFinished(chunkID, chunkState_UploadError)
			metricsUploadFailed.Inc()
			return
		}

		// All done
		r.uploadFinished(chunkID, chunkState_Remote)
		metricsUploadSucceeded.Inc()
	}()
}

func (r *RemoteStorageAppendable) downloadFinished(chunkID int64, state chunkState) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	log.Printf("Downloading of chunk %d finished in state: %v", chunkID, state)

	r.chunkInfos[chunkID].state = state
	r.chunkInfos[chunkID].cancelUpload = nil
	r.chunkDownloadFinished.Broadcast()
}

func (r *RemoteStorageAppendable) downloadChunk(chunkID int64) {
	r.shutdownWaitGroup.Add(1)
	ctx, cancelFunc := context.WithCancel(r.mainContext)
	r.chunkInfos[chunkID].state = chunkState_Downloading
	r.chunkInfos[chunkID].cancelUpload = cancelFunc

	go func() {
		defer r.shutdownWaitGroup.Done()

		metricsDownloadStarted.Inc()
		defer metricsDownloadFinished.Inc()

		appName := r.appendableName(chunkID)
		fileName := filepath.Join(r.path, appName)

		// Downloading to a temporary file first, we can't risk
		// having corrupted (partially downloaded) file because
		// it would be prioritized over the remote data in case
		// of conflict
		fileNameTmp := fileName + ".tmp_download"
		defer func() { _ = os.Remove(fileNameTmp) }()

		var flTmp *os.File
		defer func() { flTmp.Close() }()

		cp := r.chunkedProcess(ctx)
		cp.RetryableStep(func(retries int, delay time.Duration) (retryNeeded bool, err error) {

			data, err := r.rStorage.Get(ctx, r.remotePath+r.appendableName(chunkID), 0, -1)
			if err != nil {
				metricsDownloadRetried.Inc()
				return true, nil
			}
			defer data.Close()

			flTmp, err = os.OpenFile(fileNameTmp, os.O_CREATE|os.O_RDWR, r.fileMode)
			if err != nil {
				// Couldn't create temporary local file, something is broken on local FS
				return false, err
			}

			_, err = io.Copy(flTmp, data)
			if err != nil {
				metricsDownloadRetried.Inc()
				return true, nil
			}
			return false, nil
		})
		cp.Step(func() error {
			return flTmp.Sync()
		})
		cp.Step(func() error {
			return flTmp.Close()
		})
		cp.Step(func() error {
			err := os.Rename(fileNameTmp, fileName)
			if err != nil {
				return err
			}

			return fileutils.SyncDir(r.path)
		})

		if ctx.Err() != nil {
			log.Printf("Downloading chunk %d cancelled", chunkID)
			metricsDownloadCancelled.Inc()
			r.downloadFinished(chunkID, chunkState_DownloadError)
			return
		}

		if cp.Err() != nil {
			log.Printf("Downloading chunk %d failed: %v", chunkID, cp.Err())
			metricsDownloadFailed.Inc()
			r.downloadFinished(chunkID, chunkState_DownloadError)
			return
		}

		metricsDownloadSucceeded.Inc()
		r.downloadFinished(chunkID, chunkState_Local)
	}()
}

func (r *RemoteStorageAppendable) Close() error {
	err := r.MultiFileAppendable.Close()
	if err != nil {
		return err
	}

	// Upload as much as possible,
	// note that flushing is not needed here because
	// all chunks are already closed
	r.mutex.Lock()
	for chunkID, info := range r.chunkInfos {
		switch info.state {
		case chunkState_Active, chunkState_Local:
			r.uploadChunk(int64(chunkID), true)
		}
	}
	r.mutex.Unlock()

	r.shutdownWaitGroup.Wait()

	r.mainCancelFunc()
	r.statsUpdaterWaitGroup.Wait()
	return nil
}

func (r *RemoteStorageAppendable) OpenAppendable(options *singleapp.Options, appname string, activeChunk bool) (appendable.Appendable, error) {
	if options.GetCompressionFormat() != appendable.NoCompression {
		return nil, ErrCompressionNotSupported
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	appID, err := chunkIdFromName(appname)
	if err != nil {
		return nil, err
	}

	if appID >= int64(len(r.chunkInfos)) {
		// Switching to a new appendable at the end, update the state
		for i := int64(len(r.chunkInfos)); i <= appID; i++ {
			// Ensure there's enough room for chunk info,
			// during initialization it will build info for every chunk,
			// during normal operations this appends one info only
			r.chunkInfos = append(r.chunkInfos, chunkInfo{})
		}
		r.chunkInfos[appID].state = chunkState_Local
	}

	if activeChunk {
		// Deactivate previous active chunk (if present)
		// and schedule the upload for it
		for i := int64(0); i < appID; i++ {
			if r.chunkInfos[i].state == chunkState_Active {
				r.uploadChunk(int64(i), false)
				break
			}
		}
		// Deactivate and cancel uploads for all chunks that follow,
		for i := appID + 1; i < int64(len(r.chunkInfos)); i++ {
			switch r.chunkInfos[i].state {
			case chunkState_Uploading:
				r.chunkInfos[i].cancelUpload()
			case chunkState_Active:
				r.chunkInfos[i].state = chunkState_Local
			}
		}
	}

	for {
		switch r.chunkInfos[appID].state {
		case chunkState_Active, chunkState_Local, chunkState_UploadError:
			// File is stored locally. If there was an IO error during upload,
			// try to open a local file, there's a chance it is still partially readable
			if activeChunk {
				// Switch to the active chunk state if needed
				r.chunkInfos[appID].state = chunkState_Active
			}
			return singleapp.Open(filepath.Join(r.path, appname), options)

		case chunkState_Uploading:
			if !activeChunk {
				return singleapp.Open(filepath.Join(r.path, appname), options)
			}

			r.chunkInfos[appID].cancelUpload()
			r.chunkUploadFinished.Wait()
			continue

		case chunkState_Cleaning:
			r.chunkUploadFinished.Wait() // Removal operation can not be interrupted,wait for it to finish
			continue

		case chunkState_Remote:
			if activeChunk {
				// Force download of the chunk, we'll have to wait for it
				r.downloadChunk(appID)
				continue
			}

			return r.openRemoteAppendableReader(appname)

		case chunkState_Downloading:
			r.chunkDownloadFinished.Wait()
			continue

		case chunkState_DownloadError:
			// Even though the chunk couldn't be downloaded locally,
			// it's still available remotely and we should be able to read from it
			if activeChunk {
				return nil, ErrCantDownload
			}
			return r.openRemoteAppendableReader(appname)

		default:
			return nil, ErrInvalidChunkState
		}
	}
}

func (r *RemoteStorageAppendable) appendableName(appID int64) string {
	return fmt.Sprintf("%08d.%s", appID, r.fileExt)
}

func (r *RemoteStorageAppendable) OpenInitialAppendable(opts *multiapp.Options, singleAppOpts *singleapp.Options) (appendable.Appendable, int64, error) {
	chunkInfos := []chunkInfo{}

	// Scan local chunks
	fis, err := ioutil.ReadDir(r.path)
	if err != nil {
		return nil, 0, err
	}

	for _, fi := range fis {
		id, err := chunkIdFromName(fi.Name())
		if err != nil {
			return nil, 0, err
		}

		// Sanity check
		if r.appendableName(id) != fi.Name() {
			return nil, 0, ErrInvalidLocalStorage
		}

		for len(chunkInfos) <= int(id) {
			chunkInfos = append(chunkInfos, chunkInfo{state: chunkState_Invalid})
		}

		chunkInfos[id].state = chunkState_Local
		chunkInfos[id].storageSize = fi.Size()
	}

	// Scan remote chunks
	remoteEntries, _, err := r.rStorage.ListEntries(context.Background(), r.remotePath)
	if err != nil {
		return nil, 0, err
	}

	for _, entry := range remoteEntries {
		id, err := chunkIdFromName(entry.Name)
		if err != nil {
			return nil, 0, err
		}

		// Sanity check
		if r.appendableName(id) != entry.Name {
			return nil, 0, ErrInvalidRemoteStorage
		}

		for len(chunkInfos) <= int(id) {
			chunkInfos = append(chunkInfos, chunkInfo{state: chunkState_Invalid})
		}

		if chunkInfos[id].state == chunkState_Local {
			if entry.Size > chunkInfos[id].storageSize {
				// Chunk size can only grow in size,
				// if the local file is smaller than the remote object,
				// there must have been some corruption of local file
				log.Printf("Chunk validation failed, remote chunk %d has more data than the local file", id)
				return nil, 0, ErrInvalidRemoteStorage
			}
		} else {
			chunkInfos[id].state = chunkState_Remote
			chunkInfos[id].storageSize = entry.Size
		}
	}

	// Ensure we have all chunks
	for id, info := range chunkInfos {
		if info.state == chunkState_Invalid {
			// Chunk was not found in neither local nor remote storage
			log.Printf("Chunk validation failed, missing chunk %d", id)
			return nil, 0, ErrMissingRemoteChunk
		}
	}

	r.chunkInfos = chunkInfos

	if len(chunkInfos) == 0 {
		// Opening new DB, the first chunk will be created
		chunkInfos = append(chunkInfos, chunkInfo{
			state:       chunkState_Local,
			storageSize: 0,
		})
	}

	appID := int64(len(chunkInfos) - 1)

	app, err := r.OpenAppendable(singleAppOpts, r.appendableName(appID), true)
	if err != nil {
		return nil, 0, err
	}

	return app, appID, nil
}

func (r *RemoteStorageAppendable) openRemoteAppendableReader(name string) (appendable.Appendable, error) {
	return openRemoteStorageReader(
		r.rStorage,
		r.remotePath+name,
	)
}

func (r *RemoteStorageAppendable) startStatsUpdater() {
	r.statsUpdaterWaitGroup.Add(1)
	go func() {
		defer r.statsUpdaterWaitGroup.Done()

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-r.mainContext.Done():
				return
			case <-ticker.C:
			}
			r.calculateChunkMetrics()
		}
	}()
}

func (r *RemoteStorageAppendable) calculateChunkMetrics() error {
	states := map[string]int64{}
	sizes := map[string]int64{}

	for _, state := range chunkStateNames {
		states[state] = 0
		sizes[state] = 0
	}

	_, currAppID := r.CurrApp()

	r.mutex.Lock()

	if r.chunkInfos[currAppID].state == chunkState_Active {
		// Sync current appendable stats
		appName := r.appendableName(currAppID)
		fileName := filepath.Join(r.path, appName)
		fi, err := os.Stat(fileName)
		if err == nil {
			r.chunkInfos[currAppID].storageSize = fi.Size()
		}
	}

	for _, info := range r.chunkInfos {
		states[info.state.String()]++
		sizes[info.state.String()] += info.storageSize
	}

	r.mutex.Unlock()

	for state, count := range states {
		metricsChunkCounts.With(prometheus.Labels{
			"path":  r.remotePath,
			"state": state,
		}).Set(float64(count))
	}

	for state, size := range sizes {
		metricsChunkDataBytes.With(prometheus.Labels{
			"path":  r.remotePath,
			"state": state,
		}).Set(float64(size))
	}

	return nil
}

var _ appendable.Appendable = (*RemoteStorageAppendable)(nil)
