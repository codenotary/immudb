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

package remoteapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenInllegalArguments(t *testing.T) {
	dir := t.TempDir()
	app, err := Open(dir, "", memory.Open(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, app)

	app, err = Open(dir, "", nil, DefaultOptions())
	require.ErrorIs(t, err, ErrIllegalArguments)
	require.Nil(t, app)

	app, err = Open(dir, "remotepath", memory.Open(), DefaultOptions())
	require.Equal(t, ErrIllegalArguments, err, "remotePath must end with '/'")
	require.Nil(t, app)

	app, err = Open(dir, "/remotepath/", memory.Open(), DefaultOptions())
	require.Equal(t, ErrIllegalArguments, err, "remotePath must not start with '/'")
	require.Nil(t, app)

	app, err = Open(dir, "remote//path/", memory.Open(), DefaultOptions())
	require.Equal(t, ErrIllegalArguments, err, "remote path must not contain '//'")
	require.Nil(t, app)
}

func TestOpenMultiappError(t *testing.T) {
	fl := filepath.Join(t.TempDir(), "testfile")
	require.NoError(t, ioutil.WriteFile(fl, []byte{}, 0777))

	app, err := Open(fl, "", memory.Open(), DefaultOptions())
	require.NotNil(t, err)
	require.Nil(t, app)
}

func TestOpenRemoteStorageAppendable(t *testing.T) {
	app, err := Open(t.TempDir(), "", memory.Open(), DefaultOptions())
	require.NoError(t, err)

	err = app.Close()
	require.NoError(t, err)

	err = app.Close()
	require.ErrorIs(t, err, multiapp.ErrAlreadyClosed)
}

func TestOpenRemoteStorageAppendableCompression(t *testing.T) {
	opts := DefaultOptions()
	opts.WithCompressionFormat(appendable.FlateCompression).
		WithCompresionLevel(appendable.BestCompression)

	app, err := Open(t.TempDir(), "", memory.Open(), opts)
	require.ErrorIs(t, err, ErrCompressionNotSupported)
	require.Nil(t, app)
}

func TestRemoteStorageOpenAppendableInvalidName(t *testing.T) {
	app, err := Open(t.TempDir(), "", memory.Open(), DefaultOptions())
	require.NoError(t, err)

	subApp, err := app.OpenAppendable(singleapp.DefaultOptions(), "invalid-app-name", false)
	require.Error(t, err)
	require.Nil(t, subApp)
}

const testWaitTimeout = 10 * time.Second

func waitForObject(mem *memory.Storage, objectName string) bool {
	startWait := time.Now()
	for {
		if exists, _ := mem.Exists(context.Background(), objectName); exists {
			return true
		}

		if time.Since(startWait) > testWaitTimeout {
			return false
		}

		time.Sleep(time.Millisecond)
	}
}

func waitForRemoval(fileName string) bool {
	startWait := time.Now()
	for {
		if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
			return true
		}

		if time.Since(startWait) > testWaitTimeout {
			return false
		}

		time.Sleep(time.Millisecond)
	}
}

func waitForChunkState(app *RemoteStorageAppendable, chunkID int, state chunkState) bool {
	startWait := time.Now()
	for {
		app.mutex.Lock()
		if len(app.chunkInfos) > chunkID &&
			app.chunkInfos[chunkID].state == state {
			app.mutex.Unlock()
			return true
		}
		app.mutex.Unlock()

		if time.Since(startWait) > testWaitTimeout {
			return false
		}

		time.Sleep(time.Millisecond)
	}
}

func waitForFile(fileName string, maxWait time.Duration) bool {
	startWait := time.Now()
	for {
		if st, err := os.Stat(fileName); err == nil && st.Mode().IsRegular() {
			return true
		}

		if time.Since(startWait) > maxWait {
			return false
		}

		time.Sleep(time.Millisecond)
	}
}

func TestWritePastFirstChunk(t *testing.T) {
	path := t.TempDir()

	opts := DefaultOptions()
	opts.WithFileSize(10)
	opts.WithMaxOpenedFiles(3)

	mem := memory.Open()

	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	testData := []byte("Large buffer")

	offs, n, err := app.Append(testData)
	assert.EqualValues(t, 0, offs)
	assert.EqualValues(t, 12, n)
	require.NoError(t, err)

	err = app.Flush() // Needs an explicit flush, it's managed on the immustore layer
	require.NoError(t, err)

	// Ensure the chunk is uploaded to a remote storage
	assert.True(t, waitForObject(mem, "00000000.aof"))

	readData := make([]byte, 12)
	n, err = app.ReadAt(readData, 0)
	require.NoError(t, err)
	assert.EqualValues(t, n, 12)
	assert.EqualValues(t, testData, readData)

	// Append some more data
	testData2 := []byte("Even larger buffer spanning across multiple files")
	offs, n, err = app.Append(testData2)
	require.NoError(t, err)
	assert.EqualValues(t, 12, offs)
	assert.EqualValues(t, 49, n)

	err = app.Flush() // Needs an explicit flush, it's managed on the immustore layer
	require.NoError(t, err)

	assert.True(t, waitForObject(mem, "00000005.aof"))

	// Cache shuld contain chunks: 3, 4, 5 now, 6th is the active one
	assert.True(t, waitForRemoval(fmt.Sprintf("%s/00000000.aof", path)))
	assert.True(t, waitForRemoval(fmt.Sprintf("%s/00000001.aof", path)))
	assert.True(t, waitForRemoval(fmt.Sprintf("%s/00000002.aof", path)))

	readData = make([]byte, 12+49)
	n, err = app.ReadAt(readData, 0)
	require.NoError(t, err)
	assert.EqualValues(t, n, 12+49)
	assert.EqualValues(t, append(testData, testData2...), readData)

	err = app.Close()
	require.NoError(t, err)
}

func prepareLocalTestFiles(t *testing.T) string {
	path := t.TempDir()
	mapp, err := multiapp.Open(path, multiapp.DefaultOptions().WithFileSize(10).WithFileExt("tst"))
	require.NoError(t, err)

	_, _, err = mapp.Append([]byte("Some pretty long string to cross a chunk boundary"))
	require.NoError(t, err)
	err = mapp.Close()
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.FileExists(t, fmt.Sprintf("%s/%08d.tst", path, i))
	}

	return path
}

func TestRemoteAppUploadOnStartup(t *testing.T) {
	path := prepareLocalTestFiles(t)

	opts := DefaultOptions()
	opts.WithFileExt("tst")

	mem := memory.Open()
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		require.True(t, waitForObject(mem, fmt.Sprintf("%08d.tst", i)))
		require.True(t, waitForRemoval(fmt.Sprintf("%s/%08d.tst", path, i)))
	}
	err = app.Close()
	require.NoError(t, err)
}

func TestReopenOnCleanShutdownWhenEmpty(t *testing.T) {
	path := t.TempDir()

	mem := memory.Open()
	opts := DefaultOptions()
	opts.WithFileExt("tst")
	opts.WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)
	require.FileExists(t, fmt.Sprintf("%s/00000000.tst", path))

	err = app.Close()
	require.NoError(t, err)
	require.FileExists(t, fmt.Sprintf("%s/00000000.tst", path))

	exists, err := mem.Exists(context.Background(), "00000000.tst")
	require.NoError(t, err)
	require.True(t, exists)

	err = os.RemoveAll(path)
	require.NoError(t, err)

	app, err = Open(path, "", mem, opts)
	require.NoError(t, err)

	require.True(t, waitForFile(fmt.Sprintf("%s/00000000.tst", path), time.Second))
	size, err := app.Size()
	require.NoError(t, err)
	require.EqualValues(t, 0, size)
}

func TestReopenFromRemoteStorageOnCleanShutdown(t *testing.T) {
	path := t.TempDir()

	mem := memory.Open()
	opts := DefaultOptions()
	opts.WithFileExt("tst")
	opts.WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	dataWritten := []byte("Some pretty long string to cross a chunk boundary")

	offs, n, err := app.Append(dataWritten)
	require.NoError(t, err)
	require.EqualValues(t, 0, offs)
	require.EqualValues(t, len(dataWritten), n)
	require.True(t, waitForRemoval(fmt.Sprintf("%s/00000003.tst", path)))

	err = app.Close()
	require.NoError(t, err)
	require.FileExists(t, fmt.Sprintf("%s/00000004.tst", path))

	exists, err := mem.Exists(context.Background(), "00000004.tst")
	require.NoError(t, err)
	require.True(t, exists)

	err = os.RemoveAll(path)
	require.NoError(t, err)

	app, err = Open(path, "", mem, opts)
	require.NoError(t, err)

	require.True(t, waitForFile(fmt.Sprintf("%s/00000004.tst", path), time.Second))

	dataRead := make([]byte, len(dataWritten))
	n, err = app.ReadAt(dataRead, 0)
	require.NoError(t, err)
	require.EqualValues(t, len(dataWritten), n)
	require.Equal(t, dataWritten, dataRead)
}

func TestRemoteStorageMetrics(t *testing.T) {
	mStarted := testutil.ToFloat64(metricsUploadStarted)
	mFinished := testutil.ToFloat64(metricsUploadFinished)
	mFailed := testutil.ToFloat64(metricsUploadFailed)
	mSucceeded := testutil.ToFloat64(metricsUploadSucceeded)

	path := t.TempDir()

	mem := memory.Open()
	opts := DefaultOptions()
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	dataWritten := []byte("Some pretty long string to cross a chunk boundary")

	offs, n, err := app.Append(dataWritten)
	require.NoError(t, err)
	require.EqualValues(t, 0, offs)
	require.EqualValues(t, len(dataWritten), n)
	for i := 0; i < 4; i++ {
		require.True(t, waitForChunkState(app, i, chunkState_Remote))
	}

	err = app.Flush()
	require.NoError(t, err)

	err = app.calculateChunkMetrics()
	require.NoError(t, err)

	for _, d := range []struct {
		state           string
		expectedCount   int
		expectedStorage int
	}{
		{"Active", 1, 198},
		{"Remote", 4, 4 * 199},
		{"Uploading", 0, 0},
	} {
		t.Run("Checking count for "+d.state, func(t *testing.T) {
			g, err := metricsChunkCounts.GetMetricWithLabelValues("", d.state)
			require.NoError(t, err)
			require.EqualValues(t, d.expectedCount, testutil.ToFloat64(g))

			g, err = metricsChunkDataBytes.GetMetricWithLabelValues("", d.state)
			require.NoError(t, err)
			require.EqualValues(t, d.expectedStorage, testutil.ToFloat64(g))
		})
	}

	require.EqualValues(t, 4, testutil.ToFloat64(metricsUploadStarted)-mStarted)
	require.EqualValues(t, 4, testutil.ToFloat64(metricsUploadFinished)-mFinished)
	require.EqualValues(t, 4, testutil.ToFloat64(metricsUploadSucceeded)-mSucceeded)
	require.EqualValues(t, 0, testutil.ToFloat64(metricsUploadFailed)-mFailed)
}

type remoteStorageMockingWrapper struct {
	wrapped remotestorage.Storage

	fnGet         func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error)
	fnPut         func(ctx context.Context, name string, fileName string, next func() error) error
	fnExists      func(ctx context.Context, name string, next func() (bool, error)) (bool, error)
	fnListEntries func(ctx context.Context, path string, next func() (entries []remotestorage.EntryInfo, subPaths []string, err error)) (entries []remotestorage.EntryInfo, subPaths []string, err error)
}

func (r *remoteStorageMockingWrapper) Kind() string {
	return r.wrapped.Kind()
}

func (r *remoteStorageMockingWrapper) String() string {
	return r.wrapped.String()
}

func (r *remoteStorageMockingWrapper) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if r.fnGet != nil {
		return r.fnGet(ctx, name, offs, size, func() (io.ReadCloser, error) {
			return r.wrapped.Get(ctx, name, offs, size)
		})
	}
	return r.wrapped.Get(ctx, name, offs, size)
}

func (r *remoteStorageMockingWrapper) Put(ctx context.Context, name string, fileName string) error {
	if r.fnPut != nil {
		return r.fnPut(ctx, name, fileName, func() error {
			return r.wrapped.Put(ctx, name, fileName)
		})
	}
	return r.wrapped.Put(ctx, name, fileName)
}

func (r *remoteStorageMockingWrapper) Remove(ctx context.Context, name string) error {
	panic("unimplemented")
}

func (r *remoteStorageMockingWrapper) RemoveAll(ctx context.Context, path string) error {
	panic("unimplemented")
}

func (r *remoteStorageMockingWrapper) Exists(ctx context.Context, name string) (bool, error) {
	if r.fnExists != nil {
		return r.fnExists(ctx, name, func() (bool, error) {
			return r.wrapped.Exists(ctx, name)
		})
	}
	return r.wrapped.Exists(ctx, name)
}

func (r *remoteStorageMockingWrapper) ListEntries(ctx context.Context, path string) (entries []remotestorage.EntryInfo, subPaths []string, err error) {
	if r.fnListEntries != nil {
		return r.fnListEntries(ctx, path, func() (entries []remotestorage.EntryInfo, subPaths []string, err error) {
			return r.wrapped.ListEntries(ctx, path)
		})
	}
	return r.wrapped.ListEntries(ctx, path)
}

func TestRemoteStorageUploadRetry(t *testing.T) {
	mRetries := testutil.ToFloat64(metricsUploadRetried)

	path := t.TempDir()

	// Injecting exactly one error in put, get and exists operations
	var putErrInjected int32
	var getErrInjected int32
	var existsErrInjected int32
	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnPut: func(ctx context.Context, name, fileName string, next func() error) error {
			if name == "00000000.tst" && atomic.CompareAndSwapInt32(&putErrInjected, 0, 1) {
				return errors.New("Injected error")
			}
			return next()
		},
		fnGet: func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
			if name == "00000001.tst" && atomic.CompareAndSwapInt32(&getErrInjected, 0, 1) {
				return nil, errors.New("Injected error")
			}
			return next()
		},
		fnExists: func(ctx context.Context, name string, next func() (bool, error)) (bool, error) {
			if name == "00000002.tst" && atomic.CompareAndSwapInt32(&existsErrInjected, 0, 1) {
				return false, errors.New("Injected error")
			}
			return next()
		},
	}

	opts := DefaultOptions().WithRetryMinDelay(time.Microsecond).WithRetryMaxDelay(time.Microsecond)
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	dataWritten := []byte("Some pretty long string to cross a chunk boundary")

	_, _, err = app.Append(dataWritten)
	require.NoError(t, err)

	err = app.Close()
	require.NoError(t, err)

	require.EqualValues(t, 3, testutil.ToFloat64(metricsUploadRetried)-mRetries)
}

func TestRemoteStorageUploadCancel(t *testing.T) {
	path := t.TempDir()

	for _, name := range []string{"Put", "Get", "Exists"} {
		t.Run(name, func(t *testing.T) {

			require.NoError(t, os.RemoveAll(path))

			var getErrInjected int32
			retryWait := sync.WaitGroup{}
			retryWait.Add(1)

			// Injecting exactly one error in put, get and exists operations
			mem := &remoteStorageMockingWrapper{
				wrapped: memory.Open(),
			}

			switch name {
			case "Put":
				mem.fnPut = func(ctx context.Context, name, fileName string, next func() error) error {
					if name == "00000000.tst" {
						if atomic.CompareAndSwapInt32(&getErrInjected, 0, 1) {
							retryWait.Done()
						}
						return errors.New("Neverending error")
					}
					return next()
				}
			case "Get":
				mem.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
					if name == "00000000.tst" {
						if atomic.CompareAndSwapInt32(&getErrInjected, 0, 1) {
							retryWait.Done()
						}
						return nil, errors.New("Neverending error")
					}
					return next()
				}
			case "Exists":
				mem.fnExists = func(ctx context.Context, name string, next func() (bool, error)) (bool, error) {
					if name == "00000000.tst" && atomic.CompareAndSwapInt32(&getErrInjected, 0, 1) {
						retryWait.Done()
						return false, errors.New("Neverending error")
					}
					return next()
				}

			}

			opts := DefaultOptions()
			opts.WithFileExt("tst").WithFileSize(10)
			app, err := Open(path, "", mem, opts)
			require.NoError(t, err)

			dataWritten := []byte("Some pretty long string to cross a chunk boundary")

			_, _, err = app.Append(dataWritten)
			require.NoError(t, err)
			retryWait.Wait()

			app.mainCancelFunc()

			require.True(t, waitForChunkState(app, 0, chunkState_Local))
		})
	}
}

func TestRemoteStorageUploadCancelWhenThrottled(t *testing.T) {
	path := t.TempDir()

	// Injecting exactly one error in put, get and exists operations
	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnPut: func(ctx context.Context, name, fileName string, next func() error) error {
			// Upload wil be retried indefinitely occupying the slot
			return errors.New("Neverending error")
		},
	}

	opts := DefaultOptions()
	opts.WithParallelUploads(1)
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	// Write past 0th chunk, upload that starts should occupy the slot
	_, _, err = app.Append(make([]byte, 11))
	require.NoError(t, err)
	require.True(t, waitForChunkState(app, 0, chunkState_Uploading))

	// Write some more data to spawn more upload goroutines - those should stop on the throttler
	_, _, err = app.Append(make([]byte, 40))
	require.NoError(t, err)
	require.True(t, waitForChunkState(app, 1, chunkState_Uploading))

	// After cancellation, chunks should get back to the local state
	app.mainCancelFunc()
	require.True(t, waitForChunkState(app, 0, chunkState_Local))
	require.True(t, waitForChunkState(app, 1, chunkState_Local))
}

func _TestRemoteStorageUploadUnrecoverableError(t *testing.T) {
	path := t.TempDir()

	mUploadFailed := testutil.ToFloat64(metricsUploadFailed)

	existReached := sync.WaitGroup{}
	existReached.Add(1)
	existContinue := sync.WaitGroup{}
	existContinue.Add(1)

	// Injecting exactly one error in put, get and exists operations
	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnExists: func(ctx context.Context, name string, next func() (bool, error)) (bool, error) {
			if name == "00000000.tst" {
				existReached.Done()
				existContinue.Wait()
			}
			return next()
		},
	}

	opts := DefaultOptions()
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	dataWritten := []byte("Some pretty long string to cross a chunk boundary")

	_, _, err = app.Append(dataWritten)
	require.NoError(t, err)

	// Wait for all chunks but the first one to finish uploading
	for i := 1; i < 4; i++ {
		require.True(t, waitForChunkState(app, i, chunkState_Remote))
	}

	// Remove the folder during `exists` check - that way
	// there will be an error while trying to removing the file
	existReached.Wait()
	err = os.RemoveAll(path)
	require.NoError(t, err)
	existContinue.Done()

	require.True(t, waitForChunkState(app, 0, chunkState_UploadError))
	require.EqualValues(t, 1, testutil.ToFloat64(metricsUploadFailed)-mUploadFailed)
}

type errReader struct {
	err error
}

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func _TestRemoteStorageDownloadRetry(t *testing.T) {
	path := t.TempDir()

	for _, errKind := range []string{"Open", "Read"} {
		t.Run(errKind, func(t *testing.T) {

			mRetries := testutil.ToFloat64(metricsDownloadRetried)

			require.NoError(t, os.RemoveAll(path))

			mem := memory.Open()

			opts := DefaultOptions().WithRetryMinDelay(time.Microsecond).WithRetryMaxDelay(time.Microsecond)
			opts.WithFileExt("tst").WithFileSize(10)
			app, err := Open(path, "", mem, opts)
			require.NoError(t, err)

			dataWritten := []byte("Some pretty long string to cross a chunk boundary")

			_, _, err = app.Append(dataWritten)
			require.NoError(t, err)

			err = app.Close()
			require.NoError(t, err)

			require.NoError(t, os.RemoveAll(path))

			var errInjected int32
			memErr := &remoteStorageMockingWrapper{wrapped: mem}

			switch errKind {
			case "Open":
				memErr.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
					if name == "00000004.tst" && atomic.CompareAndSwapInt32(&errInjected, 0, 1) {
						return nil, errors.New("Injected error")
					}
					return next()
				}
			case "Read":
				memErr.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
					if name == "00000004.tst" && atomic.CompareAndSwapInt32(&errInjected, 0, 1) {
						return ioutil.NopCloser(errReader{errors.New("Injected error")}), nil
					}
					return next()
				}
			}

			app, err = Open(path, "", memErr, opts)
			require.NoError(t, err)

			dataRead := make([]byte, len(dataWritten))
			n, err := app.ReadAt(dataRead, 0)
			require.NoError(t, err)
			require.EqualValues(t, len(dataWritten), n)

			err = app.Close()
			require.NoError(t, err)

			require.EqualValues(t, 1, testutil.ToFloat64(metricsDownloadRetried)-mRetries)
		})
	}
}

func TestRemoteStorageDownloadCancel(t *testing.T) {
	path := t.TempDir()

	for _, errKind := range []string{"Open", "Read"} {
		t.Run(errKind, func(t *testing.T) {

			require.NoError(t, os.RemoveAll(path))

			mem := memory.Open()

			opts := DefaultOptions().WithRetryMinDelay(time.Microsecond).WithRetryMaxDelay(time.Microsecond)
			opts.WithFileExt("tst").WithFileSize(10)
			app, err := Open(path, "", mem, opts)
			require.NoError(t, err)

			dataWritten := []byte("Some pretty long string to cross a chunk boundary")

			_, _, err = app.Append(dataWritten)
			require.NoError(t, err)

			err = app.Close()
			require.NoError(t, err)

			require.NoError(t, os.RemoveAll(path))

			var errInjected int32
			retryWait := sync.WaitGroup{}
			retryWait.Add(1)

			memErr := &remoteStorageMockingWrapper{wrapped: mem}
			switch errKind {
			case "Open":
				memErr.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
					if name == "00000003.tst" {
						if atomic.CompareAndSwapInt32(&errInjected, 0, 1) {
							retryWait.Done()
						}
						return nil, errors.New("Injected error")
					}
					return next()
				}
			case "Read":
				memErr.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
					if name == "00000003.tst" {
						if atomic.CompareAndSwapInt32(&errInjected, 0, 1) {
							retryWait.Done()
						}
						return ioutil.NopCloser(errReader{errors.New("Injected error")}), nil
					}
					return next()
				}
			}

			app, err = Open(path, "", memErr, opts)
			require.NoError(t, err)

			// Switch active chunk to 00000003
			setOffsetWg := sync.WaitGroup{}
			setOffsetWg.Add(1)
			var setOffsetErr error
			go func() {
				// SetOffset will block until we cancel
				setOffsetErr = app.SetOffset(35)
				setOffsetWg.Done()
			}()
			retryWait.Wait()

			// Cancel and check if everything propagated correctly
			app.mainCancelFunc()
			setOffsetWg.Wait()

			require.Error(t, setOffsetErr)
		})
	}
}

func TestRemoteStorageDownloadUnrecoverableError(t *testing.T) {
	path := t.TempDir()

	mDownloadFailed := testutil.ToFloat64(metricsDownloadFailed)

	mem := memory.Open()

	opts := DefaultOptions().WithRetryMinDelay(time.Microsecond).WithRetryMaxDelay(time.Microsecond)
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	dataWritten := []byte("Some pretty long string to cross a chunk boundary")

	_, _, err = app.Append(dataWritten)
	require.NoError(t, err)

	err = app.Close()
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(path))

	memErr := &remoteStorageMockingWrapper{
		wrapped: mem,
		fnGet: func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
			if name == "00000003.tst" {
				// Remove local folder to cause errors when creating a temporary file
				os.RemoveAll(path)
			}
			return next()
		},
	}

	app, err = Open(path, "", memErr, opts)
	require.NoError(t, err)

	// Switch active chunk to 00000003
	err = app.SetOffset(35)
	require.Error(t, err)

	require.EqualValues(t, 1, testutil.ToFloat64(metricsDownloadFailed)-mDownloadFailed)
}

func TestRemoteStorageOpenChunkWhenUploading(t *testing.T) {
	path := prepareLocalTestFiles(t)

	mem := &remoteStorageMockingWrapper{
		wrapped: memory.Open(),
		fnPut: func(ctx context.Context, name, fileName string, next func() error) error {
			if name == "00000003.tst" {
				return errors.New("Neverending upload")
			}
			return next()
		},
	}

	opts := DefaultOptions()
	opts.WithFileExt("tst").WithFileSize(10)
	app, err := Open(path, "", mem, opts)
	require.NoError(t, err)

	require.True(t, waitForChunkState(app, 0, chunkState_Remote))
	require.True(t, waitForChunkState(app, 1, chunkState_Remote))
	require.True(t, waitForChunkState(app, 2, chunkState_Remote))
	require.True(t, waitForChunkState(app, 3, chunkState_Uploading))
	require.True(t, waitForChunkState(app, 4, chunkState_Active))

	// Read chunk while it's being uploaded
	readBytes := make([]byte, 8)
	n, err := app.ReadAt(readBytes, 31)
	require.NoError(t, err)
	require.EqualValues(t, 8, n)
	require.Equal(t, []byte("s a chun"), readBytes)

	// Switch chunk to active while it's being uploaded
	err = app.SetOffset(35)
	require.NoError(t, err)

	require.True(t, waitForChunkState(app, 3, chunkState_Active))
	require.True(t, waitForChunkState(app, 4, chunkState_Local))

}

func TestRemoteStorageOpenInitialAppendableMissingRemoteChunk(t *testing.T) {
	path := t.TempDir()

	// Prepare test dataset
	opts := DefaultOptions()
	opts.WithFileSize(10)
	opts.WithFileExt("tst")
	m := &remoteStorageMockingWrapper{wrapped: memory.Open()}
	app, err := Open(path, "", m, opts)
	require.NoError(t, err)
	_, _, err = app.Append([]byte("Even larger buffer spanning across multiple files"))
	require.NoError(t, err)
	require.True(t, waitForRemoval(fmt.Sprintf("%s/00000000.tst", path)))
	err = app.Close()
	require.NoError(t, err)

	// Simulate missing file on a remote storage
	m.fnExists = func(ctx context.Context, name string, next func() (bool, error)) (bool, error) {
		if name == "00000000.tst" {
			return false, nil
		}
		return next()
	}
	m.fnGet = func(ctx context.Context, name string, offs, size int64, next func() (io.ReadCloser, error)) (io.ReadCloser, error) {
		if name == "00000000.tst" {
			return nil, remotestorage.ErrNotFound
		}
		return next()
	}
	m.fnListEntries = func(ctx context.Context, path string, next func() (entries []remotestorage.EntryInfo, subPaths []string, err error)) (entries []remotestorage.EntryInfo, subPaths []string, err error) {
		e, s, err := next()
		require.True(t, e[0].Name == "00000000.tst")
		return e[1:], s, err
	}

	// Opening should fail now
	app, err = Open(path, "", m, opts)
	require.ErrorIs(t, err, ErrMissingRemoteChunk)
	require.Nil(t, app)
}

func TestRemoteStorageOpenInitialAppendableCorruptedLocalFile(t *testing.T) {
	path := t.TempDir()

	// Prepare test dataset
	opts := DefaultOptions()
	opts.WithFileSize(10)
	opts.WithFileExt("tst")
	m := &remoteStorageMockingWrapper{wrapped: memory.Open()}
	app, err := Open(path, "", m, opts)
	require.NoError(t, err)
	_, _, err = app.Append([]byte("Even larger buffer spanning across multiple files"))
	require.NoError(t, err)
	require.True(t, waitForRemoval(fmt.Sprintf("%s/00000000.tst", path)))
	err = app.Close()
	require.NoError(t, err)

	// Local file smaller than a corresponding remote object indicates data corruption
	err = ioutil.WriteFile(fmt.Sprintf("%s/00000000.tst", path), []byte{}, 0777)
	require.NoError(t, err)

	// Opening should fail now
	app, err = Open(path, "", m, opts)
	require.ErrorIs(t, err, ErrInvalidRemoteStorage)
	require.Nil(t, app)
}
