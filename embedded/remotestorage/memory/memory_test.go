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

package memory

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/remotestorage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tmpFile(t *testing.T, data string) (fileName string, cleanup func()) {
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	fmt.Fprint(fl, data)
	err = fl.Close()
	require.NoError(t, err)
	return fl.Name(), func() {
		os.Remove(fl.Name())
	}
}

func storeData(t *testing.T, s *Storage, name, data string) {
	fl, c := tmpFile(t, data)
	defer c()

	err := s.Put(context.Background(), name, fl)
	require.NoError(t, err)
}

func TestRemoteStorageAPIMemory(t *testing.T) {
	storage := Open()
	ctx := context.Background()

	t.Run("Empty storage after initialization", func(t *testing.T) {
		object, err := storage.Get(ctx, "does-not-exist", 0, -1)
		assert.Nil(t, object)
		assert.Equal(t, remotestorage.ErrNotFound, err)
		exists, err := storage.Exists(ctx, "does-not-exist")
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("Single object store", func(t *testing.T) {
		fl, c := tmpFile(t, "objectdata")
		defer c()

		err := storage.Put(ctx, "object-name", fl)
		assert.NoError(t, err)
		exists, err := storage.Exists(ctx, "object-name")
		require.NoError(t, err)
		assert.True(t, exists)
		exists, err = storage.Exists(ctx, "does-not-exist")
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, 1, len(storage.objects))

		t.Run("Read the whole object", func(t *testing.T) {
			data, err := storage.Get(ctx, "object-name", 0, 1000)
			require.NoError(t, err)

			readData, err := ioutil.ReadAll(data)
			require.NoError(t, err)
			require.Equal(t, []byte("objectdata"), readData)
		})

		for _, d := range []struct {
			offset, size int64
			expectedData string
		}{
			{0, 4, "obje"},        // Beginning of the data
			{2, 4, "ject"},        // In the middle
			{0, 10, "objectdata"}, // Exact size
			{2, 10, "jectdata"},   // Past the end
			{100, 10, ""},         // Outside of the object
		} {

			t.Run(fmt.Sprintf("Read part of the object (%d-%d)", d.offset, d.size), func(t *testing.T) {
				data, err := storage.Get(ctx, "object-name", d.offset, d.size)
				require.NoError(t, err)

				readData, err := ioutil.ReadAll(data)
				require.NoError(t, err)
				require.Equal(t, []byte(d.expectedData), readData)
			})
		}
	})
}

func TestRemoteStorageAPIMemoryPutDelay(t *testing.T) {
	ctx := context.Background()

	t.Run("Object must not appear immediately", func(t *testing.T) {
		storage := Open()
		storage.SetRandomPutDelays(1000000, 1000000)

		fl, c := tmpFile(t, "object-data")
		defer c()

		err := storage.Put(ctx, "object-name", fl)
		require.NoError(t, err)

		exists, err := storage.Exists(ctx, "object-name")
		require.NoError(t, err)
		assert.False(t, exists)

	})

	t.Run("object must appear after some delay", func(t *testing.T) {
		storage := Open()
		storage.SetRandomPutDelays(1, 1)

		fl, c := tmpFile(t, "object-data-2")
		defer c()

		err := storage.Put(ctx, "object-name", fl)
		require.NoError(t, err)
		// Data shold be stored within 1ms but due to go scheduler and threads
		// it may be delayed a bit more
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond)
			exists, err := storage.Exists(ctx, "object-name")
			require.NoError(t, err)
			if exists {
				break
			}
		}
		exists, err := storage.Exists(ctx, "object-name")
		require.NoError(t, err)
		require.True(t, exists)

		data, err := storage.Get(ctx, "object-name", 0, 10000)
		require.NoError(t, err)
		bytesRead, err := ioutil.ReadAll(data)
		require.NoError(t, err)
		require.Equal(t, []byte("object-data-2"), bytesRead)
	})
}

func TestRemoteStorageAPIMemoryPutError(t *testing.T) {
	storage := Open()
	ctx := context.Background()

	err := storage.Put(ctx, "object-name", "/file/that/does/not/exist")
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestRemoteStorageName(t *testing.T) {
	storage := Open()
	require.Contains(t, storage.String(), "memory")
	require.Equal(t, "memory", storage.Kind())
}

func TestRemoteStorageGetInvalidParams(t *testing.T) {
	storage := Open()
	ctx := context.Background()

	r, err := storage.Get(ctx, "testfile", -1, 100)
	require.ErrorIs(t, err, ErrInvalidArguments)
	require.Nil(t, r)

	r, err = storage.Get(ctx, "testfile", 0, 0)
	require.ErrorIs(t, err, ErrInvalidArguments)
	require.Nil(t, r)
}

func TestRemoteStorageListEntries(t *testing.T) {
	storage := Open()

	storeData(t, storage, "path/file1", "")
	storeData(t, storage, "path/file2", "abc")
	storeData(t, storage, "path/subPath/file3", "defg")
	storeData(t, storage, "file4", "hi")
	storeData(t, storage, "path2/subPath/file5", "jklmnop")

	entries, subFolders, err := storage.ListEntries(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, []remotestorage.EntryInfo{
		{Name: "file4", Size: 2},
	}, entries)
	require.Equal(t, []string{"path", "path2"}, subFolders)

	entries, subFolders, err = storage.ListEntries(context.Background(), "path/")
	require.NoError(t, err)
	require.Equal(t, []remotestorage.EntryInfo{
		{Name: "file1", Size: 0},
		{Name: "file2", Size: 3},
	}, entries)
	require.Equal(t, []string{"subPath"}, subFolders)
}

func TestRemoteStorageListEntriesInvalidArgs(t *testing.T) {
	storage := Open()

	for _, path := range []string{"/", "no_slash", "double_slash_//_inside/"} {
		t.Run(path, func(t *testing.T) {
			e, s, err := storage.ListEntries(context.Background(), path)
			require.ErrorIs(t, err, ErrInvalidArguments)
			require.Nil(t, e)
			require.Nil(t, s)
		})
	}
}
