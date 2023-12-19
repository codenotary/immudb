//go:build minio
// +build minio

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

package s3

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3WithServer(t *testing.T) {
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	require.NoError(t, err)

	s, err := Open(
		"http://localhost:9000",
		false,
		"",
		"minioadmin",
		"minioadmin",
		"immudb",
		"",
		fmt.Sprintf("prefix_%x", randomBytes),
		"",
	)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("check exist if file was not created", func(t *testing.T) {
		exists, err := s.Exists(ctx, "test1")
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("store a file", func(t *testing.T) {
		fl, err := ioutil.TempFile("", "")
		require.NoError(t, err)
		fmt.Fprintf(fl, "Hello world")
		fl.Close()
		defer os.Remove(fl.Name())

		err = s.Put(ctx, "test1", fl.Name())
		require.NoError(t, err)
	})

	t.Run("check exist after file was created", func(t *testing.T) {
		exists, err := s.Exists(ctx, "test1")
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("read whole file", func(t *testing.T) {
		in, err := s.Get(ctx, "test1", 0, -1)
		require.NoError(t, err)
		data, err := ioutil.ReadAll(in)
		require.NoError(t, err)
		err = in.Close()
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("read file partially", func(t *testing.T) {
		in, err := s.Get(ctx, "test1", 1, 5)
		require.NoError(t, err)
		data, err := ioutil.ReadAll(in)
		require.NoError(t, err)
		err = in.Close()
		require.NoError(t, err)
		require.Equal(t, []byte("ello "), data)
	})

	t.Run("create multiple file-s in multiple folders", func(t *testing.T) {

		const foldersCount = 3
		const entriesCount = 20

		for i := 0; i < foldersCount; i++ {
			for j := 0; j < entriesCount; j++ {
				fl, err := ioutil.TempFile("", "")
				require.NoError(t, err)
				fmt.Fprintf(fl, "Hello world_%d_%d", i, j)
				fl.Close()
				defer os.Remove(fl.Name())

				err = s.Put(ctx, fmt.Sprintf("test2/folder%d/file%d", i, j), fl.Name())
				require.NoError(t, err)
			}
		}

		entries, sub, err := s.ListEntries(ctx, "test2/")
		require.NoError(t, err)
		require.Empty(t, entries)
		require.Len(t, sub, foldersCount)

		entries, sub, err = s.ListEntries(ctx, "test2/folder0/")
		require.NoError(t, err)
		require.Empty(t, sub)
		require.Len(t, entries, entriesCount)
		require.EqualValues(t, "file1", entries[1].Name)
		require.EqualValues(t, "file0", entries[0].Name)
		require.EqualValues(t, "file10", entries[2].Name)
		require.EqualValues(t, 15, entries[0].Size)
		require.EqualValues(t, 15, entries[1].Size)
		require.EqualValues(t, 16, entries[2].Size)
	})
}
