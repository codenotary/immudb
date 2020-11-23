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
package mocked

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMocked(t *testing.T) {
	mocked := &MockedAppendable{}

	mocked.MetadataFn = func() []byte {
		return nil
	}

	mocked.SizeFn = func() (int64, error) {
		return 0, nil
	}

	mocked.OffsetFn = func() int64 {
		return 0
	}

	mocked.SetOffsetFn = func(off int64) error {
		return nil
	}

	mocked.AppendFn = func(bs []byte) (off int64, n int, err error) {
		return 0, 0, nil
	}

	mocked.FlushFn = func() error {
		return nil
	}

	mocked.SyncFn = func() error {
		return nil
	}

	mocked.ReadAtFn = func(bs []byte, off int64) (int, error) {
		return 0, nil
	}

	mocked.CloseFn = func() error {
		return nil
	}

	md := mocked.Metadata()
	require.Nil(t, md)

	sz, err := mocked.Size()
	require.Equal(t, int64(0), sz)
	require.NoError(t, err)

	off := mocked.Offset()
	require.Equal(t, int64(0), off)

	err = mocked.SetOffset(0)
	require.NoError(t, err)

	off, n, err := mocked.Append(nil)
	require.Equal(t, int64(0), off)
	require.Equal(t, 0, n)
	require.NoError(t, err)

	err = mocked.Flush()
	require.NoError(t, err)

	err = mocked.Sync()
	require.NoError(t, err)

	n, err = mocked.ReadAt(nil, 0)
	require.Equal(t, 0, n)
	require.NoError(t, err)

	err = mocked.Close()
	require.NoError(t, err)
}
