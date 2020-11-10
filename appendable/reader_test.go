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
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type MockedAppendable struct {
	metadata []byte

	sz    int64
	szErr error

	offset    int64
	offsetErr error

	appendOff int64
	appendN   int
	appendErr error

	flushErr error

	syncErr error

	readAt func(bs []byte, off int64) (int, error)

	closeErr error
}

func (a *MockedAppendable) Metadata() []byte {
	return a.metadata
}

func (a *MockedAppendable) Size() (int64, error) {
	return a.sz, a.szErr
}

func (a *MockedAppendable) Offset() int64 {
	return a.offset
}

func (a *MockedAppendable) SetOffset(off int64) error {
	return a.offsetErr
}

func (a *MockedAppendable) Append(bs []byte) (off int64, n int, err error) {
	return a.appendOff, a.appendN, a.appendErr
}

func (a *MockedAppendable) Flush() error {
	return a.flushErr
}

func (a *MockedAppendable) Sync() error {
	return a.syncErr
}

func (a *MockedAppendable) ReadAt(bs []byte, off int64) (int, error) {
	return a.readAt(bs, off)
}

func (a *MockedAppendable) Close() error {
	return a.closeErr
}

func TestReader(t *testing.T) {
	a := &MockedAppendable{}

	r := NewReaderFrom(a, 0, 1024)
	require.NotNil(t, r)

	require.Equal(t, int64(0), r.Offset())

	a.readAt = func(bs []byte, off int64) (int, error) {
		return 0, errors.New("error")
	}
	_, err := r.Read([]byte{0})
	require.Error(t, err)

	a.readAt = func(bs []byte, off int64) (int, error) {
		bs[0] = 127
		return 1, nil
	}
	b, err := r.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(127), b)

	a.readAt = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint32(bs, 256)
		return 4, nil
	}
	n32, err := r.ReadUint32()
	require.NoError(t, err)
	require.Equal(t, uint32(256), n32)

	a.readAt = func(bs []byte, off int64) (int, error) {
		binary.BigEndian.PutUint64(bs, 1024)
		return 8, nil
	}
	n64, err := r.ReadUint64()
	require.NoError(t, err)
	require.Equal(t, uint64(1024), n64)
}
