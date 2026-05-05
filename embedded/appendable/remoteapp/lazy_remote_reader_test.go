/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package remoteapp

import (
	"errors"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/stretchr/testify/require"
)

type fakeAppendable struct {
	closeErr  error
	closeCnt  int
	readAtErr error
}

func (f *fakeAppendable) Close() error                         { f.closeCnt++; return f.closeErr }
func (f *fakeAppendable) ReadAt(bs []byte, off int64) (int, error) {
	if f.readAtErr != nil {
		return 0, f.readAtErr
	}
	return len(bs), nil
}
func (f *fakeAppendable) Flush() error                         { return nil }
func (f *fakeAppendable) Sync() error                          { return nil }
func (f *fakeAppendable) SwitchToReadOnlyMode() error          { return nil }
func (f *fakeAppendable) Metadata() []byte                     { return nil }
func (f *fakeAppendable) Size() (int64, error)                 { return 0, nil }
func (f *fakeAppendable) Offset() int64                        { return 0 }
func (f *fakeAppendable) SetOffset(off int64) error            { return nil }
func (f *fakeAppendable) DiscardUpto(off int64) error          { return nil }
func (f *fakeAppendable) Append(bs []byte) (int64, int, error) { return 0, 0, nil }
func (f *fakeAppendable) CompressionFormat() int               { return 0 }
func (f *fakeAppendable) CompressionLevel() int                { return 0 }
func (f *fakeAppendable) Copy(dstPath string) error            { return nil }

func TestLazyRemoteReader_ReadAtOpenError(t *testing.T) {
	openErr := errors.New("open failed")
	l := newLazyRemoteReader(func() (appendable.Appendable, error) {
		return nil, openErr
	})

	n, err := l.ReadAt(make([]byte, 4), 0)
	require.ErrorIs(t, err, openErr)
	require.Zero(t, n)

	// second call should hit the same cached error without reopening
	n, err = l.ReadAt(make([]byte, 4), 0)
	require.ErrorIs(t, err, openErr)
	require.Zero(t, n)

	// Close before any successful open is a no-op
	require.NoError(t, l.Close())
}

func TestLazyRemoteReader_ReadAtOpensOnce(t *testing.T) {
	calls := 0
	inner := &fakeAppendable{}
	l := newLazyRemoteReader(func() (appendable.Appendable, error) {
		calls++
		return inner, nil
	})

	for i := 0; i < 3; i++ {
		n, err := l.ReadAt(make([]byte, 8), int64(i))
		require.NoError(t, err)
		require.Equal(t, 8, n)
	}
	require.Equal(t, 1, calls, "open should be invoked exactly once")

	require.NoError(t, l.Close())
	require.Equal(t, 1, inner.closeCnt)
}

func TestLazyRemoteReader_NoOpAndPanicStubs(t *testing.T) {
	l := newLazyRemoteReader(func() (appendable.Appendable, error) {
		return &fakeAppendable{}, nil
	})

	// no-ops return nil regardless of inner state
	require.NoError(t, l.Flush())
	require.NoError(t, l.Sync())
	require.NoError(t, l.SwitchToReadOnlyMode())

	// every metadata/write-side method is unimplemented for the lazy reader
	require.Panics(t, func() { l.Metadata() })
	require.Panics(t, func() { _, _ = l.Size() })
	require.Panics(t, func() { l.Offset() })
	require.Panics(t, func() { _ = l.SetOffset(0) })
	require.Panics(t, func() { _ = l.DiscardUpto(0) })
	require.Panics(t, func() { _, _, _ = l.Append(nil) })
	require.Panics(t, func() { l.CompressionFormat() })
	require.Panics(t, func() { l.CompressionLevel() })
	require.Panics(t, func() { _ = l.Copy("/tmp/whatever") })
}
