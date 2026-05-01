/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package remoteapp

import (
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
)

// lazyRemoteReader is an Appendable that defers opening its inner
// remote-storage reader until the first ReadAt call. It exists so the
// post-upload cache swap in RemoteStorageAppendable.uploadChunk can
// hand the multiapp cache a placeholder without paying a chunk-window
// download right after the Put — most uploaded chunks are never read
// again, and on those the deferred open is a pure savings.
//
// On first ReadAt, `open` runs once (sync.Once); the result is
// cached. Close is safe whether or not the inner has been opened.
//
// All write- and metadata-side methods panic with "unimplemented",
// matching the behaviour of remoteStorageReader (the type that `open`
// returns). The lazy wrapper never claims to support anything the
// underlying remote reader doesn't.
type lazyRemoteReader struct {
	open func() (appendable.Appendable, error)

	once  sync.Once
	inner appendable.Appendable
	err   error
}

func newLazyRemoteReader(open func() (appendable.Appendable, error)) *lazyRemoteReader {
	return &lazyRemoteReader{open: open}
}

func (l *lazyRemoteReader) ensure() (appendable.Appendable, error) {
	l.once.Do(func() {
		l.inner, l.err = l.open()
	})
	return l.inner, l.err
}

func (l *lazyRemoteReader) ReadAt(bs []byte, off int64) (int, error) {
	inner, err := l.ensure()
	if err != nil {
		return 0, err
	}
	return inner.ReadAt(bs, off)
}

func (l *lazyRemoteReader) Close() error {
	if l.inner != nil {
		return l.inner.Close()
	}
	return nil
}

func (l *lazyRemoteReader) Flush() error                  { return nil }
func (l *lazyRemoteReader) Sync() error                   { return nil }
func (l *lazyRemoteReader) SwitchToReadOnlyMode() error   { return nil }
func (l *lazyRemoteReader) Metadata() []byte              { panic("unimplemented") }
func (l *lazyRemoteReader) Size() (int64, error)          { panic("unimplemented") }
func (l *lazyRemoteReader) Offset() int64                 { panic("unimplemented") }
func (l *lazyRemoteReader) SetOffset(off int64) error     { panic("unimplemented") }
func (l *lazyRemoteReader) DiscardUpto(off int64) error   { panic("unimplemented") }
func (l *lazyRemoteReader) Append(bs []byte) (int64, int, error) {
	panic("unimplemented")
}
func (l *lazyRemoteReader) CompressionFormat() int { panic("unimplemented") }
func (l *lazyRemoteReader) CompressionLevel() int  { panic("unimplemented") }
func (l *lazyRemoteReader) Copy(dstPath string) error {
	panic("unimplemented")
}

var _ appendable.Appendable = (*lazyRemoteReader)(nil)
