package remoteapp

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
	"github.com/stretchr/testify/require"
)

func tmpFile(t *testing.T, data []byte) (fileName string, cleanup func()) {
	fl, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	_, err = fl.Write(data)
	require.NoError(t, err)
	err = fl.Close()
	require.NoError(t, err)
	return fl.Name(), func() {
		os.Remove(fl.Name())
	}
}

func storeData(t *testing.T, s remotestorage.Storage, name string, data []byte) {
	fl, c := tmpFile(t, data)
	defer c()

	err := s.Put(context.Background(), name, fl)
	require.NoError(t, err)
}

func TestRemoteStorageReaderUnsupportedMethods(t *testing.T) {
	r := remoteStorageReader{}

	require.Panics(t, func() { r.Metadata() })
	require.Panics(t, func() { r.Size() })
	require.Panics(t, func() { r.Offset() })
	require.Panics(t, func() { r.SetOffset(0) })
	require.Panics(t, func() { r.Append([]byte{0}) })
	require.Panics(t, func() { r.DiscardUpto(0) })
	require.Panics(t, func() { r.Copy("/tmp") })

	// CompressionFormat/Level used to panic but are now real
	// accessors — required for the multiapp prefetch snapshot to
	// work and for the chunk-level decompress path. A zero-value
	// reader reports NoCompression / level 0.
	require.Equal(t, appendable.NoCompression, r.CompressionFormat())
	require.Equal(t, 0, r.CompressionLevel())
}

func TestRemoteStorageFlush(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.Flush())
}

func TestRemoteStorageSync(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.Sync())
}

func TestRemoteStorageSwitchToReadOnlyMode(t *testing.T) {
	r := remoteStorageReader{}
	require.NoError(t, r.SwitchToReadOnlyMode())
}

func TestRemoteStorageReadAt(t *testing.T) {
	m := memory.Open()
	storeData(t, m, "fl", []byte{
		0, 0, 0, 4, // Dummy empty header
		0, 0, 0, 0,
		1, 2, 3, 4, // Data, 4 bytes
	})

	r, err := openRemoteStorageReader(m, "fl", 0)
	require.NoError(t, err)

	b := make([]byte, 4)
	n, err := r.ReadAt(b, 0)
	require.NoError(t, err)
	require.EqualValues(t, 4, n)
	require.Equal(t, []byte{1, 2, 3, 4}, b)

	n, err = r.ReadAt(make([]byte, 10), 0)
	require.EqualValues(t, 4, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), 3)
	require.EqualValues(t, 1, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), -1)
	require.EqualValues(t, 0, n)
	require.ErrorIs(t, err, ErrIllegalArguments)

	n, err = r.ReadAt(make([]byte, 2), 4)
	require.EqualValues(t, 0, n)
	require.Equal(t, io.EOF, err)

	n, err = r.ReadAt(make([]byte, 2), 5)
	require.EqualValues(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestRemoteStorageCorruptedHeader(t *testing.T) {
	for _, d := range []struct {
		name  string
		bytes []byte
	}{
		{"less than 4 bytes", []byte{0, 0, 0}},
		{"not enough header bytes", []byte{0, 0, 0, 5, 0, 0, 0, 0}},
	} {
		t.Run(d.name, func(t *testing.T) {
			m := memory.Open()
			storeData(t, m, "fl", d.bytes)

			r, err := openRemoteStorageReader(m, "fl", 0)
			require.ErrorIs(t, err, ErrCorruptedMetadata)
			require.Nil(t, r)
		})
	}
}

type remoteStorageErrorInjector struct {
	remotestorage.Storage
	err           error
	errDuringRead bool
}

func (r *remoteStorageErrorInjector) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if r.errDuringRead {
		return ioutil.NopCloser(errReader{r.err}), nil
	}
	return nil, r.err
}

// rangeRecordingStorage wraps an inner Storage and records (offs, size)
// for every Get call. Used by TestReaderRangeFetch_DoesNotDownloadEntireChunk
// to assert the reader actually issues bounded Range GETs after open
// rather than dragging the whole object on every ReadAt.
type rangeRecordingStorage struct {
	remotestorage.Storage
	mu   sync.Mutex
	gets []rangeGet
}

type rangeGet struct {
	offs int64
	size int64
}

func (r *rangeRecordingStorage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	r.mu.Lock()
	r.gets = append(r.gets, rangeGet{offs, size})
	r.mu.Unlock()
	return r.Storage.Get(ctx, name, offs, size)
}

func (r *rangeRecordingStorage) snapshot() []rangeGet {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]rangeGet, len(r.gets))
	copy(out, r.gets)
	return out
}

func TestReaderRangeFetch_DoesNotDownloadEntireChunk(t *testing.T) {
	// Build a payload large enough that one full-object download would
	// dominate any range-aware path. 4 MiB payload + tiny metadata
	// header. Default reader range cache size is 256 KiB, so any
	// reachable Get should be ~256 KiB, never 4 MiB and never `-1`
	// (whole object).
	const (
		payloadSize     = 4 * 1024 * 1024
		rangeCacheSize  = 256 * 1024
		readerWindowMax = rangeCacheSize + 4096 // small slack for header bytes
	)

	header := []byte{0, 0, 0, 0} // 0-byte metadata
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	m := &rangeRecordingStorage{Storage: memory.Open()}
	storeData(t, m, "fl", append(header, payload...))

	r, err := openRemoteStorageReader(m, "fl", rangeCacheSize)
	require.NoError(t, err)

	// Open issues a Get sized close to rangeCacheSize (not -1).
	// Wave 3 over-fetches by openHeaderSlack so the first cached
	// payload window covers a full aligned region after stripping
	// the metadata header — assert bounded, not exact.
	gets := m.snapshot()
	require.GreaterOrEqual(t, len(gets), 1, "open should issue at least one Get")
	require.Equal(t, int64(0), gets[0].offs, "open Get should start at byte 0")
	require.NotEqual(t, int64(-1), gets[0].size,
		"open Get must not request -1 (full object)")
	require.LessOrEqual(t, gets[0].size, int64(rangeCacheSize+openHeaderSlack),
		"open Get must be bounded by rangeCacheSize + openHeaderSlack")
	require.GreaterOrEqual(t, gets[0].size, int64(rangeCacheSize),
		"open Get must cover at least rangeCacheSize")

	// Read 64 bytes near the END of the payload — guaranteed cache miss.
	const tailReadOff = payloadSize - 100
	buf := make([]byte, 64)
	n, err := r.ReadAt(buf, tailReadOff)
	require.NoError(t, err)
	require.Equal(t, 64, n)
	require.Equal(t, payload[tailReadOff:tailReadOff+64], buf,
		"ranged read must return the right bytes from the right offset")

	// Sequential read inside the now-cached window returns the right
	// bytes. We can't pin "no new Get here" because Wave 2's
	// background read-ahead prefetch may legitimately fire between
	// snapshots — the bounded-size invariant below catches the actual
	// regression we care about (no full-chunk download).
	buf2 := make([]byte, 32)
	n, err = r.ReadAt(buf2, tailReadOff+64)
	require.NoError(t, err)
	require.Equal(t, 32, n)
	require.Equal(t, payload[tailReadOff+64:tailReadOff+96], buf2)

	// Every Get issued so far must have requested a bounded window —
	// never -1 (whole object) and never larger than ~rangeCacheSize.
	// This is the load-bearing invariant: full-chunk downloads are
	// gone for good. Wave 2 may add a read-ahead prefetch on top, so
	// we don't pin the exact Get count, only the per-Get size cap.
	for i, g := range m.snapshot() {
		require.NotEqual(t, int64(-1), g.size,
			"Get #%d must not request -1 (whole object): %+v", i, g)
		require.LessOrEqual(t, g.size, int64(readerWindowMax),
			"Get #%d must request a bounded window, not the whole chunk: %+v", i, g)
		require.Greater(t, g.size, int64(0),
			"Get #%d size must be positive: %+v", i, g)
	}
}

func TestRemoteStorageOpenError(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		m := &remoteStorageErrorInjector{
			Storage:       memory.Open(),
			err:           errors.New("Injected error"),
			errDuringRead: duringRead,
		}
		storeData(t, m, "fl", []byte{
			0, 0, 0, 4, // Dummy empty header
			0, 0, 0, 0,
			1, 2, 3, 4, // Data, 4 bytes
		})

		r, err := openRemoteStorageReader(m, "fl", 0)
		require.Equal(t, m.err, err)
		require.Nil(t, r)
	}
}
