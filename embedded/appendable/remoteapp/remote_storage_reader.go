package remoteapp

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/prometheus/client_golang/prometheus"
)

// DefaultReaderRangeCacheSize is the per-window size used by the
// remote-storage reader's LRU cache. Sized to match immudb's default
// multiapp chunk size (`WithFileSize(1<<20)` = 1 MiB), so a sequential
// full-chunk read on a default-configured deployment does **one** S3
// GET on open — same as the pre-Wave-1 reader — while a partial read
// of a large chunk still pays only the windows it actually touches.
//
// Tunable via WithReaderRangeCacheSize on the remoteapp options.
const DefaultReaderRangeCacheSize = 1 * 1024 * 1024

// openHeaderSlack is the extra bytes the open-time GET pulls beyond
// `rangeCacheSize` so the first cached window covers a full aligned
// payload region even after the variable-length metadata header is
// stripped. Without this, baseOffset (4 + metadata length) eats into
// the first window, the payload portion ends before the next aligned
// window boundary, and the next read past it forces an extra GET to
// fetch a window we'd otherwise have ended on. 4 KiB is large enough
// for any plausible appendable metadata while staying tiny relative
// to the cache window.
const openHeaderSlack = 4096

// DefaultReaderCacheWindows is the LRU depth of the per-reader cache.
// Each cached window is up to DefaultReaderRangeCacheSize bytes, so
// the worst-case resident memory per reader is
// DefaultReaderCacheWindows * DefaultReaderRangeCacheSize.
//
// Depth 4 trades ~4 MiB per reader for an order-of-magnitude better
// hit rate on sparse / random access patterns: revisiting a window
// touched within the last 3 misses is a memcpy instead of an S3 GET.
const DefaultReaderCacheWindows = 4

// pendingFetch represents a single in-flight read-ahead Get. The
// fetcher goroutine sets data/err and then closes done. Foreground
// ReadAt callers find the matching pendingFetch (by offs) and wait on
// done to overlap the per-window RTT with whatever they were doing
// while consuming the previous window.
type pendingFetch struct {
	offs int64
	done chan struct{}

	data []byte
	err  error
}

// cachedWindow is one entry in the LRU. data holds payload bytes for
// offsets [offs, offs+len(data)).
type cachedWindow struct {
	offs int64
	data []byte
}

type remoteStorageReader struct {
	r              remotestorage.Storage
	name           string
	baseOffset     int64
	rangeCacheSize int
	maxWindows     int

	// compressionFormat is parsed from the chunk's metadata header
	// at open time. When non-zero (i.e. anything other than
	// appendable.NoCompression), ReadAt reproduces the per-entry
	// length-prefix + decompress protocol that singleapp's writer
	// uses, on top of the same range cache that handles raw byte
	// fetches. compressionLevel is parsed alongside but only used
	// for symmetry — the readers infer level from the compressed
	// stream.
	compressionFormat int
	compressionLevel  int

	mu          sync.Mutex
	windows     []cachedWindow // MRU at index 0, LRU at the tail
	sizeKnown   bool
	payloadSize int64 // valid only when sizeKnown

	pf *pendingFetch // at most one prefetch in flight, guarded by mu

	prefetchCtx    context.Context
	prefetchCancel context.CancelFunc
}

func openRemoteStorageReader(r remotestorage.Storage, name string, rangeCacheSize int) (*remoteStorageReader, error) {
	defer prometheus.NewTimer(metricsOpenTime).ObserveDuration()

	if rangeCacheSize <= 0 {
		rangeCacheSize = DefaultReaderRangeCacheSize
	}

	ctx := context.Background()

	// Read header + as much of the leading payload as fits in one
	// window. A short response (len < rangeCacheSize) means we've
	// seen the whole object, in which case pin payloadSize so
	// out-of-range ReadAt calls don't issue a wasted GET.
	openFetchSize := int64(rangeCacheSize) + openHeaderSlack
	reader, err := r.Get(ctx, name, 0, openFetchSize)
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return nil, err
	}
	metricsUncachedReads.Inc()
	metricsUncachedReadBytes.Add(float64(len(data)))
	if len(data) < 4 {
		metricsCorruptedMetadata.Inc()
		return nil, ErrCorruptedMetadata
	}

	baseOffset := int64(4 + binary.BigEndian.Uint32(data[:4]))
	if baseOffset > int64(len(data)) {
		metricsCorruptedMetadata.Inc()
		return nil, ErrCorruptedMetadata
	}

	// Parse the chunk metadata to find out whether the upstream
	// writer used singleapp's per-entry compression. If yes, ReadAt
	// reproduces the same length-prefix + decompress protocol;
	// otherwise we serve raw bytes. Metadata key matches
	// singleapp.MetaCompressionFormat (the on-disk format key).
	//
	// Defensive: NewMetadata panics on bytes that don't follow the
	// expected TLV layout (some test fixtures + any non-singleapp
	// producer). Recover and default to NoCompression rather than
	// failing the whole open.
	compressionFormat := appendable.NoCompression
	if baseOffset > 4 {
		compressionFormat = parseCompressionFormat(data[4:baseOffset])
	}

	pCtx, pCancel := context.WithCancel(context.Background())
	sr := &remoteStorageReader{
		r:                 r,
		name:              name,
		baseOffset:        baseOffset,
		rangeCacheSize:    rangeCacheSize,
		maxWindows:        DefaultReaderCacheWindows,
		compressionFormat: compressionFormat,
		windows: []cachedWindow{
			{offs: 0, data: data[baseOffset:]},
		},
		prefetchCtx:    pCtx,
		prefetchCancel: pCancel,
	}
	if int64(len(data)) < openFetchSize {
		sr.sizeKnown = true
		sr.payloadSize = int64(len(sr.windows[0].data))
	}
	// Wave 3: do NOT start a prefetch on open. If the consumer never
	// issues a ReadAt past the initial window, no further GET fires.
	// Prefetch is started lazily from fetchAndCopyLocked once we've
	// observed an actual cache miss — that's evidence the consumer is
	// reading sequentially past the cache and will benefit from the
	// next window arriving in parallel.
	return sr, nil
}

func (r *remoteStorageReader) Metadata() []byte {
	panic("unimplemented")
}

func (r *remoteStorageReader) Size() (int64, error) {
	panic("unimplemented")
}

func (r *remoteStorageReader) Offset() int64 {
	panic("unimplemented")
}

func (r *remoteStorageReader) SetOffset(off int64) error {
	panic("unimplemented")
}

func (r *remoteStorageReader) DiscardUpto(off int64) error {
	panic("unimplemented")
}

func (r *remoteStorageReader) Append(bs []byte) (off int64, n int, err error) {
	panic("unimplemented")
}

func (r *remoteStorageReader) CompressionFormat() int {
	return r.compressionFormat
}

func (r *remoteStorageReader) CompressionLevel() int {
	return r.compressionLevel
}

func (r *remoteStorageReader) Flush() error {
	return nil
}

func (r *remoteStorageReader) Sync() error {
	return nil
}

func (r *remoteStorageReader) SwitchToReadOnlyMode() error {
	return nil
}

// ReadAt is the public entry. For uncompressed chunks it delegates
// straight to the raw range-cache path. For compressed chunks it
// reproduces singleapp's per-entry frame protocol on top of the raw
// reader: read the 4-byte length prefix at off, fetch that many
// compressed bytes at off+4, decompress in memory, then copy.
func (r *remoteStorageReader) ReadAt(bs []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrIllegalArguments
	}
	if len(bs) == 0 {
		return 0, nil
	}

	if r.compressionFormat == appendable.NoCompression {
		return r.readAtRaw(bs, off)
	}
	return r.readAtCompressedFrame(bs, off)
}

// readAtRaw is the uncompressed range-cache + prefetch path. Also
// used by readAtCompressedFrame to fetch the underlying compressed
// bytes (length prefix + compressed payload) before decompressing.
func (r *remoteStorageReader) readAtRaw(bs []byte, off int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sizeKnown && off >= r.payloadSize {
		return 0, io.EOF
	}

	// Cache hit: any of the LRU windows covers this offset?
	if w, ok := r.lookupWindowLocked(off); ok {
		return r.serveFromWindowLocked(bs, off, w)
	}

	// Cache miss. If an in-flight prefetch is fetching the
	// window-sized region containing this offset, wait on it instead
	// of issuing a fresh GET. The fetcher goroutine doesn't take
	// r.mu, so dropping the lock while we wait is safe.
	if r.pf != nil && off >= r.pf.offs && off < r.pf.offs+int64(r.rangeCacheSize) {
		pf := r.pf
		r.mu.Unlock()
		<-pf.done
		r.mu.Lock()

		// Adopt the prefetched data (if it's still the current pf).
		if r.pf == pf {
			r.pf = nil
			if pf.err == nil {
				r.insertWindowLocked(pf.offs, pf.data)
				if int64(len(pf.data)) < int64(r.rangeCacheSize) {
					r.sizeKnown = true
					r.payloadSize = pf.offs + int64(len(pf.data))
				}
				// Keep the pipeline primed for the next window — without
				// this, every adoption would stall the next sequential
				// miss for a full RTT instead of overlapping.
				r.maybeStartPrefetchLocked()
			}
		}

		if w, ok := r.lookupWindowLocked(off); ok {
			return r.serveFromWindowLocked(bs, off, w)
		}
		// Prefetch errored or was preempted; fall through to direct fetch.
	}

	return r.fetchAndCopyLocked(bs, off)
}

// lookupWindowLocked returns the window covering off, moving it to
// MRU position on hit. Caller must hold r.mu.
func (r *remoteStorageReader) lookupWindowLocked(off int64) (cachedWindow, bool) {
	for i, w := range r.windows {
		if off >= w.offs && off < w.offs+int64(len(w.data)) {
			if i > 0 {
				// Move w to MRU (index 0); shift the prefix right.
				copy(r.windows[1:i+1], r.windows[0:i])
				r.windows[0] = w
			}
			return w, true
		}
	}
	return cachedWindow{}, false
}

// insertWindowLocked installs a freshly-fetched window at MRU,
// evicting the LRU entry if we're at capacity. Caller must hold r.mu.
func (r *remoteStorageReader) insertWindowLocked(offs int64, data []byte) {
	nw := cachedWindow{offs: offs, data: data}
	if len(r.windows) < r.maxWindows {
		// Grow by one and shift everything right by one slot.
		r.windows = append(r.windows, cachedWindow{})
	}
	copy(r.windows[1:], r.windows[:len(r.windows)-1])
	r.windows[0] = nw
}

// serveFromWindowLocked copies from `w` into bs and handles partial
// reads (cache window boundary or end of object). On a partial read
// it either returns io.EOF (object exhausted) or recursively fetches
// the next window. Caller must hold r.mu.
func (r *remoteStorageReader) serveFromWindowLocked(bs []byte, off int64, w cachedWindow) (int, error) {
	rel := off - w.offs
	n := copy(bs, w.data[rel:])
	metricsReads.Inc()
	metricsReadBytes.Add(float64(n))

	if n == len(bs) {
		return n, nil
	}
	if r.sizeKnown && off+int64(n) >= r.payloadSize {
		return n, io.EOF
	}
	more, err := r.fetchAndCopyLocked(bs[n:], off+int64(n))
	return n + more, err
}

// fetchAndCopyLocked issues a Range GET aligned to a fixed window
// boundary that contains `off`, inserts the result as the new MRU
// cache window, and copies into bs. Aligning to fixed boundaries
// (rather than starting the GET at the random caller offset) means
// subsequent reads near the same region land in the same cached
// window — without alignment, two sparse reads that fall in the
// "same logical 1 MiB region" but at different offsets would each
// fetch their own overlapping window and the cache would thrash.
//
// If the request straddles two windows, this fetches the first one
// and recurses for the remainder. Caller must hold r.mu.
func (r *remoteStorageReader) fetchAndCopyLocked(bs []byte, off int64) (int, error) {
	windowSize := int64(r.rangeCacheSize)
	windowOffs := (off / windowSize) * windowSize
	fetchLen := windowSize

	ctx := context.Background()
	reader, err := r.r.Get(ctx, r.name, windowOffs+r.baseOffset, fetchLen)
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return 0, err
	}
	data, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		metricsUncachedReadErrors.Inc()
		return 0, err
	}
	metricsUncachedReads.Inc()
	metricsUncachedReadBytes.Add(float64(len(data)))

	r.insertWindowLocked(windowOffs, data)
	if int64(len(data)) < fetchLen {
		r.sizeKnown = true
		r.payloadSize = windowOffs + int64(len(data))
	}

	rel := off - windowOffs
	if rel >= int64(len(data)) {
		// Window doesn't actually contain off (object ended inside
		// the aligned window before this offset). EOF.
		return 0, io.EOF
	}
	n := copy(bs, data[rel:])
	metricsReads.Inc()
	metricsReadBytes.Add(float64(n))
	if n < len(bs) {
		// Request straddles a window boundary or extends past EOF.
		if r.sizeKnown && off+int64(n) >= r.payloadSize {
			return n, io.EOF
		}
		more, err := r.fetchAndCopyLocked(bs[n:], off+int64(n))
		return n + more, err
	}
	// Wave 3: only kick a prefetch from the cache-miss path. Cache
	// hits don't fire one because every hit on a random pattern would
	// otherwise queue a wasted GET — and on a sequential pattern the
	// next miss will fire it just as effectively.
	r.maybeStartPrefetchLocked()
	return n, nil
}

// maybeStartPrefetchLocked spawns one background Get for the window
// immediately after the current MRU cache entry, if and only if (a)
// no useful prefetch is already in flight and (b) we don't already
// know the payload ends inside the cached prefix. A completed but
// stale prefetch (offs no longer matches the next-after-MRU we want)
// is dropped first so a new one can start. Caller must hold r.mu.
func (r *remoteStorageReader) maybeStartPrefetchLocked() {
	if r.sizeKnown {
		return
	}
	if len(r.windows) == 0 {
		return
	}
	mru := r.windows[0]
	nextOffs := mru.offs + int64(len(mru.data))
	if nextOffs <= 0 {
		return
	}

	// Existing prefetch already lined up correctly? Leave it.
	if r.pf != nil && r.pf.offs == nextOffs {
		return
	}
	// Existing prefetch in flight for a different offset? Don't start
	// a second concurrent fetch; let the in-flight one drain first.
	// If it's already completed but stale, drop it so we can issue
	// the right one.
	if r.pf != nil {
		select {
		case <-r.pf.done:
			r.pf = nil
		default:
			return
		}
	}

	pf := &pendingFetch{
		offs: nextOffs,
		done: make(chan struct{}),
	}
	r.pf = pf

	go func() {
		defer close(pf.done)
		reader, err := r.r.Get(r.prefetchCtx, r.name, nextOffs+r.baseOffset, int64(r.rangeCacheSize))
		if err != nil {
			pf.err = err
			return
		}
		data, errRead := ioutil.ReadAll(reader)
		reader.Close()
		if errRead != nil {
			pf.err = errRead
			return
		}
		pf.data = data
	}()
}

// readAtCompressedFrame implements singleapp's per-Append frame
// protocol on top of readAtRaw:
//
//	[ 4-byte big-endian length N ][ N bytes of compressed payload ]
//
// The frame at `off` decompresses to one Append's payload. We copy
// up to len(bs) bytes of that payload into bs and return io.EOF if
// the decoded payload was shorter than the request.
func (r *remoteStorageReader) readAtCompressedFrame(bs []byte, off int64) (int, error) {
	var lenBuf [4]byte
	if _, err := r.readAtRaw(lenBuf[:], off); err != nil {
		return 0, err
	}
	clen := binary.BigEndian.Uint32(lenBuf[:])
	if clen == 0 {
		return 0, io.EOF
	}

	cBs := make([]byte, clen)
	if _, err := r.readAtRaw(cBs, off+4); err != nil {
		return 0, err
	}

	dec, err := newDecompressReader(r.compressionFormat, bytes.NewReader(cBs))
	if err != nil {
		return 0, err
	}
	defer dec.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(dec); err != nil {
		return 0, err
	}
	rbs := buf.Bytes()

	n := copy(bs, rbs)
	if n < len(bs) {
		return n, io.EOF
	}
	return n, nil
}

// parseCompressionFormat extracts singleapp's MetaCompressionFormat
// from a chunk's TLV metadata bytes. Returns NoCompression on any
// parse failure — the metadata is owned by the writer and may legally
// be in formats this package doesn't understand.
func parseCompressionFormat(metaBytes []byte) (cf int) {
	defer func() {
		if r := recover(); r != nil {
			cf = appendable.NoCompression
		}
	}()
	md := appendable.NewMetadata(metaBytes)
	if v, ok := md.GetInt(singleapp.MetaCompressionFormat); ok {
		return v
	}
	return appendable.NoCompression
}

// newDecompressReader returns a decoder for the given compression
// format. Mirrors singleapp.AppendableFile.reader so the on-disk
// frame format stays identical between local and remote chunks.
func newDecompressReader(format int, src io.Reader) (io.ReadCloser, error) {
	switch format {
	case appendable.FlateCompression:
		return flate.NewReader(src), nil
	case appendable.GZipCompression:
		return gzip.NewReader(src)
	case appendable.LZWCompression:
		return lzw.NewReader(src, lzw.MSB, 8), nil
	case appendable.ZLibCompression:
		return zlib.NewReader(src)
	default:
		return nil, ErrCorruptedMetadata
	}
}

func (r *remoteStorageReader) Close() error {
	// Cancel any in-flight prefetch; the goroutine drains itself
	// promptly because the underlying Storage.Get respects the
	// context. We don't wait — Close is called from the cache
	// eviction path and shouldn't block on network I/O.
	if r.prefetchCancel != nil {
		r.prefetchCancel()
	}
	return nil
}

func (r *remoteStorageReader) Copy(dstPath string) error {
	panic("unimplemented")
}

var _ appendable.Appendable = (*remoteStorageReader)(nil)
