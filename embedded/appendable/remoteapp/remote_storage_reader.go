package remoteapp

import (
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"sync"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/prometheus/client_golang/prometheus"
)

// DefaultReaderRangeCacheSize is the per-reader sliding window used to
// service ReadAt without re-issuing an S3 GET on every call. It also
// caps the open-time header download — the open path does one GET of
// this size, parses the metadata length prefix from the first 4 bytes,
// keeps the trailing payload bytes as the initial cache, and (when the
// response was strictly shorter than the request) pins the payload
// size so subsequent ReadAt past EOF can short-circuit without an S3
// round trip.
//
// 256 KiB is large enough that hot-restore replay and SQL scans fall
// almost entirely inside the cache while sequentially walking a chunk,
// and small enough that random-access patterns don't drag a megabyte
// per ReadAt call. Tunable via WithReaderRangeCacheSize on the
// remoteapp options.
const DefaultReaderRangeCacheSize = 256 * 1024

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

type remoteStorageReader struct {
	r              remotestorage.Storage
	name           string
	baseOffset     int64
	rangeCacheSize int

	mu          sync.Mutex
	cache       []byte // payload bytes [cacheOffs, cacheOffs+len(cache))
	cacheOffs   int64
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
	reader, err := r.Get(ctx, name, 0, int64(rangeCacheSize))
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

	pCtx, pCancel := context.WithCancel(context.Background())
	sr := &remoteStorageReader{
		r:              r,
		name:           name,
		baseOffset:     baseOffset,
		rangeCacheSize: rangeCacheSize,
		cache:          data[baseOffset:],
		cacheOffs:      0,
		prefetchCtx:    pCtx,
		prefetchCancel: pCancel,
	}
	if int64(len(data)) < int64(rangeCacheSize) {
		sr.sizeKnown = true
		sr.payloadSize = int64(len(sr.cache))
	} else {
		// Pipeline the second window while the caller processes the
		// first — Wave 2 read-ahead. Hot-restore replay, SQL scans,
		// and replication catch-up all walk a chunk linearly, so this
		// shaves one full RTT per window from the wall-clock.
		sr.mu.Lock()
		sr.maybeStartPrefetchLocked()
		sr.mu.Unlock()
	}
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
	panic("unimplemented")
}

func (r *remoteStorageReader) CompressionLevel() int {
	panic("unimplemented")
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

func (r *remoteStorageReader) ReadAt(bs []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrIllegalArguments
	}
	if len(bs) == 0 {
		return 0, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.sizeKnown && off >= r.payloadSize {
		return 0, io.EOF
	}

	// Fast path: requested range starts inside the cache.
	if off >= r.cacheOffs && off < r.cacheOffs+int64(len(r.cache)) {
		return r.serveFromCacheLocked(bs, off)
	}

	// Cache miss. If the in-flight prefetch matches the requested
	// offset, wait on it instead of issuing a fresh GET. The fetcher
	// goroutine doesn't take r.mu, so dropping the lock while we wait
	// is safe and lets other state (e.g. an unrelated Close) progress.
	if r.pf != nil && r.pf.offs == off {
		pf := r.pf
		r.mu.Unlock()
		<-pf.done
		r.mu.Lock()

		// Adopt the prefetched data (if it's still the current pf).
		if r.pf == pf {
			r.pf = nil
			if pf.err == nil {
				r.cache = pf.data
				r.cacheOffs = off
				if int64(len(pf.data)) < int64(r.rangeCacheSize) {
					r.sizeKnown = true
					r.payloadSize = off + int64(len(pf.data))
				}
			}
		}

		if off >= r.cacheOffs && off < r.cacheOffs+int64(len(r.cache)) {
			return r.serveFromCacheLocked(bs, off)
		}
		// Prefetch errored or was preempted; fall through to direct fetch.
	}

	return r.fetchAndCopyLocked(bs, off)
}

// serveFromCacheLocked copies from the in-memory cache into bs and
// handles partial reads (cache window boundary or end of object). On
// a partial read it either returns io.EOF (if the object is exhausted)
// or recursively fetches the next window. Caller must hold r.mu.
func (r *remoteStorageReader) serveFromCacheLocked(bs []byte, off int64) (int, error) {
	rel := off - r.cacheOffs
	n := copy(bs, r.cache[rel:])
	metricsReads.Inc()
	metricsReadBytes.Add(float64(n))

	if n == len(bs) {
		r.maybeStartPrefetchLocked()
		return n, nil
	}
	if r.sizeKnown && off+int64(n) >= r.payloadSize {
		return n, io.EOF
	}
	more, err := r.fetchAndCopyLocked(bs[n:], off+int64(n))
	return n + more, err
}

// fetchAndCopyLocked issues a Range GET starting at payload offset
// `off` big enough to cover the request, refreshes the per-reader
// cache with the result, and copies into bs. Caller must hold r.mu.
func (r *remoteStorageReader) fetchAndCopyLocked(bs []byte, off int64) (int, error) {
	fetchLen := int64(r.rangeCacheSize)
	if int64(len(bs)) > fetchLen {
		fetchLen = int64(len(bs))
	}

	ctx := context.Background()
	reader, err := r.r.Get(ctx, r.name, off+r.baseOffset, fetchLen)
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

	r.cache = data
	r.cacheOffs = off
	if int64(len(data)) < fetchLen {
		r.sizeKnown = true
		r.payloadSize = off + int64(len(data))
	}

	n := copy(bs, data)
	metricsReads.Inc()
	metricsReadBytes.Add(float64(n))
	if n < len(bs) {
		return n, io.EOF
	}
	r.maybeStartPrefetchLocked()
	return n, nil
}

// maybeStartPrefetchLocked spawns one background Get for the window
// immediately after the current cache, if and only if no prefetch is
// already in flight and we don't already know the payload ends inside
// the current cache. Caller must hold r.mu.
func (r *remoteStorageReader) maybeStartPrefetchLocked() {
	if r.pf != nil {
		return
	}
	if r.sizeKnown {
		return
	}
	nextOffs := r.cacheOffs + int64(len(r.cache))
	if nextOffs <= 0 {
		return
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
