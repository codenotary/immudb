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
// response was strictly shorter than the request) pins the payload size
// so subsequent ReadAt past EOF can short-circuit without an S3 round
// trip.
//
// 256 KiB is large enough that hot-restore replay and SQL scans fall
// almost entirely inside the cache while sequentially walking a chunk,
// and small enough that random-access patterns don't drag a megabyte
// per ReadAt call. Tunable via WithReaderRangeCacheSize on the
// remoteapp options.
const DefaultReaderRangeCacheSize = 256 * 1024

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
}

func openRemoteStorageReader(r remotestorage.Storage, name string, rangeCacheSize int) (*remoteStorageReader, error) {
	defer prometheus.NewTimer(metricsOpenTime).ObserveDuration()

	if rangeCacheSize <= 0 {
		rangeCacheSize = DefaultReaderRangeCacheSize
	}

	ctx := context.Background()

	// Read header + as much of the leading payload as fits in one window.
	// A short response (len < rangeCacheSize) means we've seen the whole
	// object, in which case pin payloadSize so out-of-range ReadAt calls
	// don't issue a wasted GET.
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

	sr := &remoteStorageReader{
		r:              r,
		name:           name,
		baseOffset:     baseOffset,
		rangeCacheSize: rangeCacheSize,
		cache:          data[baseOffset:],
		cacheOffs:      0,
	}
	if int64(len(data)) < int64(rangeCacheSize) {
		sr.sizeKnown = true
		sr.payloadSize = int64(len(sr.cache))
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
		rel := off - r.cacheOffs
		n := copy(bs, r.cache[rel:])
		metricsReads.Inc()
		metricsReadBytes.Add(float64(n))

		if n == len(bs) {
			return n, nil
		}
		// Cache covered the prefix but not the tail. Either we ran off the
		// end of the cache window (need another GET) or off the end of the
		// object (return EOF without one).
		if r.sizeKnown && off+int64(n) >= r.payloadSize {
			return n, io.EOF
		}
		more, err := r.fetchAndCopyLocked(bs[n:], off+int64(n))
		return n + more, err
	}

	// Cache miss — fetch a fresh window centered on `off`.
	return r.fetchAndCopyLocked(bs, off)
}

// fetchAndCopyLocked issues a Range GET starting at payload offset `off`
// big enough to cover the request, refreshes the per-reader cache with
// the result, and copies into bs. Caller must hold r.mu.
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
	return n, nil
}

func (r *remoteStorageReader) Close() error {
	return nil
}

func (r *remoteStorageReader) Copy(dstPath string) error {
	panic("unimplemented")
}

var _ appendable.Appendable = (*remoteStorageReader)(nil)
