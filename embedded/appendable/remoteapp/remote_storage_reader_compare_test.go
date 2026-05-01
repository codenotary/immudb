/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package remoteapp

import (
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
)

// oldStyleReader is the pre-Wave-1 implementation of the remote-storage
// reader — it eagerly downloads the entire chunk on open and slices the
// resulting buffer for ReadAt. Embedded here verbatim so the
// side-by-side benchmarks below run both code paths in one go test
// invocation without checkout-dancing across commits.
//
// This is the v1.11.0 baseline reproduced from
// embedded/appendable/remoteapp/remote_storage_reader.go before
// commit b65a7c10 ("perf(remoteapp): range-fetch ReadAt").
type oldStyleReader struct {
	r          remotestorage.Storage
	name       string
	baseOffset int64
	dataCache  []byte
}

func openOldStyleReader(r remotestorage.Storage, name string) (*oldStyleReader, error) {
	reader, err := r.Get(context.Background(), name, 0, -1)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, ErrCorruptedMetadata
	}
	baseOffset := int64(4 + binary.BigEndian.Uint32(data[:4]))
	if baseOffset > int64(len(data)) {
		return nil, ErrCorruptedMetadata
	}
	return &oldStyleReader{
		r:          r,
		name:       name,
		baseOffset: baseOffset,
		dataCache:  data[baseOffset:],
	}, nil
}

func (r *oldStyleReader) ReadAt(bs []byte, off int64) (int, error) {
	if off < 0 {
		return 0, ErrIllegalArguments
	}
	if off > int64(len(r.dataCache)) {
		return 0, io.EOF
	}
	n := copy(bs, r.dataCache[off:])
	if n < len(bs) {
		return n, io.EOF
	}
	return n, nil
}

func (r *oldStyleReader) Close() error { return nil }

// byteCountingStorage wraps a Storage and counts both Get calls and the
// total number of payload bytes returned. Used to report the actual
// over-the-wire cost the two readers pay.
type byteCountingStorage struct {
	remotestorage.Storage
	getDelay time.Duration
	gets     int64
	bytes    int64
}

func (b *byteCountingStorage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&b.gets, 1)
	if b.getDelay > 0 {
		time.Sleep(b.getDelay)
	}
	rc, err := b.Storage.Get(ctx, name, offs, size)
	if err != nil {
		return nil, err
	}
	return &byteCountingReadCloser{rc: rc, parent: b}, nil
}

func (b *byteCountingStorage) reset() {
	atomic.StoreInt64(&b.gets, 0)
	atomic.StoreInt64(&b.bytes, 0)
}

type byteCountingReadCloser struct {
	rc     io.ReadCloser
	parent *byteCountingStorage
}

func (b *byteCountingReadCloser) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		atomic.AddInt64(&b.parent.bytes, int64(n))
	}
	return n, err
}

func (b *byteCountingReadCloser) Close() error { return b.rc.Close() }

// --- shared workload setup ---

const (
	cmpPayloadBytes = 4 * 1024 * 1024 // 4 MiB chunk
	cmpPageSize     = 8 * 1024        // 8 KiB read pages
	cmpReadSize     = 256             // sparse read window
	cmpReads        = 100             // sparse reads per iter
	cmpChunks       = 8               // hot-restore chunk count
	cmpChunkBytes   = 1 * 1024 * 1024 // hot-restore per-chunk size
)

func cmpSeed(b *testing.B, rttMs int) (*byteCountingStorage, []byte) {
	b.Helper()
	store := &byteCountingStorage{
		Storage:  memory.Open(),
		getDelay: time.Duration(rttMs) * time.Millisecond,
	}
	header := []byte{0, 0, 0, 0}
	payload := make([]byte, cmpPayloadBytes)
	rand.New(rand.NewSource(7)).Read(payload)
	putRawTB(b, store, "fl", append(header, payload...))
	return store, payload
}

func cmpSeedMulti(b *testing.B, rttMs int) *byteCountingStorage {
	b.Helper()
	store := &byteCountingStorage{
		Storage:  memory.Open(),
		getDelay: time.Duration(rttMs) * time.Millisecond,
	}
	header := []byte{0, 0, 0, 0}
	for c := 0; c < cmpChunks; c++ {
		payload := make([]byte, cmpChunkBytes)
		rand.New(rand.NewSource(int64(c) + 11)).Read(payload)
		putRawTB(b, store, chunkName(c), append(header, payload...))
	}
	return store
}

func cmpReportLocked(b *testing.B, store *byteCountingStorage) {
	b.Helper()
	b.ReportMetric(float64(atomic.LoadInt64(&store.gets))/float64(b.N), "gets/op")
	b.ReportMetric(float64(atomic.LoadInt64(&store.bytes))/float64(b.N), "wire-bytes/op")
}

// --- sequential, no per-page work ---

func BenchmarkCompare_Old_SequentialRead(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openOldStyleReader(store, "fl")
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_SequentialRead(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

// --- sequential WITH per-page consumer work (where prefetch shines) ---

const cmpPerPageWork = 80 * time.Microsecond

func BenchmarkCompare_Old_SequentialReadWithProcessing(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openOldStyleReader(store, "fl")
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			time.Sleep(cmpPerPageWork)
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_SequentialReadWithProcessing(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			time.Sleep(cmpPerPageWork)
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

// --- sparse random reads (the read-amplification killer) ---

func BenchmarkCompare_Old_SparseRead(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	rng := rand.New(rand.NewSource(99))
	offsets := make([]int64, cmpReads)
	for i := range offsets {
		offsets[i] = rng.Int63n(int64(cmpPayloadBytes - cmpReadSize))
	}
	buf := make([]byte, cmpReadSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openOldStyleReader(store, "fl")
		if err != nil {
			b.Fatal(err)
		}
		for _, off := range offsets {
			if _, err := r.ReadAt(buf, off); err != nil && err != io.EOF {
				b.Fatal(err)
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_SparseRead(b *testing.B) {
	store, _ := cmpSeed(b, 2)
	rng := rand.New(rand.NewSource(99))
	offsets := make([]int64, cmpReads)
	for i := range offsets {
		offsets[i] = rng.Int63n(int64(cmpPayloadBytes - cmpReadSize))
	}
	buf := make([]byte, cmpReadSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		for _, off := range offsets {
			if _, err := r.ReadAt(buf, off); err != nil && err != io.EOF {
				b.Fatal(err)
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

// --- partial-chunk read (open N chunks but only touch the first ~32 KB) ---
// This is the dominant pattern for "read header / first record / early
// abort" workloads — the old reader pays full-chunk download per open
// regardless; the new reader pays only the bytes it touches.

const cmpPartialReadBytes = 32 * 1024

func BenchmarkCompare_Old_PartialChunkRead(b *testing.B) {
	store := cmpSeedMulti(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openOldStyleReader(store, chunkName(c))
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpPartialReadBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_PartialChunkRead(b *testing.B) {
	store := cmpSeedMulti(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openRemoteStorageReader(store, chunkName(c), DefaultReaderRangeCacheSize)
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpPartialReadBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

// --- high-RTT sweep: 50 ms simulated round trip, where prefetch
//     overlap actually moves the wall-clock needle ---

func BenchmarkCompare_Old_SequentialReadWithProcessing_HighRTT(b *testing.B) {
	store, _ := cmpSeed(b, 50)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openOldStyleReader(store, "fl")
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			time.Sleep(cmpPerPageWork)
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_SequentialReadWithProcessing_HighRTT(b *testing.B) {
	store, _ := cmpSeed(b, 50)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(cmpPayloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			time.Sleep(cmpPerPageWork)
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_Old_PartialChunkRead_HighRTT(b *testing.B) {
	store := cmpSeedMulti(b, 50)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openOldStyleReader(store, chunkName(c))
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpPartialReadBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_PartialChunkRead_HighRTT(b *testing.B) {
	store := cmpSeedMulti(b, 50)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openRemoteStorageReader(store, chunkName(c), DefaultReaderRangeCacheSize)
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpPartialReadBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

// --- hot-restore replay (open N adjacent chunks, walk each) ---

func BenchmarkCompare_Old_HotRestoreReplay(b *testing.B) {
	store := cmpSeedMulti(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openOldStyleReader(store, chunkName(c))
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpChunkBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				time.Sleep(50 * time.Microsecond)
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}

func BenchmarkCompare_New_HotRestoreReplay(b *testing.B) {
	store := cmpSeedMulti(b, 2)
	buf := make([]byte, cmpPageSize)
	store.reset()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for c := 0; c < cmpChunks; c++ {
			r, err := openRemoteStorageReader(store, chunkName(c), DefaultReaderRangeCacheSize)
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(cmpChunkBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				time.Sleep(50 * time.Microsecond)
				off += int64(n)
				if err == io.EOF {
					break
				}
			}
			_ = r.Close()
		}
	}
	b.StopTimer()
	cmpReportLocked(b, store)
}
