/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package remoteapp

import (
	"context"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
)

// latencyStorage wraps any remotestorage.Storage and adds a fixed
// per-Get sleep on the calling goroutine, simulating S3 round-trip
// time. Lets benchmarks measure how well the reader amortizes
// network round trips without needing a real S3 / minio process.
//
// Only Get is delayed — the cost we're trying to amortize is the
// per-request HTTP latency on the read path. getCount counts the
// number of Get calls so benchmarks can verify the wire-cost
// reduction from range caching / prefetch.
type latencyStorage struct {
	remotestorage.Storage
	getDelay time.Duration
	getCount int64
}

func (l *latencyStorage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&l.getCount, 1)
	if l.getDelay > 0 {
		time.Sleep(l.getDelay)
	}
	return l.Storage.Get(ctx, name, offs, size)
}

func (l *latencyStorage) Gets() int64 {
	return atomic.LoadInt64(&l.getCount)
}

// putRawTB uploads `data` directly to `s` under `name`. Mirrors the
// *testing.T-only storeData helper in remote_storage_reader_test.go
// but accepts testing.TB so benchmarks can reuse the same flow.
func putRawTB(tb testing.TB, s remotestorage.Storage, name string, data []byte) {
	tb.Helper()
	fl, err := os.CreateTemp("", "")
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := fl.Write(data); err != nil {
		tb.Fatal(err)
	}
	if err := fl.Close(); err != nil {
		tb.Fatal(err)
	}
	defer os.Remove(fl.Name())
	if err := s.Put(context.Background(), name, fl.Name()); err != nil {
		tb.Fatal(err)
	}
}

// BenchmarkRemoteReader_SequentialRead measures the cost of opening
// a chunk and reading it end-to-end in fixed-size pages. The Wave 1
// range cache should keep the per-page cost down to a memcpy after
// the first page in each cache window.
//
// Reports:
//   - ns/op: wall-clock per full chunk read (open + N pages)
//   - gets/op: number of S3 Get calls per iteration. Should be ~
//     ceil(payloadBytes / rangeCacheSize) + 1 (the +1 is open).
func BenchmarkRemoteReader_SequentialRead(b *testing.B) {
	const (
		payloadBytes = 4 * 1024 * 1024 // 4 MiB chunk
		pageSize     = 8 * 1024        // 8 KiB read pages
		rttSimulated = 2 * time.Millisecond
	)

	store := &latencyStorage{Storage: memory.Open(), getDelay: rttSimulated}
	header := []byte{0, 0, 0, 0}
	payload := make([]byte, payloadBytes)
	rand.New(rand.NewSource(1)).Read(payload)
	putRawTB(b, store, "fl", append(header, payload...))

	buf := make([]byte, pageSize)

	b.ResetTimer()
	startGets := store.Gets()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(payloadBytes) {
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
	gets := store.Gets() - startGets
	b.ReportMetric(float64(gets)/float64(b.N), "gets/op")
}

// BenchmarkRemoteReader_SequentialReadWithProcessing models the real
// hot-restore consumer: per-page work (decode, validate, replay) that
// dominates a memcpy. With Wave 2's read-ahead prefetch, the next
// cache window's Get completes in parallel with this processing, so
// the per-window RTT is hidden — wall clock should track
// Σ(processing) + 1 RTT instead of Σ(processing) + N RTT.
func BenchmarkRemoteReader_SequentialReadWithProcessing(b *testing.B) {
	const (
		payloadBytes  = 4 * 1024 * 1024
		pageSize      = 8 * 1024
		rttSimulated  = 2 * time.Millisecond
		perPageWorkUs = 80 // ~10 ms per 256 KiB window — well above RTT
	)

	store := &latencyStorage{Storage: memory.Open(), getDelay: rttSimulated}
	header := []byte{0, 0, 0, 0}
	payload := make([]byte, payloadBytes)
	rand.New(rand.NewSource(3)).Read(payload)
	putRawTB(b, store, "fl", append(header, payload...))

	buf := make([]byte, pageSize)

	b.ResetTimer()
	startGets := store.Gets()
	for i := 0; i < b.N; i++ {
		r, err := openRemoteStorageReader(store, "fl", DefaultReaderRangeCacheSize)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		for off < int64(payloadBytes) {
			n, err := r.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			// Simulate per-page consumer work (protobuf decode etc).
			time.Sleep(time.Duration(perPageWorkUs) * time.Microsecond)
			off += int64(n)
			if err == io.EOF {
				break
			}
		}
		_ = r.Close()
	}
	b.StopTimer()
	gets := store.Gets() - startGets
	b.ReportMetric(float64(gets)/float64(b.N), "gets/op")
}

// BenchmarkRemoteReader_SparseRead measures cost of N small random
// reads on a single chunk. Without range caching this is N full-chunk
// downloads; with Wave 1 it's at most N bounded-window Gets, and for
// reads that happen to land in the most-recent window, zero extra Gets.
func BenchmarkRemoteReader_SparseRead(b *testing.B) {
	const (
		payloadBytes = 4 * 1024 * 1024
		readSize     = 256
		readsPerIter = 100
		rttSimulated = 2 * time.Millisecond
	)

	store := &latencyStorage{Storage: memory.Open(), getDelay: rttSimulated}
	header := []byte{0, 0, 0, 0}
	payload := make([]byte, payloadBytes)
	rand.New(rand.NewSource(2)).Read(payload)
	putRawTB(b, store, "fl", append(header, payload...))

	rng := rand.New(rand.NewSource(99))
	offsets := make([]int64, readsPerIter)
	for i := range offsets {
		offsets[i] = rng.Int63n(int64(payloadBytes - readSize))
	}
	buf := make([]byte, readSize)

	b.ResetTimer()
	startGets := store.Gets()
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
	gets := store.Gets() - startGets
	b.ReportMetric(float64(gets)/float64(b.N), "gets/op")
}

// BenchmarkRemoteReader_HotRestoreReplay mirrors the dominant
// hot-restore / replication catch-up workload: open many adjacent
// chunks and read each fully in order. This is the path Wave 2's
// sequential prefetch is meant to accelerate by overlapping the
// per-window Get RTT with the ReadAt copy.
func BenchmarkRemoteReader_HotRestoreReplay(b *testing.B) {
	const (
		chunks       = 8
		payloadBytes = 1 * 1024 * 1024 // 1 MiB per chunk (default WithFileSize)
		pageSize     = 8 * 1024
		rttSimulated = 2 * time.Millisecond
	)

	store := &latencyStorage{Storage: memory.Open(), getDelay: rttSimulated}
	header := []byte{0, 0, 0, 0}
	for c := 0; c < chunks; c++ {
		payload := make([]byte, payloadBytes)
		rand.New(rand.NewSource(int64(c))).Read(payload)
		putRawTB(b, store, chunkName(c), append(header, payload...))
	}

	buf := make([]byte, pageSize)

	b.ResetTimer()
	startGets := store.Gets()
	for i := 0; i < b.N; i++ {
		for c := 0; c < chunks; c++ {
			r, err := openRemoteStorageReader(store, chunkName(c), DefaultReaderRangeCacheSize)
			if err != nil {
				b.Fatal(err)
			}
			var off int64
			for off < int64(payloadBytes) {
				n, err := r.ReadAt(buf, off)
				if err != nil && err != io.EOF {
					b.Fatal(err)
				}
				// Lightweight per-page work — enough that the
				// next-window prefetch RTT can overlap with consumer
				// time, mirroring real replay.
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
	gets := store.Gets() - startGets
	b.ReportMetric(float64(gets)/float64(b.N), "gets/op")
}

func chunkName(i int) string {
	return "chunk_" + string(rune('a'+i))
}
