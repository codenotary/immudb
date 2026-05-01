/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
*/

package remoteapp

import (
	"context"
	"io"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/memory"
)

// rttStorage wraps any remotestorage.Storage and adds a configurable
// per-Get + per-Put + per-Exists sleep so benchmarks can simulate S3
// round-trip time on a fast local backend. Counts each operation so
// we can report how many round trips a benchmark actually issues.
type rttStorage struct {
	remotestorage.Storage
	rtt    time.Duration
	gets   int64
	puts   int64
	exists int64
}

func (r *rttStorage) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&r.gets, 1)
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Get(ctx, name, offs, size)
}

func (r *rttStorage) Put(ctx context.Context, name, fileName string) error {
	atomic.AddInt64(&r.puts, 1)
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Put(ctx, name, fileName)
}

func (r *rttStorage) Exists(ctx context.Context, name string) (bool, error) {
	atomic.AddInt64(&r.exists, 1)
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Exists(ctx, name)
}

func (r *rttStorage) reset() {
	atomic.StoreInt64(&r.gets, 0)
	atomic.StoreInt64(&r.puts, 0)
	atomic.StoreInt64(&r.exists, 0)
}

// --- #3: skip post-upload verification ---
//
// Compares the Append + Flush + chunk-rotation cost with the legacy
// verification path (Put + Exists + remote-reader open) vs the new
// fast path (just Put). Reports gets/puts/exists per chunk written.
//
// Workload: 8 chunks of 8 KiB each, simulated 50 ms RTT.

func benchUpload(b *testing.B, verifyUploads bool) {
	const (
		chunks  = 8
		chunkSz = 8 * 1024
		rtt     = 50 * time.Millisecond
	)
	payload := make([]byte, chunkSz*chunks)
	rand.New(rand.NewSource(1)).Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := &rttStorage{Storage: memory.Open(), rtt: rtt}
		opts := DefaultOptions().
			WithVerifyUploads(verifyUploads).
			WithRetryMinDelay(time.Microsecond).
			WithRetryMaxDelay(time.Microsecond)
		opts.WithFileSize(chunkSz).WithFileExt("aof")
		// Disable the chunk-prefetch for this isolated upload bench.
		opts.WithPrefetchAheadDepth(0)

		app, err := Open(b.TempDir(), "", store, opts)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := app.Append(payload); err != nil {
			b.Fatal(err)
		}
		if err := app.Flush(); err != nil {
			b.Fatal(err)
		}
		// Wait for all chunks to actually upload before tearing down.
		for c := int64(0); c < chunks-1; c++ {
			waitForChunkState(app, int(c), chunkState_Remote)
		}
		_ = app.Close()
		b.StopTimer()
		b.ReportMetric(float64(atomic.LoadInt64(&store.gets))/float64(chunks-1), "gets/chunk")
		b.ReportMetric(float64(atomic.LoadInt64(&store.puts))/float64(chunks-1), "puts/chunk")
		b.ReportMetric(float64(atomic.LoadInt64(&store.exists))/float64(chunks-1), "exists/chunk")
		b.StartTimer()
	}
}

func BenchmarkWave3_Upload_Legacy(b *testing.B) { benchUpload(b, true) }
func BenchmarkWave3_Upload_Fast(b *testing.B)   { benchUpload(b, false) }

// --- #1: parallel multi-chunk prefetch on the multiapp layer ---
//
// Sequentially reads N adjacent chunks through the full remoteapp +
// multiapp stack. With prefetchDepth=0, each chunk's open RTT
// serializes with the consumer. With depth=4, the next 4 opens
// overlap with consumer per-page processing — at 50 ms RTT this is
// the wall-clock difference between (chunks × RTT) and (RTT + work).

func benchMultiChunkSequential(b *testing.B, prefetchDepth int) {
	const (
		chunks      = 16
		chunkSz     = 8 * 1024
		pageSz      = 1024
		perPageWork = 5 * time.Millisecond // realistic decode/replay
		rtt         = 50 * time.Millisecond
	)

	store := &rttStorage{Storage: memory.Open(), rtt: rtt}
	path := b.TempDir()

	// Build a multiapp pre-populated with `chunks` worth of data.
	writeOpts := DefaultOptions().
		WithRetryMinDelay(time.Microsecond).
		WithRetryMaxDelay(time.Microsecond)
	writeOpts.WithFileSize(chunkSz).WithFileExt("aof")
	writeOpts.WithPrefetchAheadDepth(0)

	app, err := Open(path, "", store, writeOpts)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, chunkSz*chunks)
	rand.New(rand.NewSource(2)).Read(payload)
	if _, _, err := app.Append(payload); err != nil {
		b.Fatal(err)
	}
	if err := app.Flush(); err != nil {
		b.Fatal(err)
	}
	for c := int64(0); c < chunks-1; c++ {
		waitForChunkState(app, int(c), chunkState_Remote)
	}
	_ = app.Close()

	store.reset()

	buf := make([]byte, pageSz)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readOpts := DefaultOptions().
			WithRetryMinDelay(time.Microsecond).
			WithRetryMaxDelay(time.Microsecond)
		readOpts.WithReadOnly(true).
			WithFileSize(chunkSz).
			WithFileExt("aof").
			WithMaxOpenedFiles(chunks + 4)
		readOpts.WithPrefetchAheadDepth(prefetchDepth)
		readApp, err := Open(path, "", store, readOpts)
		if err != nil {
			b.Fatal(err)
		}
		var off int64
		end := int64(chunkSz * (chunks - 1))
		for off < end {
			n, err := readApp.ReadAt(buf, off)
			if err != nil && err != io.EOF {
				b.Fatal(err)
			}
			if n == 0 {
				break
			}
			time.Sleep(perPageWork)
			off += int64(n)
		}
		_ = readApp.Close()
	}
	b.StopTimer()
	b.ReportMetric(float64(atomic.LoadInt64(&store.gets))/float64(b.N), "gets/op")
}

func BenchmarkWave1_MultiChunkRead_NoPrefetch(b *testing.B) {
	benchMultiChunkSequential(b, 0)
}
func BenchmarkWave1_MultiChunkRead_Prefetch4(b *testing.B) {
	benchMultiChunkSequential(b, 4)
}

// --- #2: compression on the wire ---
//
// Writes the same compressible payload with and without compression,
// then measures (a) bytes uploaded to the remote storage and (b)
// wall-clock for a sequential ReadAt-back of every byte. Reports the
// upload size as `wire-bytes`.

func benchCompression(b *testing.B, format int) {
	// Single chunk, single Append, large enough that the
	// compression ratio is meaningful and small enough that the
	// per-Append frame fits in one chunk file.
	const payloadSize = 256 * 1024
	const chunkSz = 1024 * 1024 // generous chunk so the whole payload fits

	// Highly compressible payload — long runs of repeating bytes.
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('A' + (i / 4096))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store := &rttStorage{Storage: memory.Open()}
		opts := DefaultOptions().
			WithRetryMinDelay(time.Microsecond).
			WithRetryMaxDelay(time.Microsecond)
		opts.WithCompressionFormat(format).
			WithCompresionLevel(appendable.BestSpeed).
			WithFileSize(chunkSz).
			WithFileExt("aof")
		opts.WithPrefetchAheadDepth(0)

		app, err := Open(b.TempDir(), "", store, opts)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := app.Append(payload); err != nil {
			b.Fatal(err)
		}
		if err := app.Flush(); err != nil {
			b.Fatal(err)
		}
		// Force the chunk to rotate and upload by closing the
		// appendable. Close blocks until in-flight uploads drain.
		_ = app.Close()

		entries, _, err := store.ListEntries(context.Background(), "")
		if err != nil {
			b.Fatal(err)
		}
		var total int64
		for _, e := range entries {
			total += e.Size
		}

		b.StopTimer()
		b.ReportMetric(float64(total), "wire-bytes")
		if total > 0 {
			b.ReportMetric(float64(len(payload))/float64(total), "compression-ratio")
		}
		b.StartTimer()
	}
}

func BenchmarkWave2_Compress_None(b *testing.B) {
	benchCompression(b, appendable.NoCompression)
}
func BenchmarkWave2_Compress_Flate(b *testing.B) {
	benchCompression(b, appendable.FlateCompression)
}
func BenchmarkWave2_Compress_GZip(b *testing.B) {
	benchCompression(b, appendable.GZipCompression)
}
func BenchmarkWave2_Compress_ZLib(b *testing.B) {
	benchCompression(b, appendable.ZLibCompression)
}
