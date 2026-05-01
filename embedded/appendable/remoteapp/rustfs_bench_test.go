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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/remotestorage"
	"github.com/codenotary/immudb/embedded/remotestorage/s3"
)

// These benchmarks require a running rustfs (or any S3-compatible
// service) reachable at $RUSTFS_ENDPOINT (default 127.0.0.1:9100)
// with a pre-created bucket $RUSTFS_BUCKET (default "immudb-bench").
// Skipped automatically when the endpoint isn't reachable.
//
// Spin up rustfs locally with:
//   docker run -d --name rustfs-bench -p 9100:9000 \
//     -e RUSTFS_ROOT_USER=rustfsadmin \
//     -e RUSTFS_ROOT_PASSWORD=rustfsadmin \
//     -v /tmp/rustfs-data:/data rustfs/rustfs:latest
//   AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
//     aws --endpoint-url http://127.0.0.1:9100 s3 mb s3://immudb-bench

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func openRustFS(tb testing.TB, prefix string) remotestorage.Storage {
	tb.Helper()
	endpoint := "http://" + envOr("RUSTFS_ENDPOINT", "127.0.0.1:9100")
	bucket := envOr("RUSTFS_BUCKET", "immudb-bench")
	access := envOr("RUSTFS_ACCESS_KEY", "rustfsadmin")
	secret := envOr("RUSTFS_SECRET_KEY", "rustfsadmin")

	store, err := s3.Open(
		endpoint,
		false, "",
		access, secret,
		bucket,
		"us-east-1",
		prefix,
		"http://169.254.169.254",
		false,
	)
	if err != nil {
		tb.Skipf("rustfs unavailable: %v", err)
	}
	return store
}

// countingS3 wraps the s3.Storage and counts each operation; lets us
// report puts/gets/exists per chunk without needing to instrument the
// upstream client.
type countingS3 struct {
	remotestorage.Storage
	gets, puts, exists, removes int64
}

func (c *countingS3) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	atomic.AddInt64(&c.gets, 1)
	return c.Storage.Get(ctx, name, offs, size)
}
func (c *countingS3) Put(ctx context.Context, name, fileName string) error {
	atomic.AddInt64(&c.puts, 1)
	return c.Storage.Put(ctx, name, fileName)
}
func (c *countingS3) Exists(ctx context.Context, name string) (bool, error) {
	atomic.AddInt64(&c.exists, 1)
	return c.Storage.Exists(ctx, name)
}
func (c *countingS3) Remove(ctx context.Context, name string) error {
	atomic.AddInt64(&c.removes, 1)
	return c.Storage.Remove(ctx, name)
}
func (c *countingS3) RemoveAll(ctx context.Context, path string) error {
	atomic.AddInt64(&c.removes, 1)
	return c.Storage.RemoveAll(ctx, path)
}

// uniquePrefix returns a fresh per-bench-iter S3 path so concurrent
// runs on the same bucket don't collide and prior runs don't pollute.
func uniquePrefix(name string) string {
	return name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "/"
}

// --- #3: drop post-upload verification (real rustfs) ---

func benchRustFSUpload(b *testing.B, verifyUploads bool) {
	const (
		chunks  = 8
		chunkSz = 8 * 1024
	)
	payload := make([]byte, chunkSz*chunks)
	rand.New(rand.NewSource(11)).Read(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw := openRustFS(b, uniquePrefix("upload"))
		store := &countingS3{Storage: raw}
		opts := DefaultOptions().
			WithVerifyUploads(verifyUploads).
			WithRetryMinDelay(50 * time.Millisecond).
			WithRetryMaxDelay(time.Second)
		opts.WithFileSize(chunkSz).WithFileExt("aof")
		opts.WithPrefetchAheadDepth(0)

		app, err := Open(b.TempDir(), "rustfs/", store, opts)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := app.Append(payload); err != nil {
			b.Fatal(err)
		}
		if err := app.Flush(); err != nil {
			b.Fatal(err)
		}
		for c := 0; c < chunks-1; c++ {
			waitForChunkState(app, c, chunkState_Remote)
		}
		_ = app.Close()
		b.StopTimer()
		b.ReportMetric(float64(atomic.LoadInt64(&store.puts))/float64(chunks-1), "puts/chunk")
		b.ReportMetric(float64(atomic.LoadInt64(&store.exists))/float64(chunks-1), "exists/chunk")
		b.ReportMetric(float64(atomic.LoadInt64(&store.gets))/float64(chunks-1), "gets/chunk")
		b.StartTimer()
	}
}

func BenchmarkRustFS_Upload_Legacy(b *testing.B) { benchRustFSUpload(b, true) }
func BenchmarkRustFS_Upload_Fast(b *testing.B)   { benchRustFSUpload(b, false) }

// --- #1: parallel multi-chunk prefetch (real rustfs) ---

func benchRustFSMultiChunkSequential(b *testing.B, prefetchDepth int) {
	const (
		chunks      = 16
		chunkSz     = 8 * 1024
		pageSz      = 1024
		perPageWork = 5 * time.Millisecond
	)
	prefix := uniquePrefix("seqread")
	store := openRustFS(b, prefix)
	tmp := b.TempDir()

	// Pre-populate the bucket with `chunks` worth of data.
	writeOpts := DefaultOptions().
		WithRetryMinDelay(50 * time.Millisecond).
		WithRetryMaxDelay(time.Second)
	writeOpts.WithFileSize(chunkSz).WithFileExt("aof")
	writeOpts.WithPrefetchAheadDepth(0)

	app, err := Open(tmp, "rustfs/", store, writeOpts)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, chunkSz*chunks)
	rand.New(rand.NewSource(12)).Read(payload)
	if _, _, err := app.Append(payload); err != nil {
		b.Fatal(err)
	}
	if err := app.Flush(); err != nil {
		b.Fatal(err)
	}
	for c := 0; c < chunks-1; c++ {
		waitForChunkState(app, c, chunkState_Remote)
	}
	_ = app.Close()
	_ = os.RemoveAll(tmp)

	buf := make([]byte, pageSz)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readTmp := b.TempDir()
		counted := &countingS3{Storage: store}
		readOpts := DefaultOptions().
			WithRetryMinDelay(50 * time.Millisecond).
			WithRetryMaxDelay(time.Second)
		readOpts.WithReadOnly(true).
			WithFileSize(chunkSz).
			WithFileExt("aof").
			WithMaxOpenedFiles(chunks + 4)
		readOpts.WithPrefetchAheadDepth(prefetchDepth)

		readApp, err := Open(readTmp, "rustfs/", counted, readOpts)
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
		b.StopTimer()
		b.ReportMetric(float64(atomic.LoadInt64(&counted.gets)), "gets/op")
		b.StartTimer()
	}
}

func BenchmarkRustFS_MultiChunkRead_NoPrefetch(b *testing.B) {
	benchRustFSMultiChunkSequential(b, 0)
}
func BenchmarkRustFS_MultiChunkRead_Prefetch4(b *testing.B) {
	benchRustFSMultiChunkSequential(b, 4)
}

// rttInjector wraps a Storage and adds a fixed sleep on every Get,
// Put and Exists. Models the wall-clock cost of a high-RTT WAN link
// (e.g. cross-region S3) on top of an otherwise-fast local backend.
type rttInjector struct {
	remotestorage.Storage
	rtt time.Duration
}

func (r *rttInjector) Get(ctx context.Context, name string, offs, size int64) (io.ReadCloser, error) {
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Get(ctx, name, offs, size)
}
func (r *rttInjector) Put(ctx context.Context, name, fileName string) error {
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Put(ctx, name, fileName)
}
func (r *rttInjector) Exists(ctx context.Context, name string) (bool, error) {
	if r.rtt > 0 {
		time.Sleep(r.rtt)
	}
	return r.Storage.Exists(ctx, name)
}

func benchRustFSMultiChunkSequentialWithRTT(b *testing.B, prefetchDepth int, rtt time.Duration) {
	const (
		chunks      = 16
		chunkSz     = 8 * 1024
		pageSz      = 1024
		perPageWork = 5 * time.Millisecond
	)
	prefix := uniquePrefix("seqread-rtt")
	rawStore := openRustFS(b, prefix)

	// Pre-populate without injected latency (just real loopback).
	tmp := b.TempDir()
	writeOpts := DefaultOptions().
		WithRetryMinDelay(50 * time.Millisecond).
		WithRetryMaxDelay(time.Second)
	writeOpts.WithFileSize(chunkSz).WithFileExt("aof")
	writeOpts.WithPrefetchAheadDepth(0)

	app, err := Open(tmp, "rustfs/", rawStore, writeOpts)
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, chunkSz*chunks)
	rand.New(rand.NewSource(13)).Read(payload)
	if _, _, err := app.Append(payload); err != nil {
		b.Fatal(err)
	}
	if err := app.Flush(); err != nil {
		b.Fatal(err)
	}
	for c := 0; c < chunks-1; c++ {
		waitForChunkState(app, c, chunkState_Remote)
	}
	_ = app.Close()
	_ = os.RemoveAll(tmp)

	// Reads go through the rttInjector so each Get pays the
	// simulated WAN round trip.
	store := &rttInjector{Storage: rawStore, rtt: rtt}
	buf := make([]byte, pageSz)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readTmp := b.TempDir()
		counted := &countingS3{Storage: store}
		readOpts := DefaultOptions().
			WithRetryMinDelay(50 * time.Millisecond).
			WithRetryMaxDelay(time.Second)
		readOpts.WithReadOnly(true).
			WithFileSize(chunkSz).
			WithFileExt("aof").
			WithMaxOpenedFiles(chunks + 4)
		readOpts.WithPrefetchAheadDepth(prefetchDepth)

		readApp, err := Open(readTmp, "rustfs/", counted, readOpts)
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
		b.StopTimer()
		b.ReportMetric(float64(atomic.LoadInt64(&counted.gets)), "gets/op")
		b.StartTimer()
	}
}

func BenchmarkRustFS_MultiChunkRead_NoPrefetch_50msRTT(b *testing.B) {
	benchRustFSMultiChunkSequentialWithRTT(b, 0, 50*time.Millisecond)
}
func BenchmarkRustFS_MultiChunkRead_Prefetch4_50msRTT(b *testing.B) {
	benchRustFSMultiChunkSequentialWithRTT(b, 4, 50*time.Millisecond)
}

// --- #2: end-to-end compression on real S3 wire ---

func benchRustFSCompression(b *testing.B, format int) {
	const payloadSize = 256 * 1024
	const chunkSz = 1024 * 1024

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte('A' + (i / 4096))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw := openRustFS(b, uniquePrefix("compress"))
		store := &countingS3{Storage: raw}
		opts := DefaultOptions().
			WithRetryMinDelay(50 * time.Millisecond).
			WithRetryMaxDelay(time.Second)
		opts.WithCompressionFormat(format).
			WithCompresionLevel(appendable.BestSpeed).
			WithFileSize(chunkSz).
			WithFileExt("aof")
		opts.WithPrefetchAheadDepth(0)

		app, err := Open(b.TempDir(), "rustfs/", store, opts)
		if err != nil {
			b.Fatal(err)
		}
		if _, _, err := app.Append(payload); err != nil {
			b.Fatal(err)
		}
		if err := app.Flush(); err != nil {
			b.Fatal(err)
		}
		_ = app.Close()

		// Walk the bucket prefix to total wire bytes.
		entries, _, err := raw.ListEntries(context.Background(), "rustfs/")
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

func BenchmarkRustFS_Compress_None(b *testing.B) {
	benchRustFSCompression(b, appendable.NoCompression)
}
func BenchmarkRustFS_Compress_Flate(b *testing.B) {
	benchRustFSCompression(b, appendable.FlateCompression)
}
func BenchmarkRustFS_Compress_GZip(b *testing.B) {
	benchRustFSCompression(b, appendable.GZipCompression)
}
func BenchmarkRustFS_Compress_ZLib(b *testing.B) {
	benchRustFSCompression(b, appendable.ZLibCompression)
}
