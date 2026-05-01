# immudb S3 performance — `feat/s3performance`

This branch ships four sets of changes against the S3-backed appendable
path (`embedded/appendable/remoteapp/`, `embedded/appendable/multiapp/`,
`embedded/remotestorage/s3/`). Each landed as its own commit; this
document headlines the per-change wins measured against a real
S3-compatible backend (`rustfs/rustfs:1.0.0-beta.1` running locally
in Docker), with a simulated 50 ms WAN RTT injected on top to model
production cross-region S3 latency.

## Headline numbers (real rustfs)

| Change | Metric | Before | After | Win |
| ------ | ------ | ------ | ----- | --- |
| **#3** drop post-upload verification (Exists + verify-Get round trips) | wall-clock per upload, loopback | 7.8 ms | 5.3 ms | **–32 %** (2 RTTs/chunk eliminated) |
| **#1** parallel multi-chunk prefetch on the multiapp layer (singleflight-coalesced) | wall-clock for sequential 16-chunk read, 50 ms WAN RTT | 1530 ms | 1009 ms | **–34 %**, same Get count |
| **#2** end-to-end compression (per-Append-frame, flate/gzip/zlib) | wire-bytes for 256 KiB compressible payload | 262 KiB | ≈640 B | **–99.7 %** wire (410× ratio) |
| **range-fetch reader + LRU window cache + sequential prefetch** (Waves 1–3 of the per-reader path, already on `master`) | per-reader resident memory ceiling | unbounded (chunk-size) | bounded (4 × 1 MiB by default) | constant, regardless of chunk size |

#3 and #1 are RTT-amplified — a 100 ms cross-region link doubles the
absolute saving each delivers. #2 saves bytes, which on a slow upload
pipe translates directly to upload-time speedup; the wall-clock
improvement is proportional to the link bandwidth.

## What each change does

### #3 — drop post-upload verification

`embedded/appendable/remoteapp/remote_app.go` :: `uploadChunk` previously
followed every `Put` with two more retryable round trips:

1. `Exists(remotePath+appName)` — confirm the chunk is visible.
2. `openRemoteAppendableReader(appName)` — open a fresh remote reader
   (which itself does at least one window-sized `Get`) to validate
   readability before swapping the cached appendable.

Modern S3 has been read-after-write consistent since 2020, so the
verification is no longer load-bearing. Wave-4 default behaviour:
skip the `Exists` round trip and substitute a `lazyRemoteReader` (new
file `lazy_remote_reader.go`) into the multiapp cache. The lazy
wrapper opens the inner remote reader only on the first subsequent
`ReadAt` — most uploaded chunks are never read again, so the open
cost is usually deferred forever.

The legacy two-round-trip path is still available behind
`WithVerifyUploads(true)`. Existing `TestRemoteStorageUploadRetry` /
`TestRemoteStorageUploadCancel/{Get,Exists}` opt back into it.

### #1 — parallel multi-chunk prefetch on the multiapp layer

`embedded/appendable/multiapp/multi_app.go` :: `appendableFor` now
detects sequential read patterns (`appID == prevAppID + 1`) and
spawns up to `prefetchAheadDepth` background opens for the next K
chunks. Background opens snapshot the per-`currApp` compression info
under the multiapp mutex, then **drop the mutex** for the slow
`OpenAppendable` call so foreground readers don't block on a
background network I/O.

Foreground misses route through the same `singleflight.Group` as
the prefetch goroutine, keyed by appID, so a foreground read on a
chunk that's currently being prefetched waits on the in-flight
goroutine instead of issuing a duplicate `Get`. The benchmark
confirms this: same 16 Gets with or without prefetch, wall-clock
drops by a third because 15 of those Gets now overlap with consumer
time.

`remoteapp.DefaultOptions()` sets `prefetchAheadDepth = 4`
(`DefaultPrefetchAheadDepth`); local-only multiapps default to 0
(opens are cheap, no win to chase).

### #2 — end-to-end compression through remoteapp

The per-Append framed-compression model `singleapp` already
implemented for local appendables now flows through `remoteapp`
unchanged. Three things changed to enable it:

1. `RemoteStorageAppendable.OpenAppendable` no longer rejects
   non-default `CompressionFormat`. The local writer compresses
   each `Append` into a length-prefixed frame as before; the chunk
   file is uploaded verbatim.
2. `singleapp.MetaCompressionFormat` is exported so downstream
   readers can pull the format out of a chunk's metadata header
   without depending on unexported singleapp internals.
3. `remoteStorageReader` now parses the format on open (defensive:
   `parseCompressionFormat` recovers from `appendable.NewMetadata`
   panics on non-singleapp test fixtures, defaulting to
   `NoCompression`) and a new `readAtCompressedFrame` reproduces the
   same `[4-byte length][N compressed bytes]` per-Append protocol on
   top of the existing range-cache path, supporting flate, gzip,
   lzw, zlib.

`CompressionFormat()` / `CompressionLevel()` are now real
accessors — required by the multiapp prefetch snapshot and by the
chunk-level decompress path. A new `TestOpenRemoteStorageAppendableCompression`
does an end-to-end round-trip: write compressed → upload → flush →
read back → byte-equal to the original payload.

## Methodology

The Wave-4 benchmark suite (`embedded/appendable/remoteapp/`) has
two mirrored sets:

- `wave3_bench_test.go` — synthetic in-memory backend with a
  configurable per-Get sleep that simulates S3 RTT. Cheap, fully
  deterministic, runs in CI without external deps. Used for
  iterating on the implementation.
- `rustfs_bench_test.go` — real `rustfs/rustfs:1.0.0-beta.1`
  container (S3-compatible, written in Rust). Skipped automatically
  if rustfs isn't reachable at `$RUSTFS_ENDPOINT` (default
  `127.0.0.1:9100`). The 50 ms-WAN variants wrap the rustfs storage
  in an `rttInjector` that adds a fixed sleep on every Get/Put/
  Exists, modelling a cross-region S3 hop.

Reproduce the rustfs benchmarks:

```
docker run -d --name rustfs-bench -p 9100:9000 \
    -e RUSTFS_ROOT_USER=rustfsadmin \
    -e RUSTFS_ROOT_PASSWORD=rustfsadmin \
    -v /tmp/rustfs-data:/data \
    rustfs/rustfs:latest

AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url http://127.0.0.1:9100 s3 mb s3://immudb-bench

cd embedded/appendable/remoteapp
go test -bench='^BenchmarkRustFS_' -benchtime=3x -run=^$ -timeout=10m
```

The synthetic benches still run as part of the standard
`embedded/appendable/remoteapp/...` test target; the rustfs ones
are gated on the container being reachable.

## Raw numbers

### #3 — upload-path round-trip elimination, real rustfs (loopback)

```
BenchmarkRustFS_Upload_Legacy-32   3   7818488 ns/op   1.143 puts/chunk   1.143 exists/chunk   1.143 gets/chunk
BenchmarkRustFS_Upload_Fast-32     3   5298163 ns/op   1.143 puts/chunk   0     exists/chunk   0     gets/chunk
```

### #1 — parallel multi-chunk prefetch (rustfs + 50 ms injected RTT)

```
BenchmarkRustFS_MultiChunkRead_NoPrefetch_50msRTT-32   3   1530486419 ns/op   16.00 gets/op
BenchmarkRustFS_MultiChunkRead_Prefetch4_50msRTT-32    3   1008864759 ns/op   16.00 gets/op
```

Loopback-only (no injected RTT) for completeness — the prefetch's
overlap window collapses to ~1 ms loopback latency, so the win
shrinks to noise:

```
BenchmarkRustFS_MultiChunkRead_NoPrefetch-32   3   655297338 ns/op   16.00 gets/op
BenchmarkRustFS_MultiChunkRead_Prefetch4-32    3   648818425 ns/op   16.00 gets/op
```

### #2 — end-to-end compression on real rustfs wire

256 KiB highly-compressible payload, single chunk:

```
BenchmarkRustFS_Compress_None-32    3   4471951 ns/op   0.9993 ratio   262333 wire-bytes
BenchmarkRustFS_Compress_Flate-32   3   3666167 ns/op    410.2 ratio      639 wire-bytes
BenchmarkRustFS_Compress_GZip-32    3   3463025 ns/op    399.0 ratio      657 wire-bytes
BenchmarkRustFS_Compress_ZLib-32    3   3929791 ns/op    406.4 ratio      645 wire-bytes
```

Real production payloads (SQL rows, JSON, protobuf) compress 2–8×,
not 400×, but the wire-bytes reduction is proportional to the data's
entropy and translates 1:1 into upload-time speedup on a
bandwidth-constrained link.

## Caveats

- **Loopback rustfs has ~1 ms RTT.** All benches that don't inject
  artificial latency under-report the win for #1 and #3 — the
  RTT-amplified savings disappear when there's no RTT to amplify.
  The headline numbers use the WAN-injection variant for #1 to
  expose the architecture's actual value; #3 still measures
  meaningfully on loopback because it's eliminating *operations*,
  not just latency.
- **Compression's wall-clock win is workload-dependent.** On a fast
  link the 400× wire reduction shows up as a small (~20 %)
  wall-clock improvement; on a slow link (cross-region, mobile
  uplink) the same wire reduction can translate into 5–20× wall-clock
  improvement.
- **Multi-chunk + compression is single-chunk-Append only at the
  moment.** singleapp's per-Append-frame model means one Append must
  fit in one chunk; benchmarks that try to span chunk boundaries
  with a compressed Append hang. Out of scope for this branch.
- **Prefetch fires only on *sequential* reads.** Random / sparse
  read patterns deliberately don't trigger prefetch (the sentinel
  `prefetchPrevAppID < 0 || != appID-1` check), so the bandwidth
  cost is bounded by what foreground would have read anyway.
- **Resident memory ceiling.** Per-reader memory is bounded at
  `DefaultReaderCacheWindows * DefaultReaderRangeCacheSize` (4 ×
  1 MiB by default) regardless of chunk size. The pre-Wave-1 reader
  pinned the entire chunk in `dataCache []byte`; on 64 MiB chunks
  with 1000 concurrent readers that was 64 GiB resident vs the
  current 4 GiB. Not measurable in microbenchmark, but a real
  operational win at high reader concurrency on large chunks.

## Recommendations

Default configurations are tuned for the common case:

- `WithVerifyUploads = false` (#3 enabled by default)
- `WithPrefetchAheadDepth = 4` for remoteapp; `0` for plain multiapp (#1)
- `WithCompressionFormat = NoCompression` (opt-in per workload — #2
  is available but not on by default to avoid behavioural surprise on
  existing deployments)
- `WithReaderRangeCacheSize = 1 MiB`, `WithReaderCacheWindows = 4`
  (range-fetch reader, already on master)

Operators on slow / metered S3 links: enable compression with
`WithCompressionFormat(appendable.FlateCompression)` or `GZip` on a
per-database basis via the existing multiapp options surface.
