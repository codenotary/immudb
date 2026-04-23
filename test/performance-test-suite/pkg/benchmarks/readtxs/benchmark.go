/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package readtxs is the read-side counterpart to the writetxs perf
// benchmark. It populates a database with a fixed key population during
// Warmup, then drives Get traffic from N concurrent workers using a
// Zipf-distributed key selector — modelling real workload locality, where
// a small set of "hot" keys dominate read traffic. Uniform-random reads
// (the existing GetRKey path) hide cache and index effects; the
// BenchmarkRandomGet vs BenchmarkHotSetGet pair already documented this
// asymmetry (−30% on uniform, +25% on hot-set, see
// embedded/store/options.go:48).
package readtxs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks"
)

func h256(s string) []byte {
	h := sha256.New()
	h.Write([]byte(s))
	return h.Sum(nil)
}

// Config drives a readtxs benchmark.
//
// KeyPopulation is the number of keys seeded by Warmup; reads during Run
// target these keys (the Zipf distribution selects an index in [0, KeyPopulation).
//
// ZipfS controls skew: 1.1 → mild (closer to uniform), 2.0 → aggressive
// (most reads hit the top ~1% of keys). Set 0 to fall back to a uniform
// distribution (kt.GetRKey).
type Config struct {
	Name          string
	Workers       int
	KeySize       int
	ValueSize     int
	KeyPopulation uint64
	ZipfS         float64
	ZipfV         float64
}

type benchmark struct {
	cfg Config

	getsSoFar int64
	startTime time.Time

	lastProbeGetsSoFar int64
	lastProbeTime      time.Time

	hwStatsGatherer *benchmarks.HWStatsProber

	m sync.Mutex

	primaryServer *server.ImmuServer
	clients       []client.ImmuClient
	tempDirs      []string
	zipfTracker   *benchmarks.ZipfKeyTracker
	uniformTracker *benchmarks.KeyTracker
}

// Result is what every readtxs benchmark emits. Mirrors the writetxs
// Result shape so perf-delta and the runner JSON consumer can treat both
// symmetrically; "TxTotal/KvTotal" map to "GetTotal" semantically.
type Result struct {
	GetTotal int64               `json:"getTotal"`
	Gets     float64             `json:"gets"`
	GetsInst float64             `json:"getsInstant,omitempty"`
	HWStats  *benchmarks.HWStats `json:"hwStats"`
}

func (r *Result) String() string {
	s := fmt.Sprintf("Get: %d, Get/s: %.2f", r.GetTotal, r.Gets)
	if r.GetsInst != 0.0 {
		s += fmt.Sprintf(", Get/s instant: %.2f", r.GetsInst)
	}
	if r.HWStats != nil {
		s += ", "
		s += r.HWStats.String()
	}
	return s
}

func NewBenchmark(cfg Config) benchmarks.Benchmark {
	return &benchmark{cfg: cfg}
}

func (b *benchmark) Name() string {
	return b.cfg.Name
}

// Warmup stands up an embedded immudb instance, pre-loads KeyPopulation
// keys via the same h256+rand pipeline that writetxs uses, and opens
// Workers client sessions ready for Run.
func (b *benchmark) Warmup(tempDirBase string) error {
	primaryPath, err := os.MkdirTemp(tempDirBase, "rd-test-primary")
	if err != nil {
		return err
	}
	b.tempDirs = append(b.tempDirs, primaryPath)

	primaryServerOpts := server.
		DefaultOptions().
		WithDir(primaryPath).
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithLogFormat(logger.LogFormatJSON).
		WithLogfile("./immudb.log")

	b.primaryServer = server.DefaultServer().WithOptions(primaryServerOpts).(*server.ImmuServer)

	if err := b.primaryServer.Initialize(); err != nil {
		return err
	}

	go func() { b.primaryServer.Start() }()
	time.Sleep(1 * time.Second)

	primaryPort := b.primaryServer.Listener.Addr().(*net.TCPAddr).Port

	b.clients = nil
	for i := 0; i < b.cfg.Workers; i++ {
		path, err := os.MkdirTemp(tempDirBase, "immudb_client")
		if err != nil {
			return err
		}
		c := client.NewClient().WithOptions(client.DefaultOptions().WithPort(primaryPort).WithDir(path))
		if err := c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb"); err != nil {
			return err
		}
		b.clients = append(b.clients, c)
		b.tempDirs = append(b.tempDirs, path)
	}

	// Seed the database via the first client. Batch into 1k-key SetAlls so
	// large populations stay under MaxTxEntries and complete in reasonable
	// wall-clock time during Warmup.
	seedClient := b.clients[0]
	rand := benchmarks.NewRandStringGen(b.cfg.ValueSize)
	defer rand.Stop()

	// Seed keys with sequential indices so the Zipf indexer (which picks
	// uint64 in [0, imax)) addresses them deterministically.
	const seedBatch = 1000
	kt := benchmarks.NewKeyTracker(0)
	for off := uint64(0); off < b.cfg.KeyPopulation; off += seedBatch {
		end := off + seedBatch
		if end > b.cfg.KeyPopulation {
			end = b.cfg.KeyPopulation
		}
		req := schema.SetRequest{
			KVs:    make([]*schema.KeyValue, end-off),
			NoWait: false,
		}
		for i := uint64(0); i < end-off; i++ {
			key := h256(kt.GetWKey())
			if len(key) > b.cfg.KeySize {
				key = key[:b.cfg.KeySize]
			}
			req.KVs[i] = &schema.KeyValue{Key: key, Value: rand.GetRnd()}
		}
		if _, err := seedClient.SetAll(context.Background(), &req); err != nil {
			return fmt.Errorf("readtxs warmup seed: %w", err)
		}
	}

	// kt now has max == KeyPopulation. Re-use the same start so probe-time
	// trackers index into the seeded key range.
	b.uniformTracker = kt
	if b.cfg.ZipfS > 0 {
		b.zipfTracker = benchmarks.NewZipfKeyTracker(0, b.cfg.KeyPopulation, b.cfg.ZipfS, b.cfg.ZipfV, time.Now().UnixNano())
		b.zipfTracker.KeyTracker = kt
	}

	return nil
}

func (b *benchmark) Cleanup() error {
	for _, c := range b.clients {
		if err := c.CloseSession(context.Background()); err != nil {
			return err
		}
	}

	if b.primaryServer != nil {
		b.primaryServer.Stop()
		b.primaryServer = nil
	}

	for _, tDir := range b.tempDirs {
		os.RemoveAll(tDir)
	}
	return nil
}

// Run dispatches Workers goroutines that issue Get requests for keys
// chosen via the configured distribution (Zipf when ZipfS > 0, otherwise
// uniform random). Returns a Result describing the achieved Get rate.
func (b *benchmark) Run(duration time.Duration, seed uint64) (interface{}, error) {
	wg := sync.WaitGroup{}

	// Match writetxs's done/errChan setup so worker shutdown on time-up
	// runs through the same pattern (close(done), wg.Wait()).
	done := make(chan struct{})
	errChan := make(chan error, 1)

	b.startTime = time.Now()
	b.lastProbeTime = b.startTime

	hwStatsGatherer, err := benchmarks.NewHWStatsProber()
	if err != nil {
		log.Printf("HW stats disabled, couldn't initialize gathering object: %v", err)
	} else {
		b.hwStatsGatherer = hwStatsGatherer
	}

	for i := range b.clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			c := b.clients[i]

			for {
				select {
				case <-done:
					return
				default:
				}

				var keyStr string
				if b.zipfTracker != nil {
					keyStr = b.zipfTracker.GetRKeyZipf()
				} else {
					keyStr = b.uniformTracker.GetRKey()
				}
				key := h256(keyStr)
				if len(key) > b.cfg.KeySize {
					key = key[:b.cfg.KeySize]
				}

				_, gerr := c.Get(context.Background(), key)
				if gerr != nil {
					select {
					case errChan <- gerr:
						return
					default:
					}
				}

				atomic.AddInt64(&b.getsSoFar, 1)
			}
		}(i)
	}

	select {
	case err := <-errChan:
		close(done)
		wg.Wait()
		return nil, err
	case <-time.After(duration):
		close(done)
		wg.Wait()
	}

	return b.genResults(false), nil
}

func (b *benchmark) Probe() interface{} {
	return b.genResults(true)
}

func (b *benchmark) genResults(asProbe bool) interface{} {
	getsSoFar := atomic.LoadInt64(&b.getsSoFar)
	now := time.Now()
	d := now.Sub(b.startTime)

	res := &Result{
		GetTotal: getsSoFar,
		Gets:     float64(getsSoFar) * float64(time.Second) / float64(d),
	}

	if asProbe {
		b.m.Lock()
		defer b.m.Unlock()

		dSinceLastProbe := now.Sub(b.lastProbeTime)
		res.GetsInst = float64(getsSoFar-b.lastProbeGetsSoFar) * float64(time.Second) / float64(dSinceLastProbe)

		b.lastProbeGetsSoFar = getsSoFar
		b.lastProbeTime = now
	}

	if b.hwStatsGatherer != nil {
		hwStats, err := b.hwStatsGatherer.GetHWStats()
		if err != nil {
			log.Printf("ERROR: Couldn't gather HW stats: %v", err)
		} else {
			res.HWStats = hwStats
		}
	}

	return res
}
