/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package writetxs

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

type Config struct {
	Name       string
	Workers    int
	BatchSize  int
	KeySize    int
	ValueSize  int
	AsyncWrite bool
	Replica    string
}

type benchmark struct {
	cfg Config

	txSoFar   int64
	kvSoFar   int64
	startTime time.Time

	lastProbeTxSoFar int64
	lastProbeKVSoFar int64
	lastProbeTime    time.Time

	hwStatsGatherer *benchmarks.HWStatsProber

	m sync.Mutex

	primaryServer *server.ImmuServer
	replicaServer *server.ImmuServer
	clients       []client.ImmuClient
	tempDirs      []string
}

type Result struct {
	TxTotal int64               `json:"txTotal"`
	KvTotal int64               `json:"kvTotal"`
	Txs     float64             `json:"txs"`
	Kvs     float64             `json:"kvs"`
	TxsInst float64             `json:"txsInstant,omitempty"`
	KvsInst float64             `json:"kvsInstant,omitempty"`
	HWStats *benchmarks.HWStats `json:"hwStats"`
}

func (r *Result) String() string {
	s := fmt.Sprintf(
		"TX: %d, KV: %d, TX/s: %.2f, KV/S: %.2f",
		r.TxTotal,
		r.KvTotal,
		r.Txs,
		r.Kvs,
	)
	if r.TxsInst != 0.0 || r.KvsInst != 0.0 {
		s += fmt.Sprintf(
			", TX/s instant: %.2f, KV/s instant: %.2f",
			r.TxsInst,
			r.KvsInst,
		)
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

func (b *benchmark) Warmup(tempDirBase string) error {
	primaryPath, err := os.MkdirTemp(tempDirBase, "tx-test-primary")
	if err != nil {
		return err
	}
	b.tempDirs=append(b.tempDirs,primaryPath)

	primaryServerOpts := server.
		DefaultOptions().
		WithDir(primaryPath).
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithLogFormat(logger.LogFormatJSON).
		WithLogfile("./immudb.log")

	primaryServerReplicaOptions := server.ReplicationOptions{}

	if b.cfg.Replica == "async" {
		primaryServerOpts.WithReplicationOptions(primaryServerReplicaOptions.WithIsReplica(false))
	}

	if b.cfg.Replica == "sync" {
		primaryServerOpts.WithReplicationOptions(primaryServerReplicaOptions.WithIsReplica(false).WithSyncReplication(true).WithSyncAcks(1))
	}

	b.primaryServer = server.DefaultServer().WithOptions(primaryServerOpts).(*server.ImmuServer)

	err = b.primaryServer.Initialize()
	if err != nil {
		return err
	}

	go func() {
		b.primaryServer.Start()
	}()

	time.Sleep(1 * time.Second)

	primaryPort := b.primaryServer.Listener.Addr().(*net.TCPAddr).Port

	if b.cfg.Replica == "async" || b.cfg.Replica == "sync" {
		replicaPath, err := os.MkdirTemp(tempDirBase, fmt.Sprintf("%s-tx-test-replica", b.cfg.Replica))
		if err != nil {
			return err
		}

		b.tempDirs=append(b.tempDirs,replicaPath)

		replicaServerOptions := server.
			DefaultOptions().
			WithDir(replicaPath).
			WithPort(0).
			WithLogFormat(logger.LogFormatJSON).
			WithLogfile("./replica.log")

		replicaServerOptions.PgsqlServer = false
		replicaServerOptions.MetricsServer = false
		replicaServerOptions.WebServer = false

		replicaServerReplicaOptions := server.ReplicationOptions{}

		replicaServerReplicaOptions.PrimaryHost = "127.0.0.1"
		replicaServerReplicaOptions.PrimaryPort = primaryPort
		replicaServerReplicaOptions.PrimaryUsername = "immudb"
		replicaServerReplicaOptions.PrimaryPassword = "immudb"
		replicaServerReplicaOptions.PrefetchTxBufferSize = 1000
		replicaServerReplicaOptions.ReplicationCommitConcurrency = 30

		if b.cfg.Replica == "async" {
			replicaServerOptions.WithReplicationOptions(replicaServerReplicaOptions.WithIsReplica(true))
		}

		if b.cfg.Replica == "sync" {
			replicaServerReplicaOptions = *replicaServerReplicaOptions.WithIsReplica(true).WithSyncReplication(true)

			replicaServerOptions.WithReplicationOptions(&replicaServerReplicaOptions)
		}

		b.replicaServer = server.DefaultServer().WithOptions(replicaServerOptions).(*server.ImmuServer)

		err = b.replicaServer.Initialize()
		if err != nil {
			return err
		}

		go func() {
			b.replicaServer.Start()
		}()

		time.Sleep(1 * time.Second)
	}

	b.clients = []client.ImmuClient{}
	for i := 0; i < b.cfg.Workers; i++ {
		path, err := os.MkdirTemp(tempDirBase, "immudb_client")
		if err != nil {
			return err
		}
		c := client.NewClient().WithOptions(client.DefaultOptions().WithPort(primaryPort).WithDir(path))

		err = c.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
		if err != nil {
			return err
		}

		b.clients = append(b.clients, c)
		b.tempDirs = append(b.tempDirs, path)
	}

	return nil
}

func (b *benchmark) Cleanup() error {

	for _, c := range b.clients {
		err := c.CloseSession(context.Background())
		if err != nil {
			return err
		}
	}

	b.primaryServer.Stop()
	b.primaryServer = nil

	if b.replicaServer != nil {
		b.replicaServer.Stop()
		b.replicaServer = nil
	}
	for _,tDir := range(b.tempDirs) {
		os.RemoveAll(tDir)
	}
	return nil
}

func (b *benchmark) Run(duration time.Duration, seed uint64) (interface{}, error) {
	wg := sync.WaitGroup{}

	kt := benchmarks.NewKeyTracker(seed)
	rand := benchmarks.NewRandStringGen(b.cfg.ValueSize)
	defer rand.Stop()

	var done chan bool
	var errChan chan error

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

			client := b.clients[i]

			for {

				select {
				case <-done:
					return
				default:
				}

				setRequest := schema.SetRequest{
					KVs:    make([]*schema.KeyValue, b.cfg.BatchSize),
					NoWait: b.cfg.AsyncWrite,
				}

				for i := 0; i < b.cfg.BatchSize; i++ {
					key := h256(kt.GetWKey())
					if len(key) > b.cfg.KeySize {
						key = key[:b.cfg.KeySize]
					}

					setRequest.KVs[i] = &schema.KeyValue{
						Key:   key,
						Value: rand.GetRnd(),
					}
				}

				_, err := client.SetAll(context.Background(), &setRequest)

				if err != nil {
					select {
					case errChan <- err:
						return
					default:
					}
				}

				atomic.AddInt64(&b.txSoFar, 1)
				atomic.AddInt64(&b.kvSoFar, int64(len(setRequest.KVs)))
			}

		}(i)
	}

	select {
	case err := <-errChan:
		// Finish with error
		close(done)
		wg.Wait()
		return nil, err

	case <-time.After(duration):
		// Finish after given duration
	}

	return b.genResults(false), nil
}

func (b *benchmark) Probe() interface{} {
	return b.genResults(true)
}

func (b *benchmark) genResults(asProbe bool) interface{} {

	txSoFar := atomic.LoadInt64(&b.txSoFar)
	kvSoFar := atomic.LoadInt64(&b.kvSoFar)

	now := time.Now()

	d := now.Sub(b.startTime)

	res := &Result{
		TxTotal: txSoFar,
		KvTotal: kvSoFar,
		Txs:     float64(txSoFar) * float64(time.Second) / float64(d),
		Kvs:     float64(kvSoFar) * float64(time.Second) / float64(d),
	}

	if asProbe {

		b.m.Lock()
		defer b.m.Unlock()

		dSinceLastProbe := now.Sub(b.lastProbeTime)

		res.TxsInst = float64(txSoFar-b.lastProbeTxSoFar) * float64(time.Second) / float64(dSinceLastProbe)
		res.KvsInst = float64(kvSoFar-b.lastProbeKVSoFar) * float64(time.Second) / float64(dSinceLastProbe)

		b.lastProbeTxSoFar = txSoFar
		b.lastProbeKVSoFar = kvSoFar
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
