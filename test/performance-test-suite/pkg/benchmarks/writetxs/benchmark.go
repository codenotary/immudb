/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks"
)

func h256(s string) []byte {
	h := sha256.New()
	h.Write([]byte(s))
	return h.Sum(nil)
}

type Config struct {
	Workers    int
	BatchSize  int
	KeySize    int
	ValueSize  int
	AsyncWrite bool
	Seed       int
}

var DefaultConfig = Config{
	Workers:    30,
	BatchSize:  1,
	KeySize:    32,
	ValueSize:  128,
	AsyncWrite: true,
	Seed:       0,
}

type benchmark struct {
	cfg Config

	txSoFar   int64
	kvSoFar   int64
	startTime time.Time

	lastProbeTxSoFar int64
	lastProbeKVSoFar int64
	lastProbeTime    time.Time

	m sync.Mutex

	server  *servertest.BufconnServer
	clients []client.ImmuClient
}

type Result struct {
	TxTotal int64   `json:"txTotal"`
	KvTotal int64   `json:"kvTotal"`
	Txs     float64 `json:"txs"`
	Kvs     float64 `json:"kvs"`
	TxsInst float64 `json:"txsInstant,omitempty"`
	KvsInst float64 `json:"kvsInstant,omitempty"`
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
	return s
}

func NewBenchmark(cfg Config) benchmarks.Benchmark {
	return &benchmark{cfg: cfg}
}

func (b *benchmark) Name() string {
	return "Write TX/s"
}

func (b *benchmark) Warmup() error {
	options := server.
		DefaultOptions().
		WithDir("tx-test")

	b.server = servertest.NewBufconnServer(options)
	b.server.Server.Srv.WithLogger(logger.NewMemoryLoggerWithLevel(logger.LogDebug))
	err := b.server.Start()
	if err != nil {
		return err
	}

	b.clients = []client.ImmuClient{}
	for i := 0; i < b.cfg.Workers; i++ {
		c, err := b.server.NewAuthenticatedClient(client.DefaultOptions())
		if err != nil {
			return err
		}
		b.clients = append(b.clients, c)
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

	return b.server.Stop()
}

func (b *benchmark) Run(duration time.Duration) (interface{}, error) {
	wg := sync.WaitGroup{}

	kt := keyTracker{
		start: b.cfg.Seed,
	}

	rand := NewRandStringGen(b.cfg.ValueSize)
	var done chan bool
	var errChan chan error

	b.startTime = time.Now()
	b.lastProbeTime = b.startTime

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
					key := h256(kt.getWKey())
					if len(key) > b.cfg.KeySize {
						key = key[:b.cfg.KeySize]
					}

					setRequest.KVs[i] = &schema.KeyValue{
						Key:   key,
						Value: rand.getRnd(),
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

	return res
}
