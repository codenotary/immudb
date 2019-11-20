/*
Copyright 2019 vChain, Inc.

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

package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/db"
)

func makeTopic() (*db.Topic, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}

	opts := db.DefaultOptions(dir)
	opts.Badger = opts.Badger.
		WithSyncWrites(false).
		WithEventLogging(false)

	topic, err := db.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return topic, func() {
		if err := topic.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}
}

const N = 1000000

var Concurrency = runtime.NumCPU()

var V = []byte{0, 1, 3, 4, 5, 6, 7}

func main() {
	runtime.GOMAXPROCS(128)

	topic, closer := makeTopic()
	defer closer()

	var wg sync.WaitGroup

	chunkSize := N / Concurrency
	for k := 0; k < Concurrency; k++ {
		wg.Add(1)
		go func(kk int) {
			defer wg.Done()
			start := kk * chunkSize
			end := (kk + 1) * chunkSize
			var kvPairs []db.KVPair
			for i := start; i < end; i++ {
				kvPairs = append(kvPairs, db.KVPair{
					Key:   []byte(strconv.FormatUint(uint64(i), 10)),
					Value: V,
				})
			}
			_ = topic.SetBatch(kvPairs)
		}(k)
	}

	startTime := time.Now()
	wg.Wait()
	endTime := time.Now()

	elapsed := float64(endTime.UnixNano()-startTime.UnixNano()) / (1000 * 1000 * 1000)
	txnSec := float64(N) / elapsed

	fmt.Printf(
		`
Concurency:	%d
Iterations:	%d
Elapsed t.:	%.2f sec
Throughput:	%.0f tx/sec

`,
		Concurrency, N, elapsed, txnSec)
}
