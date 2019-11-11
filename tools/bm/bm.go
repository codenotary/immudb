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
	"strconv"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/db"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

func makeBadger() *badger.DB {

	dir, err := ioutil.TempDir("", "badger")
	if err != nil {
		log.Fatal(err)
	}

	opts := badger.
		DefaultOptions(dir).
		WithTableLoadingMode(options.LoadToRAM).
		WithCompressionType(options.None).
		WithSyncWrites(false).
		WithEventLogging(false)

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return db
}

const N = 1000000
const Chunk = 8

var V = []byte{0, 1, 3, 4, 5, 6, 7}

func main() {
	badger := makeBadger()
	topic := db.NewTopic(badger)

	var wg sync.WaitGroup

	chunkSize := N / Chunk
	for k := 0; k < Chunk; k++ {
		wg.Add(1)
		go func(kk int) {
			defer wg.Done()
			start := kk * chunkSize
			end := (kk + 1) * chunkSize
			for i := start; i < end; i++ {
				topic.Set(strconv.FormatUint(uint64(i), 10), V)
			}
		}(k)
	}

	startTime := time.Now()
	wg.Wait()
	endTime := time.Now()

	elapsed := endTime.Unix() - startTime.Unix()
	txnSec := float64(N) / float64(elapsed)

	fmt.Printf(
		`
Iterations:	%d
Elapsed t.:	%d (sec)
Txn/sec   :	%f
`,
		N, elapsed, txnSec)
}
