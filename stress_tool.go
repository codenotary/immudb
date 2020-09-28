/*
Copyright 2019-2020 vChain, Inc.

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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"codenotary.io/immudb-v2/store"
)

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")
	committers := flag.Int("committers", 10, "number of concurrent committers")
	parallelIO := flag.Int("parallelIO", 1, "number of parallel IO")
	txCount := flag.Int("txCount", 1_000, "number of tx to commit")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	kLen := flag.Int("kLen", 32, "key length (bytes)")
	vLen := flag.Int("vLen", 32, "value length (bytes)")
	txDelay := flag.Int("txDelay", 10, "delay (millis) between txs")
	printAfter := flag.Int("printAfter", 100, "print a dot '.' after specified number of committed txs")
	synced := flag.Bool("synced", true, "strict sync mode - no data lost")
	txLinking := flag.Bool("txLinking", true, "full scan to verify linear cryptographic linking between txs")
	kvInclusion := flag.Bool("kvInclusion", false, "validate kv data of every tx as part of the linear verification. txLinking must be enabled")

	flag.Parse()

	fmt.Println("Opening Immutable Transactional Key-Value Log...")
	immuStore, err := store.Open(*dataDir, store.DefaultOptions().SetSynced(*synced).SetIOConcurrency(*parallelIO))

	if err != nil {
		panic(err)
	}

	fmt.Printf("Immutable Transactional Key-Value Log with %d Txs successfully openned!\r\n", immuStore.TxCount())

	fmt.Printf("Committing %d transactions...\r\n", *txCount)

	wgInit := &sync.WaitGroup{}
	wgInit.Add(*committers)

	wgWork := &sync.WaitGroup{}
	wgWork.Add(*committers)

	wgStart := &sync.WaitGroup{}
	wgStart.Add(1)

	for c := 0; c < *committers; c++ {
		go func(id int) {
			kvs := make([]*store.KV, *kvCount)

			rand.Seed(time.Now().UnixNano())

			for i := 0; i < *kvCount; i++ {
				k := make([]byte, *kLen)
				v := make([]byte, *vLen)

				rand.Read(k)
				rand.Read(v)

				kvs[i] = &store.KV{Key: k, Value: v}
			}

			fmt.Printf("\r\nCommitter %d is running...\r\n", id)

			wgInit.Done()

			wgStart.Wait()

			for t := 0; t < *txCount; t++ {
				_, _, _, _, err := immuStore.Commit(kvs)
				if err != nil {
					panic(err)
				}

				if *printAfter > 0 && t%*printAfter == 0 {
					fmt.Print(".")
				}

				time.Sleep(time.Duration(*txDelay) * time.Millisecond)
			}

			wgWork.Done()
			fmt.Printf("\r\nCommitter %d done!\r\n", id)
		}(c)
	}

	wgInit.Wait()

	wgStart.Done()

	start := time.Now()
	wgWork.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\r\nAll committers %d have successfully completed their work within %s!\r\n", *committers, elapsed)

	if *txLinking {
		fmt.Println("Starting full scan to verify linear cryptographic linking...")
		start := time.Now()

		txReader, err := immuStore.NewTxReader(0, 4096)
		if err != nil {
			panic(err)
		}

		verifiedTxs := 0

		b := make([]byte, immuStore.MaxValueLen())

		for {
			tx, err := txReader.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}

			txEntries := tx.Entries()

			if *kvInclusion {
				for i := 0; i < len(txEntries); i++ {
					path := tx.Proof(i)

					_, err = immuStore.ReadValueAt(b[:txEntries[i].ValueLen], txEntries[i].VOff)
					if err != nil {
						panic(err)
					}

					kv := &store.KV{Key: txEntries[i].Key(), Value: b[:txEntries[i].ValueLen]}

					verifies := path.VerifyInclusion(uint64(len(txEntries)-1), uint64(i), tx.Eh, kv.Digest())
					if !verifies {
						panic("kv does not verify")
					}
				}
			}

			verifiedTxs++

			if *printAfter > 0 && verifiedTxs%*printAfter == 0 {
				fmt.Print(".")
			}
		}

		elapsed := time.Since(start)
		fmt.Printf("\r\nAll transactions %d successfully verified in %s!\r\n", verifiedTxs, elapsed)
	}
}
