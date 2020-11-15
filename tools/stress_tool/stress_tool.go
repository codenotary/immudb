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
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
)

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")

	committers := flag.Int("committers", 10, "number of concurrent committers")
	txCount := flag.Int("txCount", 1_000, "number of tx to commit")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	kLen := flag.Int("kLen", 32, "key length (bytes)")
	vLen := flag.Int("vLen", 32, "value length (bytes)")
	rndKeys := flag.Bool("rndKeys", false, "keys are randomly generated")
	rndValues := flag.Bool("rndValues", true, "values are randomly generated")
	txDelay := flag.Int("txDelay", 10, "delay (millis) between txs")
	printAfter := flag.Int("printAfter", 100, "print a dot '.' after specified number of committed txs")

	flag.Parse()

	fmt.Println("Opening immudb...")

	slog := logger.NewSimpleLoggerWithLevel("stree_tool(immudb)", os.Stderr, logger.LogError)

	opts, badgerOpts := store.DefaultOptions(*dataDir, slog)
	badgerOpts.ValueDir = *dataDir
	badgerOpts.NumVersionsToKeep = math.MaxInt64

	store, err := store.Open(opts, badgerOpts)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := store.Close()
		if err != nil {
			fmt.Printf("\r\nimmudb closed with error: %v\r\n", err)
			panic(err)
		}
		fmt.Printf("\r\nimmudb successfully closed!\r\n")
	}()

	fmt.Printf("immudb successfully opened! (countAll: %d)\r\n", store.CountAll())

	fmt.Printf("Committing %d transactions...\r\n", *txCount)

	wgInit := &sync.WaitGroup{}
	wgInit.Add(*committers)

	wgWork := &sync.WaitGroup{}
	wgWork.Add(*committers)

	wgEnded := &sync.WaitGroup{}
	wgEnded.Add(*committers)

	wgStart := &sync.WaitGroup{}
	wgStart.Add(1)

	root, err := store.CurrentRoot()
	if err != nil {
		panic(err)
	}

	var lastKey []byte

	for c := 0; c < *committers; c++ {
		go func(id int) {
			fmt.Printf("\r\nCommitter %d is generating kv data...\r\n", id)

			txs := make([]schema.KVList, *txCount)

			for t := 0; t < *txCount; t++ {
				txs[t] = schema.KVList{KVs: make([]*schema.KeyValue, *kvCount)}

				rand.Seed(time.Now().UnixNano())

				for i := 0; i < *kvCount; i++ {
					k := make([]byte, *kLen)
					v := make([]byte, *vLen)

					if *rndKeys {
						rand.Read(k)
					} else {
						if *kLen < 2 {
							k[0] = byte(i)
						}

						if *kLen > 1 && *kLen < 4 {
							binary.BigEndian.PutUint16(k, uint16(i))
						}
						if *kLen > 3 && *kLen < 8 {
							binary.BigEndian.PutUint32(k, uint32(i))
						}
						if *kLen > 7 {
							binary.BigEndian.PutUint64(k, uint64(i))
						}
					}

					if *rndValues {
						rand.Read(v)
					}

					k[0] = k[0] | 1
					txs[t].KVs[i] = &schema.KeyValue{Key: k, Value: v}

					lastKey = k
				}
			}

			wgInit.Done()

			wgStart.Wait()

			fmt.Printf("\r\nCommitter %d is running...\r\n", id)

			ids := make([]*schema.Index, *txCount)

			for t := 0; t < *txCount; t++ {
				txid, err := store.SetBatch(txs[t])
				if err != nil {
					panic(err)
				}

				ids[t] = txid

				if *printAfter > 0 && t%*printAfter == 0 {
					fmt.Print(".")
				}

				time.Sleep(time.Duration(*txDelay) * time.Millisecond)
			}

			wgWork.Done()
			fmt.Printf("\r\nCommitter %d done with commits!\r\n", id)

			wgEnded.Done()

			fmt.Printf("Committer %d sucessfully ended!\r\n", id)
		}(c)
	}

	wgInit.Wait()

	wgStart.Done()

	start := time.Now()
	wgWork.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\r\nAll committers %d have successfully completed their work within %s!\r\n", *committers, elapsed)

	wgEnded.Wait()

	fmt.Printf("Waiting SafeGet of last inserted key...\r\n")
	start = time.Now()
	_, err = store.SafeGet(schema.SafeGetOptions{Key: lastKey, RootIndex: &schema.Index{Index: root.Payload.Index}})
	if err != nil {
		panic(err)
	}
	elapsed = time.Since(start)
	fmt.Printf("\r\nSafeGet of last inserted key completed within %s!\r\n", elapsed)
}
