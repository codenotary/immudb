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
	"time"

	"codenotary.io/immudb-v2/store"
)

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")
	txCount := flag.Int("txCount", 1_000, "number of tx to commit")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	txDelay := flag.Int("txDelay", 10, "delay (millis) between txs")
	printAfter := flag.Int("printAfter", 100, "print a dot '.' after specified number of committed txs")

	flag.Parse()

	fmt.Println("Openning immutable transactional log...")
	immuStore, err := store.Open(*dataDir, store.DefaultOptions())

	if err != nil {
		panic(err)
	}

	fmt.Println("Immutable transactional log openned!")

	fmt.Println("Committing transactions...")

	for t := 0; t < *txCount; t++ {
		kvs := make([]*store.KV, *kvCount)

		for i := 0; i < *kvCount; i++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(t*(*kvCount)+i))

			v := make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(t*(*kvCount)+i))

			kvs[i] = &store.KV{Key: k, Value: v}
		}

		_, _, _, _, err := immuStore.Commit(kvs)
		if err != nil {
			panic(err)
		}

		if t%*printAfter == 0 {
			fmt.Print(".")
		}

		time.Sleep(time.Duration(*txDelay) * time.Millisecond)
	}

	fmt.Println()
	fmt.Println("All transactions successfully committed!")
}
