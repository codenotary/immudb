/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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
	"math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
)

type Entry struct {
	id    int
	value []byte
}

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")
	catalogDir := flag.String("catalogDir", "catalog", "catalog directory")

	parallelIO := flag.Int("parallelIO", 1, "number of parallel IO")
	fileSize := flag.Int("fileSize", 1<<26, "file size up to which a new ones are created")
	cFormat := flag.String("compressionFormat", "no-compression", "one of: no-compression, flate, gzip, lzw, zlib")
	cLevel := flag.String("compressionLevel", "best-speed", "one of: best-speed, best-compression, default-compression, huffman-only")

	synced := flag.Bool("synced", false, "strict sync mode - no data lost")
	openedLogFiles := flag.Int("openedLogFiles", 10, "number of maximun number of opened files per each log type")

	committers := flag.Int("committers", 10, "number of concurrent committers")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	vLen := flag.Int("vLen", 32, "value length (bytes)")
	rndValues := flag.Bool("rndValues", true, "values are randomly generated")

	flag.Parse()

	fmt.Println("Opening Immutable Transactional Key-Value Log...")

	var compressionFormat int
	var compressionLevel int

	switch *cFormat {
	case "no-compression":
		compressionFormat = appendable.NoCompression
	case "flate":
		compressionFormat = appendable.FlateCompression
	case "gzip":
		compressionFormat = appendable.GZipCompression
	case "lzw":
		compressionFormat = appendable.LZWCompression
	case "zlib":
		compressionFormat = appendable.ZLibCompression
	default:
		panic("invalid compression format")
	}

	switch *cLevel {
	case "best-speed":
		compressionLevel = appendable.BestSpeed
	case "best-compression":
		compressionLevel = appendable.BestCompression
	case "default-compression":
		compressionLevel = appendable.DefaultCompression
	case "huffman-only":
		compressionLevel = appendable.HuffmanOnly
	default:
		panic("invalid compression level")
	}

	opts := store.DefaultOptions().
		WithSynced(*synced).
		WithMaxConcurrency(*committers).
		WithMaxIOConcurrency(*parallelIO).
		WithFileSize(*fileSize).
		WithVLogMaxOpenedFiles(*openedLogFiles).
		WithTxLogMaxOpenedFiles(*openedLogFiles).
		WithCommitLogMaxOpenedFiles(*openedLogFiles).
		WithCompressionFormat(compressionFormat).
		WithCompresionLevel(compressionLevel).
		WithMaxLinearProofLen(0).
		WithMaxValueLen(1 << 26) // 64Mb

	catalogStore, err := store.Open(*catalogDir, opts)
	if err != nil {
		panic(err)
	}

	dataStore, err := store.Open(*dataDir, opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		for name, store := range map[string]*store.ImmuStore{"catalog": catalogStore, "data": dataStore} {
			store.Close()
			if err != nil {
				fmt.Printf("\r\nBacking store %s closed with error: %v\r\n", name, err)
				panic(err)
			}
			fmt.Printf("\r\nImmutable Transactional Key-Value Log %s successfully closed!\r\n", name)
		}
	}()

	for name, store := range map[string]*store.ImmuStore{"catalog": catalogStore, "data": dataStore} {
		fmt.Printf("Store %s with %d Txs successfully opened!\r\n", name, store.TxCount())
	}

	engine, err := sql.NewEngine(catalogStore, dataStore, []byte("sql"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("SQL engine successfully opened!\r\n")

	defer func() {
		err := engine.Close()
		if err != nil {
			fmt.Printf("\r\nSQL engine closed with error: %v\r\n", err)
			panic(err)
		}
	}()

	_, _, err = engine.ExecStmt("CREATE DATABASE defaultdb;", map[string]interface{}{}, true)
	if err != nil {
		panic(err)
	}

	err = engine.UseDatabase("defaultdb")
	if err != nil {
		panic(err)
	}

	fmt.Printf("Creating tables\r\n")
	_, _, err = engine.ExecStmt("CREATE TABLE IF NOT EXISTS entries (id INTEGER, value BLOB, ts INTEGER, PRIMARY KEY id);", map[string]interface{}{}, true)
	if err != nil {
		panic(err)
	}

	// incremental id generator
	ids := make(chan int)
	go func() {
		i := 1
		for {
			ids <- i
			i = i + 1
		}
	}()

	entries := make(chan Entry)

	rand.Seed(time.Now().UnixNano())
	for c := 0; c < *committers; c++ {
		go func(id int) {
			fmt.Printf("\r\nWorker %d is generating rows...\r\n", id)

			for i := 0; i < *kvCount; i++ {
				id := <-ids
				v := make([]byte, *vLen)
				if *rndValues {
					rand.Read(v)
				}

				entries <- Entry{id: id, value: v}
			}
		}(c)
	}

	wg := sync.WaitGroup{}

	for c := 0; c < *committers; c++ {
		wg.Add(1)
		go func(id int) {
			fmt.Printf("\r\nCommitter %d is inserting data...\r\n", id)
			for i := 0; i < *kvCount; i++ {
				entry := <-entries
				_, _, err = engine.ExecStmt("INSERT INTO entries (id, value, ts) VALUES (@id, @value, now());",
					map[string]interface{}{"id": entry.id, "value": entry.value}, true)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
			fmt.Printf("\r\nCommitter %d done...\r\n", id)
		}(c)
	}

	wg.Wait()
	fmt.Printf("\r\nAll committers done...\r\n")

	r, err := engine.QueryStmt("SELECT count() FROM  entries;", map[string]interface{}{}, true)
	if err != nil {
		panic(err)
	}
	row, err := r.Read()
	if err != nil {
		panic(err)
	}

	count := row.Values["(defaultdb.entries.col0)"].Value().(uint64)
	fmt.Printf("- Counted %d entries\n", count)
	defer func() {
		err := r.Close()
		if err != nil {
			panic("reader closed with error")
		}
	}()
}
