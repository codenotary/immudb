/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package main

import (
	"context"
	"flag"
	"log"
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

type cfg struct {
	dataDir           string
	parallelIO        int
	fileSize          int
	compressionFormat int
	compressionLevel  int
	synced            bool
	openedLogFiles    int
	committers        int
	kvCount           int
	vLen              int
	rndValues         bool
	readers           int
	rdCount           int
	readDelay         int
	readPause         int
}

func parseConfig() (c cfg) {
	flag.StringVar(&c.dataDir, "dataDir", "data", "data directory")

	flag.IntVar(&c.parallelIO, "parallelIO", 1, "number of parallel IO")
	flag.IntVar(&c.fileSize, "fileSize", 1<<26, "file size up to which a new ones are created")
	cFormat := flag.String("compressionFormat", "no-compression", "one of: no-compression, flate, gzip, lzw, zlib")
	cLevel := flag.String("compressionLevel", "best-speed", "one of: best-speed, best-compression, default-compression, huffman-only")

	flag.BoolVar(&c.synced, "synced", false, "strict sync mode - no data lost")
	flag.IntVar(&c.openedLogFiles, "openedLogFiles", 10, "number of maximum number of opened files per each log type")

	flag.IntVar(&c.committers, "committers", 10, "number of concurrent committers")
	flag.IntVar(&c.kvCount, "kvCount", 1_000, "number of kv entries per tx")
	flag.IntVar(&c.vLen, "vLen", 32, "value length (bytes)")
	flag.BoolVar(&c.rndValues, "rndValues", true, "values are randomly generated")

	flag.IntVar(&c.readers, "readers", 0, "number of concurrent readers")
	flag.IntVar(&c.rdCount, "rdCount", 100, "number of reads for each readers")
	flag.IntVar(&c.readDelay, "readDelay", 100, "Readers start delay (ms)")
	flag.IntVar(&c.readPause, "readPause", 0, "Readers pause at every cycle")

	flag.Parse()

	switch *cFormat {
	case "no-compression":
		c.compressionFormat = appendable.NoCompression
	case "flate":
		c.compressionFormat = appendable.FlateCompression
	case "gzip":
		c.compressionFormat = appendable.GZipCompression
	case "lzw":
		c.compressionFormat = appendable.LZWCompression
	case "zlib":
		c.compressionFormat = appendable.ZLibCompression
	default:
		panic("invalid compression format")
	}

	switch *cLevel {
	case "best-speed":
		c.compressionLevel = appendable.BestSpeed
	case "best-compression":
		c.compressionLevel = appendable.BestCompression
	case "default-compression":
		c.compressionLevel = appendable.DefaultCompression
	case "huffman-only":
		c.compressionLevel = appendable.HuffmanOnly
	default:
		panic("invalid compression level")
	}

	return
}

func main() {
	c := parseConfig()

	log.Println("Opening Immutable Transactional Key-Value Log...")

	opts := store.DefaultOptions().
		WithSynced(c.synced).
		WithMaxConcurrency(c.committers).
		WithMaxIOConcurrency(c.parallelIO).
		WithFileSize(c.fileSize).
		WithVLogMaxOpenedFiles(c.openedLogFiles).
		WithTxLogMaxOpenedFiles(c.openedLogFiles).
		WithCommitLogMaxOpenedFiles(c.openedLogFiles).
		WithCompressionFormat(c.compressionFormat).
		WithCompresionLevel(c.compressionLevel).
		WithMaxValueLen(1 << 26) // 64Mb

	dataStore, err := store.Open(c.dataDir, opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		for name, store := range map[string]*store.ImmuStore{"data": dataStore} {
			store.Close()
			if err != nil {
				log.Printf("\r\nBacking store %s closed with error: %v\r\n", name, err)
				panic(err)
			}
			log.Printf("\r\nImmutable Transactional Key-Value Log %s successfully closed!\r\n", name)
		}
	}()

	for name, store := range map[string]*store.ImmuStore{"data": dataStore} {
		log.Printf("Store %s with %d Txs successfully opened!\r\n", name, store.TxCount())
	}

	engine, err := sql.NewEngine(dataStore, sql.DefaultOptions().WithPrefix([]byte("sql")))
	if err != nil {
		panic(err)
	}
	log.Printf("SQL engine successfully initialized!\r\n")

	_, _, err = engine.Exec(context.Background(), nil, "CREATE DATABASE defaultdb;", map[string]interface{}{})
	if err != nil {
		panic(err)
	}

	_, _, err = engine.Exec(context.Background(), nil, "USE DATABASE defaultdb;", map[string]interface{}{})
	if err != nil {
		panic(err)
	}

	log.Printf("Creating tables\r\n")
	_, _, err = engine.Exec(context.Background(), nil, "CREATE TABLE IF NOT EXISTS entries (id INTEGER, value BLOB, ts INTEGER, PRIMARY KEY id);", map[string]interface{}{})
	if err != nil {
		panic(err)
	}

	// incremental id generator
	ids := make(chan int)
	go func() {
		for i := 1; ; i++ {
			ids <- i
		}
	}()

	entries := make(chan Entry)

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < c.committers; i++ {
		go func(id int) {
			log.Printf("Worker %d is generating rows...\r\n", id)

			for i := 0; i < c.kvCount; i++ {
				id := <-ids
				v := make([]byte, c.vLen)
				if c.rndValues {
					rand.Read(v)
				}

				entries <- Entry{id: id, value: v}
			}
		}(i)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < c.committers; i++ {
		wg.Add(1)
		go func(id int) {
			log.Printf("Committer %d is inserting data...\r\n", id)
			for i := 0; i < c.kvCount; i++ {
				entry := <-entries
				_, _, err = engine.Exec(context.Background(), nil,
					"INSERT INTO entries (id, value, ts) VALUES (@id, @value, now());",
					map[string]interface{}{"id": entry.id, "value": entry.value})
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
			log.Printf("Committer %d done...\r\n", id)
		}(i)
	}
	for i := 0; i < c.readers; i++ {
		wg.Add(1)
		go func(id int) {
			if c.readDelay > 0 { // give time to populate db
				time.Sleep(time.Duration(c.readDelay) * time.Millisecond)
			}
			log.Printf("Reader %d is reading data\n", id)
			for i := 1; i <= c.rdCount; i++ {
				r, err := engine.Query(context.Background(), nil, "SELECT count() FROM entries where id<=@i;", map[string]interface{}{"i": i})
				if err != nil {
					log.Printf("Error querying val %d: %s", i, err.Error())
					panic(err)
				}
				ret, err := r.Read(context.Background())
				if err != nil {
					log.Printf("Error reading val %d: %s", i, err.Error())
					panic(err)
				}
				r.Close()
				n := ret.ValuesBySelector["(defaultdb.entries.col0)"].RawValue().(uint64)
				if n != uint64(i) {
					log.Printf("Reader %d read %d vs %d", id, n, i)
				}
				if c.readPause > 0 {
					time.Sleep(time.Duration(c.readPause) * time.Millisecond)
				}
			}
			wg.Done()
			log.Printf("Reader %d out\n", id)
		}(i)
	}
	wg.Wait()
	log.Printf("All committers done...\r\n")

	r, err := engine.Query(context.Background(), nil, "SELECT count() FROM  entries;", map[string]interface{}{})
	if err != nil {
		panic(err)
	}
	row, err := r.Read(context.Background())
	if err != nil {
		panic(err)
	}

	count := row.ValuesBySelector["(defaultdb.entries.col0)"].RawValue().(uint64)
	log.Printf("- Counted %d entries\n", count)
	defer func() {
		err := r.Close()
		if err != nil {
			panic("reader closed with error")
		}
	}()
}
