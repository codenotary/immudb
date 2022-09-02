/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/store"
)

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")

	parallelIO := flag.Int("parallelIO", 1, "number of parallel IO")
	fileSize := flag.Int("fileSize", 1<<29, "file size up to which a new ones are created")
	cFormat := flag.String("compressionFormat", "no-compression", "one of: no-compression, flate, gzip, lzw, zlib")
	cLevel := flag.String("compressionLevel", "best-speed", "one of: best-speed, best-compression, default-compression, huffman-only")

	synced := flag.Bool("synced", false, "strict sync mode - no data lost")
	syncFrequency := flag.Duration("syncFrequency", time.Duration(20)*time.Millisecond, "syncFrequency")

	openedLogFiles := flag.Int("openedLogFiles", 10, "number of maximun number of opened files per each log type")

	waitForIndexing := flag.Bool("waitForIndexing", false, "wait for indexing to be updated")

	indexable := flag.Bool("indexable", true, "entries are indexed")

	committers := flag.Int("committers", 100, "number of concurrent committers")
	txCount := flag.Int("txCount", 1_000, "number of tx to commit")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	kLen := flag.Int("kLen", 8, "key length (bytes)")
	vLen := flag.Int("vLen", 256, "value length (bytes)")
	rndKeys := flag.Bool("rndKeys", false, "keys are randomly generated")
	rndValues := flag.Bool("rndValues", false, "values are randomly generated")
	txDelay := flag.Int("txDelay", 0, "delay (millis) between txs")
	printAfter := flag.Int("printAfter", 1_000, "print a dot '.' after specified number of committed txs")
	txRead := flag.Bool("txRead", false, "validate committed txs against input kv data")
	txLinking := flag.Bool("txLinking", false, "full scan to verify linear cryptographic linking between txs")
	kvInclusion := flag.Bool("kvInclusion", false, "validate kv data of every tx as part of the linear verification. txLinking must be enabled")

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
		WithSyncFrequency(*syncFrequency)

	immuStore, err := store.Open(*dataDir, opts)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := immuStore.Close()
		if err != nil {
			fmt.Printf("\r\nImmutable Transactional Key-Value Log closed with error: %v\r\n", err)
			panic(err)
		}
		fmt.Printf("\r\nImmutable Transactional Key-Value Log successfully closed!\r\n")
	}()

	fmt.Printf("Immutable Transactional Key-Value Log with %d Txs successfully opened!\r\n", immuStore.TxCount())

	txHolderPool, err := immuStore.NewTxHolderPool(*committers, false)
	if err != nil {
		panic(fmt.Sprintf("Couldn't allocate tx holder pool: %v", err))
	}

	fmt.Printf("Committing %d transactions...\r\n", *txCount)

	wgInit := &sync.WaitGroup{}
	wgInit.Add(*committers)

	wgWork := &sync.WaitGroup{}
	wgWork.Add(*committers)

	wgEnded := &sync.WaitGroup{}
	wgEnded.Add(*committers)

	wgStart := &sync.WaitGroup{}
	wgStart.Add(1)

	for c := 0; c < *committers; c++ {
		go func(id int) {
			fmt.Printf("\r\nCommitter %d is generating kv data...\r\n", id)

			entries := make([][]*store.EntrySpec, *txCount)

			for t := 0; t < *txCount; t++ {
				entries[t] = make([]*store.EntrySpec, *kvCount)

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

					md := store.NewKVMetadata()
					err = md.AsNonIndexable(!*indexable)
					if err != nil {
						panic(err)
					}

					entries[t][i] = &store.EntrySpec{Key: k, Metadata: md, Value: v}
				}
			}

			wgInit.Done()

			wgStart.Wait()

			fmt.Printf("\r\nCommitter %d is running...\r\n", id)

			ids := make([]uint64, *txCount)

			for t := 0; t < *txCount; t++ {
				tx, err := immuStore.NewWriteOnlyTx()
				if err != nil {
					panic(err)
				}

				for _, e := range entries[t] {
					err = tx.Set(e.Key, e.Metadata, e.Value)
					if err != nil {
						panic(err)
					}
				}

				var txhdr *store.TxHeader

				if *waitForIndexing {
					txhdr, err = tx.Commit()
				} else {
					txhdr, err = tx.AsyncCommit()
				}
				if err != nil {
					panic(err)
				}

				ids[t] = txhdr.ID

				if *printAfter > 0 && (t+1)%*printAfter == 0 {
					fmt.Print(".")
				}

				time.Sleep(time.Duration(*txDelay) * time.Millisecond)
			}

			wgWork.Done()
			fmt.Printf("\r\nCommitter %d done with commits!\r\n", id)

			if *txRead {
				fmt.Printf("Starting committed tx against input kv data by committer %d...\r\n", id)

				txHolder, err := txHolderPool.Alloc()
				if err != nil {
					panic(err)
				}
				defer txHolderPool.Release(txHolder)

				for i := range ids {
					immuStore.ReadTx(ids[i], txHolder)

					for ei, e := range txHolder.Entries() {
						if !bytes.Equal(e.Key(), entries[i][ei].Key) {
							panic(fmt.Errorf("committed tx key does not match input values"))
						}

						val, err := immuStore.ReadValue(e)
						if err != nil {
							panic(err)
						}

						if !bytes.Equal(val, entries[i][ei].Value) {
							panic(fmt.Errorf("committed tx value does not match input values"))
						}
					}
				}

				fmt.Printf("All committed txs successfully verified against input kv data by committer %d!\r\n", id)
			}

			wgEnded.Done()

			fmt.Printf("Committer %d successfully ended!\r\n", id)
		}(c)
	}

	wgInit.Wait()

	wgStart.Done()

	start := time.Now()

	wgWork.Wait()

	wgEnded.Wait()

	elapsed := time.Since(start)

	fmt.Printf("\r\nAll committers %d have successfully completed their work within %s!\r\n", *committers, elapsed)

	if *txLinking || *kvInclusion {
		fmt.Println("Starting full scan to verify linear cryptographic linking...")
		start := time.Now()

		txHolder, err := txHolderPool.Alloc()
		if err != nil {
			panic(err)
		}
		defer txHolderPool.Release(txHolder)

		txReader, err := immuStore.NewTxReader(1, false, txHolder)
		if err != nil {
			panic(err)
		}

		verifiedTxs := 0

		for {
			tx, err := txReader.Read()
			if err != nil {
				if err == store.ErrNoMoreEntries {
					break
				}
				panic(err)
			}

			entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
			if err != nil {
				panic(err)
			}

			if *kvInclusion {
				for _, e := range tx.Entries() {
					proof, err := tx.Proof(e.Key())
					if err != nil {
						panic(err)
					}

					val, err := immuStore.ReadValue(e)
					if err != nil {
						panic(err)
					}

					kv := &store.EntrySpec{Key: e.Key(), Value: val}

					verifies := htree.VerifyInclusion(proof, entrySpecDigest(kv), tx.Header().Eh)
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
