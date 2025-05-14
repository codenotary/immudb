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

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/codenotary/immudb/v2/embedded/appendable"
	"github.com/codenotary/immudb/v2/embedded/htree"
	"github.com/codenotary/immudb/v2/embedded/store"
)

func main() {
	dataDir := flag.String("dataDir", "data", "data directory")

	parallelIO := flag.Int("parallelIO", 1, "number of parallel IO")
	fileSize := flag.Int("fileSize", 1<<26, "file size up to which a new ones are created")
	cFormat := flag.String("compressionFormat", "no-compression", "one of: no-compression, flate, gzip, lzw, zlib")
	cLevel := flag.String("compressionLevel", "best-speed", "one of: best-speed, best-compression, default-compression, huffman-only")

	synced := flag.Bool("synced", false, "strict sync mode - no data lost")
	openedLogFiles := flag.Int("openedLogFiles", 10, "number of maximum number of opened files per each log type")

	mode := flag.String("mode", "", "interactive|auto")

	action := flag.String("action", "get", "get|set")
	waitForIndexing := flag.Int("waitForIndexing", 1000, "amount of millis waiting for indexing entries")
	key := flag.String("key", "", "key to look for")
	value := flag.String("value", "", "value to be associated to key")

	committers := flag.Int("committers", 10, "number of concurrent committers")
	txCount := flag.Int("txCount", 1_000, "number of tx to commit")
	kvCount := flag.Int("kvCount", 1_000, "number of kv entries per tx")
	kLen := flag.Int("kLen", 32, "key length (bytes)")
	vLen := flag.Int("vLen", 32, "value length (bytes)")
	rndKeys := flag.Bool("rndKeys", false, "keys are randomly generated")
	rndValues := flag.Bool("rndValues", true, "values are randomly generated")
	txDelay := flag.Int("txDelay", 10, "delay (millis) between txs")
	printAfter := flag.Int("printAfter", 100, "print a dot '.' after specified number of committed txs")
	txRead := flag.Bool("txRead", false, "validate committed txs against input kv data")
	txLinking := flag.Bool("txLinking", true, "full scan to verify linear cryptographic linking between txs")
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
		WithMaxValueLen(1 << 26) // 64Mb

	immuStore, err := store.Open(*dataDir, opts)
	exitOnErr(err)

	immuStoreLedger, err := immuStore.OpenLedger("test")
	exitOnErr(err)

	st, err := store.Open("data", store.DefaultOptions())
	exitOnErr(err)

	defer st.Close()

	stLedger, err := st.OpenLedger("default")
	exitOnErr(err)

	tx, err := stLedger.NewWriteOnlyTx(context.Background())
	exitOnErr(err)

	err = tx.Set([]byte("hello"), nil, []byte("immutable-world!"))
	exitOnErr(err)

	hdr, err := tx.Commit(context.Background())
	exitOnErr(err)

	fmt.Printf("key %s successfully set in tx %d", "hello", hdr.ID)

	exitOnErr(err)

	defer func() {
		err := immuStoreLedger.Close()
		if err != nil {
			fmt.Printf("\r\nImmutable Transactional Key-Value Log closed with error: %v\r\n", err)
			exitOnErr(err)
		}
		fmt.Printf("\r\nImmutable Transactional Key-Value Log successfully closed!\r\n")
	}()

	fmt.Printf("Immutable Transactional Key-Value Log with %d Txs successfully opened!\r\n", immuStoreLedger.TxCount())

	if *mode == "interactive" {
		if *action == "get" {
			time.Sleep(time.Duration(*waitForIndexing) * time.Millisecond)

			snap, err := immuStoreLedger.Snapshot(nil)
			exitOnErr(err)

			defer snap.Close()

			valRef, err := snap.Get(context.Background(), []byte(*key))
			exitOnErr(err)

			val, err := immuStoreLedger.Resolve(valRef)
			exitOnErr(err)

			fmt.Printf("key: %s, value: %s, ts: %d, hc: %d\r\n", *key, base64.StdEncoding.EncodeToString(val), valRef.TxID(), valRef.Revision())
			return
		}

		if *action == "set" {
			tx, err := stLedger.NewWriteOnlyTx(context.Background())
			exitOnErr(err)

			err = tx.Set([]byte(*key), nil, []byte(*value))
			exitOnErr(err)

			_, err = tx.Commit(context.Background())
			exitOnErr(err)

			return
		}

		panic("invalid action")
	}

	txHolderPool, err := immuStoreLedger.NewTxHolderPool(*committers, false)
	if err != nil {
		panic(fmt.Sprintf("Couldn't allocate tx holder pool: %v", err))
	}

	if *mode == "auto" {
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

						entries[t][i] = &store.EntrySpec{Key: k, Value: v}
					}
				}

				wgInit.Done()

				wgStart.Wait()

				fmt.Printf("\r\nCommitter %d is running...\r\n", id)

				ids := make([]uint64, *txCount)

				for t := 0; t < *txCount; t++ {
					tx, err := immuStoreLedger.NewWriteOnlyTx(context.Background())
					exitOnErr(err)

					for _, e := range entries[t] {
						err = tx.Set(e.Key, e.Metadata, e.Value)
						exitOnErr(err)
					}

					txhdr, err := tx.Commit(context.Background())
					exitOnErr(err)

					ids[t] = txhdr.ID

					if *printAfter > 0 && t%*printAfter == 0 {
						fmt.Print(".")
					}

					time.Sleep(time.Duration(*txDelay) * time.Millisecond)
				}

				wgWork.Done()
				fmt.Printf("\r\nCommitter %d done with commits!\r\n", id)

				if *txRead {
					fmt.Printf("Starting committed tx against input kv data by committer %d...\r\n", id)

					txHolder, err := txHolderPool.Alloc()
					exitOnErr(err)

					defer txHolderPool.Release(txHolder)

					for i := range ids {
						immuStoreLedger.ReadTx(ids[i], true, txHolder)

						for ei, e := range txHolder.Entries() {
							if !bytes.Equal(e.Key(), entries[i][ei].Key) {
								panic(fmt.Errorf("committed tx key does not match input values"))
							}

							val, err := immuStoreLedger.ReadValue(e)
							exitOnErr(err)

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
		elapsed := time.Since(start)

		fmt.Printf("\r\nAll committers %d have successfully completed their work within %s!\r\n", *committers, elapsed)

		wgEnded.Wait()

		if *txLinking || *kvInclusion {
			fmt.Println("Starting full scan to verify linear cryptographic linking...")
			start := time.Now()

			txHolder, err := txHolderPool.Alloc()
			exitOnErr(err)

			defer txHolderPool.Release(txHolder)

			txReader, err := immuStoreLedger.NewTxReader(1, false, txHolder)
			exitOnErr(err)

			verifiedTxs := 0

			for {
				tx, err := txReader.Read()
				if err != nil {
					if errors.Is(err, store.ErrNoMoreEntries) {
						break
					}
					exitOnErr(err)
				}

				entrySpecDigest, err := store.EntrySpecDigestFor(tx.Header().Version)
				exitOnErr(err)

				if *kvInclusion {
					for _, e := range tx.Entries() {
						proof, err := tx.Proof(e.Key())
						exitOnErr(err)

						val, err := immuStoreLedger.ReadValue(e)
						exitOnErr(err)

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

		fmt.Println("Waiting for indexing...")
		time.Sleep(time.Duration(*waitForIndexing) * time.Millisecond)
		fmt.Println("Done")

		return
	}

	panic("please specify a valid mode of operation: interactive|auto")
}

func exitOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
