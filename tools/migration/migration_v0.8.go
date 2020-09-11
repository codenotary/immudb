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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/dgraph-io/badger/v2"
)

const lastFlushedMetaKey = "IMMUDB.METADATA.LAST_FLUSHED_LEAF"
const bitTreeEntry = byte(255)
const tsPrefix = byte(0)

/*
As for release 0.8 of immudb, which includes multi-key insertions, kv data needs to be univocally referenced
by a monotonic increasing index i.e. internally reffered as `ts`.
Values are prefixed with the assigned `ts`value for the entry. Thus, databases created before release 0.8 needs to
be migrated for it's manipulation from immudb v0.8.
This migration utility can be used to migrate any immudb database created with older releases to be fully operative on v0.8
*/
func main() {
	sourceDataDir := flag.String("sourceDataDir", "data_v0.7", "immudb data directory to migrate e.g. ./data_v0.7")
	targetDataDir := flag.String("targetDataDir", "data_v0.8", "immudb data directory where migrated immudb databases will be stored e.g. ./data_v0.8")

	flag.Parse()

	if *sourceDataDir == *targetDataDir || *sourceDataDir == "" || *targetDataDir == "" {
		panic(fmt.Errorf("Illegal arguments. Source and target dirs must be provided and can not be the same"))
	}

	_, err := os.Stat(*sourceDataDir)
	if os.IsNotExist(err) {
		panic(fmt.Errorf("Source data dir %s does not exist", *sourceDataDir))
	}

	dirs, err := dbList(*sourceDataDir)
	fmt.Printf("\r\n%d databases will be migrated\r\n", len(dirs))

	if err != nil {
		panic(err)
	}

	_, err = os.Stat(*targetDataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(*targetDataDir, 0700)
		if err != nil {
			panic(fmt.Errorf("Error creating target folder %s", *targetDataDir))
		}
	}

	migrationStart := time.Now()

	for _, sourcePath := range dirs {
		pathParts := strings.Split(sourcePath, string(filepath.Separator))
		dbname := pathParts[len(pathParts)-1]

		targetPath := *targetDataDir + string(filepath.Separator) + dbname

		fmt.Printf("\r\nStarted migration of %s to %s\r\n", sourcePath, targetPath)

		start := time.Now()

		err = migrateDB(sourcePath, targetPath)

		elapsed := time.Since(start)

		fmt.Println()

		if err == nil {
			fmt.Printf("Migration successfully completed in %s\r\n", elapsed)
		} else {
			fmt.Printf("ERROR migrating: %s to %s\r\n", sourcePath, targetPath)
			panic(err)
		}
	}

	totalElapsed := time.Since(migrationStart)

	fmt.Printf("\r\nAll databases have been successfully migrated in %s\r\n", totalElapsed)
}

func dbList(dataDir string) ([]string, error) {
	var dirs []string

	err := filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if dataDir != path && info.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})

	return dirs, err
}

func migrateDB(sourceDir, targetDir string) error {
	slog := logger.NewSimpleLoggerWithLevel("migration(immudb)", os.Stderr, logger.LogError)

	_, badgerOpts := store.DefaultOptions(sourceDir, slog)
	badgerOpts.ValueDir = sourceDir
	badgerOpts.NumVersionsToKeep = math.MaxInt64 // immutability, always keep all data
	sourcedb, err := badger.OpenManaged(badgerOpts)
	if err != nil {
		return err
	}
	defer sourcedb.Close()

	_, badgerOpts = store.DefaultOptions(targetDir, slog)
	badgerOpts.ValueDir = targetDir
	badgerOpts.NumVersionsToKeep = math.MaxInt64 // immutability, always keep all data
	targetdb, err := badger.OpenManaged(badgerOpts)
	if err != nil {
		return err
	}
	defer targetdb.Close()

	txn := sourcedb.NewTransactionAt(math.MaxUint64, false)
	defer txn.Discard()

	entries := treeLayerWidth(uint8(0), txn)

	fmt.Printf("Number of entries: %v\r\n", entries)

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
	})
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		err = migrateKey(it.Item().Key(), txn, targetdb, entries)

		if err != nil {
			break
		}
	}

	return err
}

func migrateKey(key []byte, txn *badger.Txn, targetdb *badger.DB, entries uint64) error {
	i := uint64(0)
	lasti := uint64(0)
	lastP := 1

	kit := txn.NewKeyIterator(key, badger.IteratorOptions{})
	defer kit.Close()

	for kit.Rewind(); kit.Valid(); kit.Next() {
		value, err := kit.Item().ValueCopy(nil)
		if err != nil {
			return err
		}

		itx := targetdb.NewTransactionAt(math.MaxUint64, true)

		newValue := value

		if (kit.Item().UserMeta()&bitTreeEntry != bitTreeEntry) &&
			!bytes.Equal([]byte(lastFlushedMetaKey), kit.Item().Key()) {

			ts, err := findTS(key, value, kit.Item().Version(), txn)

			if err != nil {
				itx.Discard()
				return err
			}

			newValue = wrapValueWithTS(value, ts)

			i++
		}

		err = itx.SetEntry(&badger.Entry{
			Key:      kit.Item().Key(),
			Value:    newValue,
			UserMeta: kit.Item().UserMeta(),
		})

		if err != nil {
			itx.Discard()
			return err
		}

		itx.CommitAt(kit.Item().Version(), nil)
		itx.Discard()

		if i > lasti && i%1000 == 0 {
			fmt.Print(".")
		}

		p := int((i * 100) / entries)
		if p > lastP && p%10 == 0 {
			fmt.Printf("(%d)%%", p)
			lastP = p
		}
	}

	return nil
}

func findTS(key []byte, value []byte, ts uint64, txn *badger.Txn) (uint64, error) {
	maxKey := treeKey(0, ts-1)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true
	opts.Reverse = true
	lit := txn.NewIterator(opts)
	defer lit.Close()

	for lit.Seek(maxKey); lit.ValidForPrefix(maxKey[:2]); lit.Next() {

		refkey, err := lit.Item().ValueCopy(nil)

		if err != nil {
			return 0, err
		}

		hash, k, err := decodeRefTreeKey(refkey)

		if err != nil {
			return 0, err
		}

		if bytes.Equal(key, k) {
			realHash := api.Digest(ts-1, key, value)
			if hash != realHash {
				return 0, fmt.Errorf("Error for key %v, %v", key, store.ErrInconsistentDigest)
			}

			return ts, nil
		}

		ts--
	}
	return 0, fmt.Errorf("Could not find associated tree leaf for key %v", key)
}

func wrapValueWithTS(v []byte, ts uint64) []byte {
	tsv := make([]byte, len(v)+8)
	binary.BigEndian.PutUint64(tsv, ts)
	copy(tsv[8:], v)
	return tsv
}

func treeKey(layer uint8, index uint64) []byte {
	k := make([]byte, 1+1+8)
	k[0] = tsPrefix
	k[1] = layer
	binary.BigEndian.PutUint64(k[2:], index)
	return k
}

// refTreeKey split a value of a badger item in an the hash array and slice reference key
func decodeRefTreeKey(rtk []byte) ([sha256.Size]byte, []byte, error) {
	lrtk := len(rtk)

	if lrtk < sha256.Size {
		// this should not happen
		return [sha256.Size]byte{}, nil, store.ErrInconsistentState
	}
	hash := make([]byte, sha256.Size)
	reference := make([]byte, lrtk-sha256.Size)
	copy(hash, rtk[:sha256.Size])
	copy(reference, rtk[sha256.Size:][:])

	var hArray [sha256.Size]byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&hash))
	hArray = *(*[sha256.Size]byte)(unsafe.Pointer(hdr.Data))
	if lrtk == sha256.Size {
		return hArray, nil, store.ErrObsoleteDataFormat
	}
	return hArray, reference, nil
}

func treeLayerWidth(layer uint8, txn *badger.Txn) uint64 {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Reverse = true
	it := txn.NewIterator(opts)
	defer it.Close()

	maxKey := []byte{tsPrefix, layer, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for it.Seek(maxKey); it.ValidForPrefix(maxKey[:2]); it.Next() {
		return binary.BigEndian.Uint64(it.Item().Key()[2:]) + 1
	}
	return 0
}
