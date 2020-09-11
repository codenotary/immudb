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
	"encoding/binary"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/store"
	"github.com/dgraph-io/badger/v2"
)

const lastFlushedMetaKey = "IMMUDB.METADATA.LAST_FLUSHED_LEAF"
const bitTreeEntry = byte(255)

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

	for _, sourcePath := range dirs {
		pathParts := strings.Split(sourcePath, string(filepath.Separator))
		dbname := pathParts[len(pathParts)-1]

		targetPath := *targetDataDir + string(filepath.Separator) + dbname

		fmt.Printf("\r\nStarted migration of %s to %s...", sourcePath, targetPath)

		err = migrateDB(sourcePath, targetPath)

		if err == nil {
			fmt.Println("COMPLETED!")
		} else {
			fmt.Printf("ERROR migrating: %s to %s\r\n", sourcePath, targetPath)
		}
	}
	fmt.Println("\r\nAll databases have been successfully migrated!")
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

	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: true,
	})
	defer it.Close()

	i := uint64(0)

	for it.Rewind(); it.Valid(); it.Next() {
		itx := targetdb.NewTransactionAt(math.MaxUint64, true)

		v, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}

		newValue := v

		if (it.Item().UserMeta()&bitTreeEntry != bitTreeEntry) &&
			!bytes.Equal([]byte(lastFlushedMetaKey), it.Item().Key()) {
			newValue = wrapValueWithTS(v, it.Item().Version())
		}

		err = itx.SetEntry(&badger.Entry{
			Key:      it.Item().Key(),
			Value:    newValue,
			UserMeta: it.Item().UserMeta(),
		})
		if err != nil {
			return err
		}

		itx.CommitAt(it.Item().Version(), nil)

		itx.Discard()

		if i%10_000 == 0 {
			fmt.Printf(".")
		}
		i++
	}

	return nil
}

func wrapValueWithTS(v []byte, ts uint64) []byte {
	tsv := make([]byte, len(v)+8)
	binary.BigEndian.PutUint64(tsv, ts)
	copy(tsv[8:], v)
	return tsv
}
