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

package store

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/dgraph-io/badger/v2"
)

type badgerWrapper struct {
	*badger.DB
	opts badger.Options
}

func makeBadger() *badgerWrapper {
	dir, err := ioutil.TempDir("", "immu_badger")
	if err != nil {
		log.Fatal(err)
		return nil
	}
	sLog := logger.NewSimpleLoggerWithLevel("test(immudb)", os.Stderr, logger.LogDebug)
	_, badgerOpts := DefaultOptions(dir, sLog)

	db, err := badger.OpenManaged(badgerOpts)
	if err != nil {
		os.RemoveAll(dir)
		log.Fatal(err)
		return nil
	}

	return &badgerWrapper{db, badgerOpts}
}

func (b *badgerWrapper) Close() {
	if err := b.DB.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(b.opts.Dir); err != nil {
		log.Fatal(err)
	}
}

func (b *badgerWrapper) Restart() {
	b.DB.Close()
	db, err := badger.OpenManaged(b.opts)
	if err != nil {
		log.Fatal(err)
		return
	}
	b.DB = db
}
