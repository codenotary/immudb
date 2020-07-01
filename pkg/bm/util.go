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

package bm

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/codenotary/immudb/pkg/logger"

	"github.com/codenotary/immudb/pkg/store"
)

func makeStore() (*store.Store, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}
	slog := logger.NewSimpleLoggerWithLevel("bm(immudb)", os.Stderr, logger.LogDebug)
	opts, badgerOpts := store.DefaultOptions(dir, slog)
	st, err := store.Open(opts, badgerOpts)
	if err != nil {
		log.Fatal(err)
	}

	return st, func() {
		if err := st.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}
}
