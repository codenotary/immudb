/*
Copyright 2019 vChain, Inc.

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

	"github.com/codenotary/immustore/pkg/logger"

	"github.com/codenotary/immustore/pkg/db"
)

func makeTopic() (*db.Topic, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}

	opts := db.DefaultOptions(dir)
	opts.Badger.Logger = logger.NewWithLevel("bm(immud)", os.Stderr, logger.LogDebug)

	topic, err := db.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	return topic, func() {
		if err := topic.Close(); err != nil {
			log.Fatal(err)
		}
		if err := os.RemoveAll(dir); err != nil {
			log.Fatal(err)
		}
	}
}
