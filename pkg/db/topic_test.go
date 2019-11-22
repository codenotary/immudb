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

package db

import (
	"crypto/sha256"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/codenotary/immudb/pkg/tree"
	"github.com/dgraph-io/badger/v2/options"

	"github.com/stretchr/testify/assert"
)

var root64th = [sha256.Size]byte{0x31, 0x48, 0x49, 0xb, 0xd8, 0xc3, 0x46, 0x4b, 0xe, 0x30, 0x1f, 0x5d, 0xe3, 0xd, 0xad, 0x87, 0x29, 0x16, 0x47, 0x5, 0x62, 0x1a, 0xc3, 0x64, 0xc4, 0x3b, 0x68, 0xd7, 0xc8, 0x0, 0xfb, 0x9b}

func makeTopic() (*Topic, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}

	opts := DefaultOptions(dir)
	opts.Badger.
		WithSyncWrites(false).
		WithEventLogging(false).
		WithChecksumVerificationMode(options.NoVerification).
		WithVerifyValueChecksum(false)

	topic, err := Open(opts)
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

func TestTopic(t *testing.T) {
	topic, closer := makeTopic()
	defer closer()

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		err := topic.Set(key, key)

		assert.NoError(t, err)
	}

	topic.store.Close()
	topic.store.resetCache() // with empty cache, next call should fetch from DB
	assert.Equal(t, root64th, tree.Root(topic.store))
}

func BenchmarkTopicSet(b *testing.B) {
	topic, closer := makeTopic()
	defer closer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topic.Set([]byte(strconv.FormatUint(uint64(i), 10)), []byte{0, 1, 3, 4, 5, 6, 7})
	}
	b.StopTimer()
}
