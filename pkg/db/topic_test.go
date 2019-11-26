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

var root64th = [sha256.Size]byte{0xb1, 0xbe, 0x73, 0xef, 0x38, 0x8e, 0x7e, 0xd3, 0x79, 0x71, 0x7, 0x26, 0xd1, 0x19, 0xa5, 0x35, 0xb8, 0x67, 0x24, 0x12, 0x48, 0x25, 0x7a, 0x7e, 0x2e, 0x34, 0x32, 0x29, 0x65, 0x60, 0xdf, 0xf9}

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
		index, err := topic.Set(key, key)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index, "n=%d", n)
	}

	for n := uint64(0); n <= 64; n++ {
		key := []byte(strconv.FormatUint(n, 10))
		value, index, err := topic.Get(key)
		assert.NoError(t, err, "n=%d", n)
		assert.Equal(t, n, index, "n=%d", n)
		assert.Equal(t, key, value, "n=%d", n)
	}

	topic.store.WaitSync()
	assert.Equal(t, root64th, tree.Root(topic.store))

	topic.store.Close()
	assert.Equal(t, root64th, tree.Root(topic.store))

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
