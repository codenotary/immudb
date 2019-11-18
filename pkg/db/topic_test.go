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

	"github.com/stretchr/testify/assert"
)

var root64th = [sha256.Size]byte{139, 26, 112, 199, 199, 252, 235, 200, 104, 229, 243, 174, 124, 18, 127, 201, 177, 204, 19, 246, 211, 70, 153, 151, 110, 66, 89, 42, 188, 35, 61, 132}

func makeTopic() (*Topic, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}

	opts := DefaultOptions(dir)
	opts.Badger.
		WithSyncWrites(false).
		WithEventLogging(false)

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
		key := strconv.FormatUint(n, 10)
		err := topic.Set(key, []byte(key))

		assert.NoError(t, err)
	}

	assert.Equal(t, root64th, tree.Root(topic.store))
}

func BenchmarkTreeAdd(b *testing.B) {
	topic, closer := makeTopic()
	defer closer()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		topic.Set(strconv.FormatUint(uint64(i), 10), []byte{0, 1, 3, 4, 5, 6, 7})
	}
	b.StopTimer()
}
