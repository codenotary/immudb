package bm

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/codenotary/immudb/pkg/db"
)

func makeTopic() (*db.Topic, func()) {

	dir, err := ioutil.TempDir("", "immu")
	if err != nil {
		log.Fatal(err)
	}

	opts := db.DefaultOptions(dir)
	opts.Badger = opts.Badger.
		WithSyncWrites(false).
		WithEventLogging(false)

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
