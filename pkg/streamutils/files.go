package streamutils

import (
	"bufio"
	"bytes"
	"github.com/codenotary/immudb/pkg/stream"
	"os"
)

// GetKeyValuesFromFiles returns an array of stream.KeyValue from full file names paths. Each key value is composed by a key that is the file name and a reader of the content of the file, if exists.
// @todo Michele use only base path to avoid to use  pieces of local file system as key
func GetKeyValuesFromFiles(filenames ...string) ([]*stream.KeyValue, error) {
	var kvs []*stream.KeyValue
	for _, fn := range filenames {
		fs, err := os.Stat(fn)
		if err != nil {
			return nil, err
		}

		f, err := os.Open(fn)
		if err != nil {
			return nil, err
		}
		kv := &stream.KeyValue{
			Key: &stream.ValueSize{
				Content: bufio.NewReader(bytes.NewBuffer([]byte(fn))),
				Size:    len([]byte(fn)),
			},
			Value: &stream.ValueSize{
				Content: bufio.NewReader(f),
				Size:    int(fs.Size()),
			},
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}
