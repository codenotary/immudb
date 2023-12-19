/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package streamutils

import (
	"bufio"
	"bytes"
	"os"

	"github.com/codenotary/immudb/pkg/stream"
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
