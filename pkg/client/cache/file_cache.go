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

package cache

import (
	"github.com/spf13/viper"
	"io/ioutil"
	"path/filepath"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

const ROOT_FN = ".root-"

type fileCache struct {
	Dir string
}

func NewFileCache(dir string) Cache {
	return &fileCache{Dir: dir}
}

func (w *fileCache) Get(serverUuid string) (*schema.Root, error) {
	fn := filepath.Join(w.Dir, string(getRootFileName([]byte(ROOT_FN), []byte(serverUuid))))

	root := new(schema.Root)
	buf, err := ioutil.ReadFile(fn)
	if err == nil {
		if err = proto.Unmarshal(buf, root); err != nil {
			return nil, err
		}
		return root, nil
	}
	return nil, err
}

func (w *fileCache) Set(root *schema.Root, serverUuid string) error {
	fn := filepath.Join(viper.GetString("dir"), string(getRootFileName([]byte(ROOT_FN), []byte(serverUuid))))

	raw, err := proto.Marshal(root)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(fn, raw, 0644)
	if err != nil {
		return err
	}
	return nil
}

func getRootFileName(prefix []byte, serverUuid []byte) []byte {
	l1 := len(prefix)
	l2 := len(serverUuid)
	var fn = make([]byte, l1+l2)
	copy(fn[:], ROOT_FN)
	copy(fn[l1:], serverUuid)
	return fn
}
