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
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

// STATE_FN ...
const STATE_FN = ".state-"

type fileCache struct {
	Dir string
}

// NewFileCache returns a new file cache
func NewFileCache(dir string) Cache {
	return &fileCache{Dir: dir}
}

func (w *fileCache) Get(serverUUID, db string) (*schema.ImmutableState, error) {
	fn := filepath.Join(w.Dir, string(getRootFileName([]byte(STATE_FN), []byte(serverUUID))))

	raw, err := ioutil.ReadFile(fn)
	if err != nil {
		return nil, err
	}
	state := &schema.ImmutableState{}
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		if strings.Contains(line, db+":") {
			r := strings.Split(line, ":")
			if len(r) != 2 {
				return nil, fmt.Errorf("could not find previous state")
			}
			oldState, err := base64.StdEncoding.DecodeString(r[1])
			if err != nil {
				return nil, fmt.Errorf("could not find previous state")
			}
			state := &schema.ImmutableState{}
			if err = proto.Unmarshal(oldState, state); err != nil {
				return nil, err
			}
			return state, nil
		}
	}
	return state, nil
}

func (w *fileCache) Set(serverUUID, db string, state *schema.ImmutableState) error {
	raw, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	fn := filepath.Join(w.Dir, string(getRootFileName([]byte(STATE_FN), []byte(serverUUID))))

	input, _ := ioutil.ReadFile(fn)
	lines := strings.Split(string(input), "\n")

	newState := db + ":" + base64.StdEncoding.EncodeToString(raw) + "\n"
	var exists bool
	for i, line := range lines {
		if strings.Contains(line, db+":") {
			exists = true
			lines[i] = newState
		}
	}
	if !exists {
		lines = append(lines, newState)
	}
	output := strings.Join(lines, "\n")

	if err = ioutil.WriteFile(fn, []byte(output), 0644); err != nil {
		return err
	}
	return nil
}

func getRootFileName(prefix []byte, serverUUID []byte) []byte {
	l1 := len(prefix)
	l2 := len(serverUUID)
	var fn = make([]byte, l1+l2)
	copy(fn[:], STATE_FN)
	copy(fn[l1:], serverUUID)
	return fn
}
