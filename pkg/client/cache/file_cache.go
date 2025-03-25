/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package cache

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
	"github.com/rogpeppe/go-internal/lockedfile"
)

// STATE_FN ...
const STATE_FN = ".state-"

type fileCache struct {
	Dir       string
	stateFile *lockedfile.File
}

// NewFileCache returns a new file cache
func NewFileCache(dir string) Cache {
	return &fileCache{Dir: dir}
}

func (w *fileCache) Get(serverUUID string, db string) (*schema.ImmutableState, error) {
	if w.stateFile == nil {
		return nil, ErrCacheNotLocked
	}
	_, err := w.stateFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(w.stateFile)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, db+":") {
			continue
		}

		oldState, err := base64.StdEncoding.DecodeString(line[len(db)+1:])
		if err != nil {
			return nil, ErrLocalStateCorrupted
		}

		if len(oldState) == 0 {
			return nil, ErrLocalStateCorrupted
		}

		state := &schema.ImmutableState{}
		if err = proto.Unmarshal(oldState, state); err != nil {
			return nil, ErrLocalStateCorrupted
		}

		return state, nil
	}
	return nil, ErrPrevStateNotFound
}

func (w *fileCache) Set(serverUUID string, db string, state *schema.ImmutableState) error {
	if w.stateFile == nil {
		return ErrCacheNotLocked
	}
	raw, err := proto.Marshal(state)
	if err != nil {
		return err
	}

	newState := db + ":" + base64.StdEncoding.EncodeToString(raw)

	var exists bool
	_, err = w.stateFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(w.stateFile)
	scanner.Split(bufio.ScanLines)
	var lines [][]byte
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, db+":") {
			exists = true
			lines = append(lines, []byte(newState))
		} else {
			lines = append(lines, []byte(line))
		}
	}
	if !exists {
		lines = append(lines, []byte(newState))
	}
	output := bytes.Join(lines, []byte("\n"))

	_, err = w.stateFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	err = w.stateFile.Truncate(0)
	if err != nil {
		return err
	}
	_, err = w.stateFile.Write(output)
	if err != nil {
		return err
	}
	return nil
}

func (w *fileCache) Lock(serverUUID string) (err error) {
	w.stateFile, err = lockedfile.OpenFile(w.getStateFilePath(serverUUID), os.O_RDWR|os.O_CREATE, 0655)
	return err
}

func (w *fileCache) Unlock() (err error) {
	if w.stateFile != nil {
		return w.stateFile.Close()
	}
	return nil
}

func (w *fileCache) ServerIdentityCheck(serverIdentity, serverUUID string) error {
	return validateServerIdentityInFile(
		serverIdentity,
		serverUUID,
		w.Dir,
	)
}

func (w *fileCache) getStateFilePath(UUID string) string {
	return filepath.Join(w.Dir, STATE_FN+UUID)
}
