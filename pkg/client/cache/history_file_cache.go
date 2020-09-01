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
	"os"
	"path/filepath"
	"strings"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

type historyFileCache struct {
	dir string
}

// NewHistoryFileCache returns a new history file cache
func NewHistoryFileCache(dir string) HistoryCache {
	return &historyFileCache{dir: dir}
}

func (history *historyFileCache) Get(serverID string, databasename string) (*schema.Root, error) {
	rootsDir := filepath.Join(history.dir, serverID)
	rootsFileInfos, err := history.getRootsFileInfos(rootsDir)
	if err != nil {
		return nil, err
	}
	if len(rootsFileInfos) == 0 {
		return nil, nil
	}
	prevRootFileName := rootsFileInfos[len(rootsFileInfos)-1].Name()
	prevRootFilePath := filepath.Join(rootsDir, prevRootFileName)
	return history.unmarshalRoot(prevRootFilePath, databasename)
}

func (history *historyFileCache) Walk(
	serverID string, databasename string,
	f func(*schema.Root) interface{},
) ([]interface{}, error) {
	rootsDir := filepath.Join(history.dir, serverID)
	rootsFileInfos, err := history.getRootsFileInfos(rootsDir)
	if err != nil {
		return nil, err
	}
	if len(rootsFileInfos) == 0 {
		return nil, nil
	}
	results := make([]interface{}, 0, len(rootsFileInfos))
	for _, rootFileInfo := range rootsFileInfos {
		rootFilePath := filepath.Join(rootsDir, rootFileInfo.Name())
		root, err := history.unmarshalRoot(rootFilePath, databasename)
		if err != nil {
			return nil, err
		}
		results = append(results, f(root))
	}
	return results, nil
}

func (history *historyFileCache) Set(root *schema.Root, serverID string, databasename string) error {
	rootsDir := filepath.Join(history.dir, serverID)
	if err := os.MkdirAll(rootsDir, os.ModePerm); err != nil {
		return fmt.Errorf("error ensuring roots dir %s exists: %v", rootsDir, err)
	}
	rootFilePath := filepath.Join(rootsDir, ".root")

	//at run first the file does not exist
	input, _ := ioutil.ReadFile(rootFilePath)

	lines := strings.Split(string(input), "\n")
	raw, err := proto.Marshal(root)
	if err != nil {
		return err
	}

	newRoot := databasename + ":" + base64.StdEncoding.EncodeToString(raw) + "\n"
	var exists bool
	for i, line := range lines {
		if strings.Contains(line, databasename+":") {
			exists = true
			lines[i] = newRoot
		}
	}
	if !exists {
		lines = append(lines, newRoot)
	}
	output := strings.Join(lines, "\n")

	if err = ioutil.WriteFile(rootFilePath, []byte(output), 0644); err != nil {
		return fmt.Errorf(
			"error writing root %d to file %s: %v",
			root.GetIndex(), rootFilePath, err)
	}

	return nil
}

func (history *historyFileCache) getRootsFileInfos(dir string) ([]os.FileInfo, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil,
			fmt.Errorf("error ensuring roots dir %s exists: %v", dir, err)
	}
	rootsFileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("error reading roots dir %s: %v", dir, err)
	}
	return rootsFileInfos, nil
}

func (history *historyFileCache) unmarshalRoot(fpath string, databasename string) (*schema.Root, error) {
	root := schema.NewRoot()
	raw, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, fmt.Errorf(
			"error reading root from %s: %v", fpath, err)
	}
	lines := strings.Split(string(raw), "\n")
	for _, line := range lines {
		if strings.Contains(line, databasename+":") {
			r := strings.Split(line, ":")
			if len(r) != 2 {
				return nil, fmt.Errorf("could not find previous root")
			}
			oldRoot, err := base64.StdEncoding.DecodeString(r[1])
			if err != nil {
				return nil, fmt.Errorf("could not find previous root")
			}
			if err = proto.Unmarshal(oldRoot, root); err != nil {
				return nil, fmt.Errorf(
					"error unmarshaling root from %s: %v", fpath, err)
			}
			return root, nil
		}
	}
	return nil, nil
}
