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

package auditor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/proto"
)

type RootService interface {
	Get(serverID string) (*schema.Root, error)
	Set(serverID string, root *schema.Root) error
}

type rootService struct {
	dir string
}

func (rs *rootService) Get(serverID string) (*schema.Root, error) {
	rootsDir := filepath.Join(rs.dir, serverID)
	if err := os.MkdirAll(rootsDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("error creating roots dir %s: %v", rootsDir, err)
	}
	roots, err := ioutil.ReadDir(rootsDir)
	if err != nil {
		return nil, fmt.Errorf("error reading roots dir %s: %v", rootsDir, err)
	}
	if len(roots) == 0 {
		return nil, nil
	}
	prevRootFilename := filepath.Join(rootsDir, roots[len(roots)-1].Name())
	prevRootBytes, err := ioutil.ReadFile(prevRootFilename)
	if err != nil {
		return nil, fmt.Errorf(
			"error reading previous root from %s: %v", prevRootFilename, err)
	}
	prevRoot := new(schema.Root)
	if err = proto.Unmarshal(prevRootBytes, prevRoot); err != nil {
		return nil, fmt.Errorf(
			"error unmarshaling previous root from %s: %v", prevRootFilename, err)
	}
	return prevRoot, nil
}

func (rs *rootService) Set(serverID string, root *schema.Root) error {
	rootBytes, err := proto.Marshal(root)
	if err != nil {
		return fmt.Errorf("error marshaling root %d: %v", root.GetIndex(), err)
	}
	rootsDir := filepath.Join(rs.dir, serverID)
	rootFilename := filepath.Join(rootsDir, ".root")
	if err = ioutil.WriteFile(rootFilename, rootBytes, 0644); err != nil {
		return fmt.Errorf(
			"error writing root %d to file %s: %v",
			root.GetIndex(), rootFilename, err)
	}
	return nil
}
