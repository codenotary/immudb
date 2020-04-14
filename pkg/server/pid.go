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

package server

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// PIDFile contains path of pid file
type PIDFile struct {
	path string
}

func checkPIDFileAlreadyExists(path string) error {
	if pidByte, err := ioutil.ReadFile(path); err == nil {
		pidString := strings.TrimSpace(string(pidByte))
		if pid, err := strconv.Atoi(pidString); err == nil {
			if processExists(pid) {
				return fmt.Errorf("pid file found, ensure immudb is not running or delete %s", path)
			}
		}
	}
	return nil
}

// NewPid returns a new PIDFile or an error
func NewPid(path string) (PIDFile, error) {
	if err := checkPIDFileAlreadyExists(path); err != nil {
		return PIDFile{}, err
	}
	if fn := filepath.Base(path); fn == "." {
		return PIDFile{}, fmt.Errorf("Pid filename is invalid: %s", path)
	}
	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		if err := os.Mkdir(filepath.Dir(path), os.FileMode(0755)); err != nil {
			return PIDFile{}, err
		}
	}
	if err := ioutil.WriteFile(path, []byte(fmt.Sprintf("%d", os.Getpid())), 0644); err != nil {
		return PIDFile{}, err
	}
	return PIDFile{path: path}, nil
}

// Remove remove the pid file
func (file PIDFile) Remove() error {
	return os.Remove(file.path)
}

func processExists(pid int) bool {
	if _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); err == nil {
		return true
	}
	return false
}
