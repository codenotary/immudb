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

package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/pkg/immuos"
)

// PIDFile contains path of pid file
type PIDFile struct {
	path string
	OS   immuos.OS
}

func checkPIDFileAlreadyExists(path string, OS immuos.OS) error {
	if pidByte, err := OS.ReadFile(path); err == nil {
		pidString := strings.TrimSpace(string(pidByte))
		if pid, err := strconv.Atoi(pidString); err == nil {
			if processExists(pid, OS) {
				return fmt.Errorf("pid file found, ensure immudb is not running or delete %s", path)
			}
		}
	}
	return nil
}

// NewPid returns a new PIDFile or an error
func NewPid(path string, OS immuos.OS) (PIDFile, error) {
	if err := checkPIDFileAlreadyExists(path, OS); err != nil {
		return PIDFile{}, err
	}
	if fn := OS.Base(path); fn == "." {
		return PIDFile{}, fmt.Errorf("Pid filename is invalid: %s", path)
	}
	if _, err := OS.Stat(OS.Dir(path)); OS.IsNotExist(err) {
		if err := OS.Mkdir(OS.Dir(path), 0755); err != nil {
			return PIDFile{}, err
		}
	}
	if err := OS.WriteFile(path, []byte(fmt.Sprintf("%d", OS.Getpid())), 0644); err != nil {
		return PIDFile{}, err
	}
	return PIDFile{path: path, OS: OS}, nil
}

// Remove remove the pid file
func (file PIDFile) Remove() error {
	return file.OS.Remove(file.path)
}

func processExists(pid int, OS immuos.OS) bool {
	if _, err := OS.Stat(OS.Join("/proc", strconv.Itoa(pid))); err == nil {
		return true
	}
	return false
}
