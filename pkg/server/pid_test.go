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
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/immuos"
	"github.com/stretchr/testify/require"
)

func TestPid(t *testing.T) {
	OS := immuos.NewStandardOS()
	OS.ReadFileF = func(string) ([]byte, error) {
		return []byte("1"), nil
	}

	OS.StatF = func(name string) (os.FileInfo, error) {
		return nil, nil
	}
	pidPath := "somepath"
	_, err := NewPid(pidPath, OS)
	require.Equal(
		t,
		fmt.Errorf("pid file found, ensure immudb is not running or delete %s", pidPath),
		err)

	OS.StatF = func(name string) (os.FileInfo, error) {
		return nil, errors.New("")
	}
	OS.BaseF = func(string) string {
		return "."
	}
	_, err = NewPid(pidPath, OS)
	require.Equal(
		t,
		fmt.Errorf("Pid filename is invalid: %s", pidPath),
		err)
	OS.BaseF = func(path string) string {
		return path
	}

	statCounter := 0
	statFOK := OS.StatF
	OS.StatF = func(name string) (os.FileInfo, error) {
		statCounter++
		if statCounter == 1 {
			return nil, errors.New("")
		}
		return nil, os.ErrNotExist
	}
	errMkdir := errors.New("Mkdir error")
	OS.MkdirF = func(name string, perm os.FileMode) error {
		return errMkdir
	}
	_, err = NewPid(pidPath, OS)
	require.ErrorIs(t, err, errMkdir)
	OS.StatF = statFOK

	errWriteFile := errors.New("WriteFile error")
	OS.WriteFileF = func(filename string, data []byte, perm os.FileMode) error {
		return errWriteFile
	}
	_, err = NewPid(pidPath, OS)
	require.ErrorIs(t, err, errWriteFile)

	OS.WriteFileF = func(filename string, data []byte, perm os.FileMode) error {
		return nil
	}
	pid, err := NewPid(pidPath, OS)
	require.NoError(t, err)
	errRemove := errors.New("Remove error")
	OS.RemoveF = func(name string) error {
		return errRemove
	}
	require.ErrorIs(t, pid.Remove(), errRemove)
}
