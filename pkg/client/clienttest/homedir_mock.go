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

// Package clienttest ...
package clienttest

import (
	"github.com/codenotary/immudb/pkg/client/homedir"
)

var (
	_ homedir.HomedirService = (*HomedirServiceMock)(nil)
)

// HomedirServiceMock ...
type HomedirServiceMock struct {
	homedir.HomedirService
	WriteFileToUserHomeDirF    func(content []byte, pathToFile string) error
	FileExistsInUserHomeDirF   func(pathToFile string) (bool, error)
	ReadFileFromUserHomeDirF   func(pathToFile string) (string, error)
	DeleteFileFromUserHomeDirF func(pathToFile string) error
}

// WriteFileToUserHomeDir ...
func (h *HomedirServiceMock) WriteFileToUserHomeDir(content []byte, pathToFile string) error {
	return h.WriteFileToUserHomeDirF(content, pathToFile)
}

// FileExistsInUserHomeDir ...
func (h *HomedirServiceMock) FileExistsInUserHomeDir(pathToFile string) (bool, error) {
	return h.FileExistsInUserHomeDirF(pathToFile)
}

// ReadFileFromUserHomeDir ...
func (h *HomedirServiceMock) ReadFileFromUserHomeDir(pathToFile string) (string, error) {
	return h.ReadFileFromUserHomeDirF(pathToFile)
}

// DeleteFileFromUserHomeDir ...
func (h *HomedirServiceMock) DeleteFileFromUserHomeDir(pathToFile string) error {
	return h.DeleteFileFromUserHomeDirF(pathToFile)
}

// DefaultHomedirServiceMock ...
func DefaultHomedirServiceMock() *HomedirServiceMock {
	return &HomedirServiceMock{
		WriteFileToUserHomeDirF: func(content []byte, pathToFile string) error {
			return nil
		},
		FileExistsInUserHomeDirF: func(pathToFile string) (bool, error) {
			return false, nil
		},
		ReadFileFromUserHomeDirF: func(pathToFile string) (string, error) {
			return "", nil
		},
		DeleteFileFromUserHomeDirF: func(pathToFile string) error {
			return nil
		},
	}
}
