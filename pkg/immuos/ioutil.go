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

package immuos

import (
	"io/ioutil"
	"os"
)

// Ioutil ...
type Ioutil interface {
	ReadFile(filename string) ([]byte, error)
	WriteFile(filename string, data []byte, perm os.FileMode) error
}

// StandardIoutil ...
type StandardIoutil struct {
	ReadFileF  func(filename string) ([]byte, error)
	WriteFileF func(filename string, data []byte, perm os.FileMode) error
}

// NewStandardIoutil ...
func NewStandardIoutil() *StandardIoutil {
	return &StandardIoutil{
		ReadFileF:  ioutil.ReadFile,
		WriteFileF: ioutil.WriteFile,
	}
}

// ReadFile ...
func (sio *StandardIoutil) ReadFile(filename string) ([]byte, error) {
	return sio.ReadFileF(filename)
}

// WriteFile ...
func (sio *StandardIoutil) WriteFile(filename string, data []byte, perm os.FileMode) error {
	return sio.WriteFileF(filename, data, perm)
}
