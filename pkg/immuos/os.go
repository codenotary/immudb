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

package immuos

import "os"

// OS ...
type OS interface {
	Filepath
	User
	Ioutil
	Create(name string) (*os.File, error)
	Getwd() (string, error)
	Mkdir(name string, perm os.FileMode) error
	MkdirAll(path string, perm os.FileMode) error
	Remove(name string) error
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	Chown(name string, uid, gid int) error
	Chmod(name string, mode os.FileMode) error
	IsNotExist(err error) bool
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Executable() (string, error)
	Getpid() int
}

// StandardOS ...
type StandardOS struct {
	*StandardFilepath
	*StandardUser
	*StandardIoutil
	CreateF     func(name string) (*os.File, error)
	GetwdF      func() (string, error)
	MkdirF      func(name string, perm os.FileMode) error
	MkdirAllF   func(path string, perm os.FileMode) error
	RemoveF     func(name string) error
	RemoveAllF  func(path string) error
	RenameF     func(oldpath, newpath string) error
	StatF       func(name string) (os.FileInfo, error)
	ChownF      func(name string, uid, gid int) error
	ChmodF      func(name string, mode os.FileMode) error
	IsNotExistF func(err error) bool
	OpenF       func(name string) (*os.File, error)
	OpenFileF   func(name string, flag int, perm os.FileMode) (*os.File, error)
	ExecutableF func() (string, error)
	GetpidF     func() int
}

// NewStandardOS ...
func NewStandardOS() *StandardOS {
	return &StandardOS{
		StandardFilepath: NewStandardFilepath(),
		StandardUser:     NewStandardUser(),
		StandardIoutil:   NewStandardIoutil(),
		CreateF:          os.Create,
		GetwdF:           os.Getwd,
		MkdirF:           os.Mkdir,
		MkdirAllF:        os.MkdirAll,
		RemoveF:          os.Remove,
		RemoveAllF:       os.RemoveAll,
		RenameF:          os.Rename,
		StatF:            os.Stat,
		ChownF:           os.Chown,
		ChmodF:           os.Chmod,
		IsNotExistF:      os.IsNotExist,
		OpenF:            os.Open,
		OpenFileF:        os.OpenFile,
		ExecutableF:      os.Executable,
		GetpidF:          os.Getpid,
	}
}

// Create ...
func (sos *StandardOS) Create(name string) (*os.File, error) {
	return sos.CreateF(name)
}

// Getwd ...
func (sos *StandardOS) Getwd() (string, error) {
	return sos.GetwdF()
}

// Mkdir ...
func (sos *StandardOS) Mkdir(name string, perm os.FileMode) error {
	return sos.MkdirF(name, perm)
}

// MkdirAll ...
func (sos *StandardOS) MkdirAll(path string, perm os.FileMode) error {
	return sos.MkdirAllF(path, perm)
}

// Remove ...
func (sos *StandardOS) Remove(name string) error {
	return sos.RemoveF(name)
}

// RemoveAll ...
func (sos *StandardOS) RemoveAll(path string) error {
	return sos.RemoveAllF(path)
}

// Rename ...
func (sos *StandardOS) Rename(oldpath, newpath string) error {
	return sos.RenameF(oldpath, newpath)
}

// Stat ...
func (sos *StandardOS) Stat(name string) (os.FileInfo, error) {
	return sos.StatF(name)
}

// Chown ...
func (sos *StandardOS) Chown(name string, uid, gid int) error {
	return sos.ChownF(name, uid, gid)
}

// Chmod ...
func (sos *StandardOS) Chmod(name string, mode os.FileMode) error {
	return sos.ChmodF(name, mode)
}

// IsNotExist ...
func (sos *StandardOS) IsNotExist(err error) bool {
	return sos.IsNotExistF(err)
}

// Open ...
func (sos *StandardOS) Open(name string) (*os.File, error) {
	return sos.OpenF(name)
}

// OpenFile ...
func (sos *StandardOS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return sos.OpenFileF(name, flag, perm)
}

// Executable ...
func (sos *StandardOS) Executable() (string, error) {
	return sos.ExecutableF()
}

// Getpid ...
func (sos *StandardOS) Getpid() int {
	return sos.GetpidF()
}
