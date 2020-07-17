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

package immuos

import "os"

// OS ...
type OS interface {
	Filepath
	Create(name string) (*os.File, error)
	Remove(name string) error
	Getwd() (string, error)
	Stat(name string) (os.FileInfo, error)
	RemoveAll(path string) error
	Rename(oldpath, newpath string) error
}

// StandardOS ...
type StandardOS struct {
	*StandardFilepath
	CreateF    func(name string) (*os.File, error)
	RemoveF    func(name string) error
	GetwdF     func() (string, error)
	StatF      func(name string) (os.FileInfo, error)
	RemoveAllF func(path string) error
	RenameF    func(oldpath, newpath string) error
}

// NewStandardOS ...
func NewStandardOS() *StandardOS {
	return &StandardOS{
		StandardFilepath: NewStandardFilepath(),
		CreateF:          os.Create,
		RemoveF:          os.Remove,
		GetwdF:           os.Getwd,
		StatF:            os.Stat,
		RemoveAllF:       os.RemoveAll,
		RenameF:          os.Rename,
	}
}

// Create ...
func (sos *StandardOS) Create(name string) (*os.File, error) {
	return sos.CreateF(name)
}

// Remove ...
func (sos *StandardOS) Remove(name string) error {
	return sos.RemoveF(name)
}

// Getwd ...
func (sos *StandardOS) Getwd() (string, error) {
	return sos.GetwdF()
}

// Stat ...
func (sos *StandardOS) Stat(name string) (os.FileInfo, error) {
	return sos.StatF(name)
}

// RemoveAll ...
func (sos *StandardOS) RemoveAll(path string) error {
	return sos.RemoveAllF(path)
}

// Rename ...
func (sos *StandardOS) Rename(oldpath, newpath string) error {
	return sos.RenameF(oldpath, newpath)
}
