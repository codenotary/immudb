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

package servicetest

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
)

type Ossmock struct{}

func (oss Ossmock) LookupGroup(name string) (*user.Group, error) {
	return &user.Group{}, nil
}

func (oss Ossmock) Lookup(username string) (*user.User, error) {
	return &user.User{}, nil
}

func (oss Ossmock) Chown(name string, uid, gid int) error {
	return nil
}

func (oss Ossmock) MkdirAll(path string, perm os.FileMode) error {
	return nil
}

func (oss Ossmock) Remove(name string) error {
	return nil
}

func (oss Ossmock) RemoveAll(path string) error {
	return nil
}

func (oss Ossmock) IsNotExist(err error) bool {
	return false
}

func (oss Ossmock) Open(name string) (*os.File, error) {
	return ioutil.TempFile("", "temp")
}

func (oss Ossmock) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return ioutil.TempFile("", "temp")
}

func (oss Ossmock) Chmod(name string, mode os.FileMode) error {
	return nil
}

type Filepathsmock struct{}

func (fp Filepathsmock) Walk(root string, walkFn filepath.WalkFunc) error {
	return nil
}
