// +build windows

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

package service

import (
	"os"
	"path/filepath"
)

type Oss interface {
	MkdirAll(path string, perm os.FileMode) error
	Remove(name string) error
	RemoveAll(name string) error
	IsNotExist(err error) bool
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
}

type oss struct{}

func (oss oss) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (oss oss) Remove(name string) error {
	return os.Remove(name)
}

func (oss oss) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (oss oss) IsNotExist(err error) bool {
	return os.IsNotExist(err)
}

func (oss oss) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (oss oss) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

type Filepaths interface {
	Walk(root string, walkFn filepath.WalkFunc) error
}

type filepaths struct{}

func (fp filepaths) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(root, walkFn)
}
