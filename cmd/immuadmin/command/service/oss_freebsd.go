// +build freebsd

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
)

type Oss interface {
	MkdirAll(path string, perm os.FileMode) error
}

type oss struct{}

func (oss oss) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

type Filepaths interface {
	Walk(root string, walkFn filepath.WalkFunc) error
}

type filepaths struct{}

func (fp filepaths) Walk(root string, walkFn filepath.WalkFunc) error {
	return nil
}
