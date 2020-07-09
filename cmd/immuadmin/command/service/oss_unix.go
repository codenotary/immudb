// +build linux darwin

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
	"os/user"
)

type Oss interface {
	LookupGroup(name string) (*user.Group, error)
	Lookup(username string) (*user.User, error)
	Chown(name string, uid, gid int) error
	MkdirAll(path string, perm os.FileMode) error
}

type oss struct{}

func (oss oss) LookupGroup(name string) (*user.Group, error) {
	return user.LookupGroup(name)
}

func (oss oss) Lookup(username string) (*user.User, error) {
	return user.Lookup(username)
}

func (oss oss) Chown(name string, uid, gid int) error {
	return os.Chown(name, uid, gid)
}

func (oss oss) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

/*func (oss oss)  FWalk( uid int, gid int) func(string, os.FileInfo, error)error{
	return func(name string, info os.FileInfo, err error)error{
		if err == nil {
			err = oss.Chown(name, uid, gid)
		}
		return err
	}
}*/
