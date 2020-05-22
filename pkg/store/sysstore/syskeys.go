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

package sysstore

import (
	"bytes"
)

var sysKeysPrefix = []byte("!immudb!")
var sysKeysPrefixes = struct {
	user []byte
}{
	user: append(sysKeysPrefix, []byte("user")...),
}

func raceSafePrefix(prefix []byte, key []byte) []byte {
	tmp := make([]byte, len(prefix)+len(key))
	copy(tmp, prefix)
	return append(tmp, key...)
}

func IsValidKey(k []byte) bool {
	return !bytes.HasPrefix(k, sysKeysPrefix)
}

func UserPrefix() []byte {
	return sysKeysPrefixes.user
}
func IsUserKey(k []byte) bool {
	return bytes.HasPrefix(k, sysKeysPrefixes.user)
}
func AddUserPrefix(k []byte) []byte {
	if IsUserKey(k) {
		return k
	}
	return raceSafePrefix(sysKeysPrefixes.user, k)
}
func TrimUserPrefix(k []byte) []byte {
	if !IsUserKey(k) {
		return k
	}
	return bytes.TrimPrefix(k, sysKeysPrefixes.user)
}
