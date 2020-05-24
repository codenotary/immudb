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

package auth

import "bytes"

var Permissions = struct {
	Admin byte
	R     byte
	W     byte
	RW    byte
}{
	Admin: 255,
	R:     1,
	W:     2,
	RW:    3,
}

func HasPermissionSuffix(username []byte, permission byte) bool {
	return username[len(username)-1:][0] == permission
}
func GetPermissionFromSuffix(username []byte) byte {
	return username[len(username)-1:][0]
}
func AddPermissionSuffix(username []byte, permission byte) []byte {
	return bytes.Join([][]byte{username, []byte{permission}}, []byte{'!'})
}
func TrimPermissionSuffix(username []byte) []byte {
	return username[:len(username)-2]
}
func ReplacePermissionSuffix(username []byte, permission byte) []byte {
	return bytes.Join(
		[][]byte{username[:len(username)-2], []byte{permission}}, []byte{'!'})
}
