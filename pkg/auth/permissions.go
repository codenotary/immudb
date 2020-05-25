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

var methodsPermissions = map[string]byte{
	// readwrite methods
	"/immudb.schema.ImmuService/Set":           Permissions.RW,
	"/immudb.schema.ImmuService/SetSV":         Permissions.RW,
	"/immudb.schema.ImmuService/SafeSet":       Permissions.RW,
	"/immudb.schema.ImmuService/SafeSetSV":     Permissions.RW,
	"/immudb.schema.ImmuService/SetBatch":      Permissions.RW,
	"/immudb.schema.ImmuService/SetBatchSV":    Permissions.RW,
	"/immudb.schema.ImmuService/Reference":     Permissions.RW,
	"/immudb.schema.ImmuService/SafeReference": Permissions.RW,
	"/immudb.schema.ImmuService/ZAdd":          Permissions.RW,
	"/immudb.schema.ImmuService/SafeZAdd":      Permissions.RW,
	// admin methods
	"/immudb.schema.ImmuService/CreateUser":       Permissions.Admin,
	"/immudb.schema.ImmuService/ChangePassword":   Permissions.Admin,
	"/immudb.schema.ImmuService/SetPermission":    Permissions.Admin,
	"/immudb.schema.ImmuService/DeleteUser":       Permissions.Admin,
	"/immudb.schema.ImmuService/UpdateAuthConfig": Permissions.Admin,
	"/immudb.schema.ImmuService/UpdateMTLSConfig": Permissions.Admin,
}

func HasPermissionForMethod(userPermission byte, method string) bool {
	methodPermission, ok := methodsPermissions[method]
	if !ok {
		methodPermission = Permissions.R
	}
	return methodPermission&userPermission == methodPermission
}
func HasPermissionSuffixForMethod(username []byte, method string) bool {
	permission, ok := methodsPermissions[method]
	if !ok {
		return true
	}
	return HasPermissionSuffix(username, permission)
}
func HasPermissionSuffix(username []byte, permission byte) bool {
	return username[len(username)-1:][0]&permission == permission
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
