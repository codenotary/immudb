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

// PermissionAdmin the admin permission byte
const PermissionAdmin = 255

// Non-admin permissions
const (
	PermissionNone = iota
	PermissionR
	PermissionW
	PermissionRW
)

var methodsPermissions = map[string]byte{
	// readwrite methods
	"/immudb.schema.ImmuService/Set":           PermissionRW,
	"/immudb.schema.ImmuService/SetSV":         PermissionRW,
	"/immudb.schema.ImmuService/SafeSet":       PermissionRW,
	"/immudb.schema.ImmuService/SafeSetSV":     PermissionRW,
	"/immudb.schema.ImmuService/SetBatch":      PermissionRW,
	"/immudb.schema.ImmuService/SetBatchSV":    PermissionRW,
	"/immudb.schema.ImmuService/Reference":     PermissionRW,
	"/immudb.schema.ImmuService/SafeReference": PermissionRW,
	"/immudb.schema.ImmuService/ZAdd":          PermissionRW,
	"/immudb.schema.ImmuService/SafeZAdd":      PermissionRW,
	// admin methods
	"/immudb.schema.ImmuService/ListUsers":        PermissionAdmin,
	"/immudb.schema.ImmuService/CreateUser":       PermissionAdmin,
	"/immudb.schema.ImmuService/ChangePassword":   PermissionAdmin,
	"/immudb.schema.ImmuService/SetPermission":    PermissionAdmin,
	"/immudb.schema.ImmuService/DeactivateUser":   PermissionAdmin,
	"/immudb.schema.ImmuService/UpdateAuthConfig": PermissionAdmin,
	"/immudb.schema.ImmuService/UpdateMTLSConfig": PermissionAdmin,
}

func hasPermissionForMethod(userPermission byte, method string) bool {
	methodPermission, ok := methodsPermissions[method]
	if !ok {
		methodPermission = PermissionR
	}
	return methodPermission&userPermission == methodPermission
}
