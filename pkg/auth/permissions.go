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

import (
	"bytes"
)

// PermissionAdmin the admin permission byte
const PermissionSysAdmin = 255

// PermissionSysAdmin the system admin permission byte
const PermissionAdmin = 254

// Non-admin permissions
const (
	PermissionNone = iota
	PermissionR
	PermissionW
	PermissionRW
)

var methodsPermissions = map[string][]byte{
	// readwrite methods
	"/immudb.schema.ImmuService/Set":           {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/Get":           {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SetSV":         {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SafeSet":       {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SafeGet":       {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SafeSetSV":     {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SetBatch":      {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SetBatchSV":    {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/Reference":     {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SafeReference": {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/ZAdd":          {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/SafeZAdd":      {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/ZScan":         {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/BySafeIndex":   {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/IScan":         {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	"/immudb.schema.ImmuService/History":       {PermissionSysAdmin, PermissionAdmin, PermissionRW},
	// admin methods
	"/immudb.schema.ImmuService/ListUsers":        {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/CreateUser":       {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/ChangePassword":   {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/SetPermission":    {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/DeactivateUser":   {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/UpdateAuthConfig": {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/UpdateMTLSConfig": {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/CreateDatabase":   {PermissionSysAdmin, PermissionAdmin},
	"/immudb.schema.ImmuService/Dump":             {PermissionSysAdmin, PermissionAdmin},
}

func hasPermissionForMethod(userPermission byte, method string) bool {
	methodPermission, ok := methodsPermissions[method]
	if !ok {
		return false
	}
	return bytes.Contains(methodPermission, []byte{userPermission})
}
