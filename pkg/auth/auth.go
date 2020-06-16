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
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kind the authentication kind
type Kind uint32

// Authentication kinds
const (
	KindNone Kind = iota
	KindPassword
	KindCryptoSig
)

// TODO OGG: in the future, after other types of auth will be implemented,
// this will have to be of Kind (see above) type instead of bool:

// AuthEnabled toggles authentication on or off
var AuthEnabled bool

// DevMode if set to true, remote client commands (except admin ones) will be accepted even if auth is off
var DevMode bool

// WarnDefaultAdminPassword warning user message for the case when admin uses the default password
var WarnDefaultAdminPassword = "admin user has the default password: please change it to ensure proper security"

var emptyStruct = struct{}{}
var methodsWithoutAuth = map[string]struct{}{
	"/immudb.schema.ImmuService/CurrentRoot": emptyStruct,
	"/immudb.schema.ImmuService/Health":      emptyStruct,
	"/immudb.schema.ImmuService/Login":       emptyStruct,
}

func hasAuth(method string) bool {
	_, noAuth := methodsWithoutAuth[method]
	return !noAuth
}

func checkAuth(ctx context.Context, method string, req interface{}) (context.Context, error) {
	if !AuthEnabled {
		permissions := methodsPermissions[method]
		isSysAdminMethod := bytes.Contains(permissions, []byte{PermissionSysAdmin})
		isLocal := isLocalClient(ctx)
		if !isSysAdminMethod && (DevMode || isLocal) {
			return ctx, nil
		} else if !isLocal {
			return ctx, status.Errorf(
				codes.PermissionDenied,
				"server has authentication disabled: only local connections are accepted")
		}
	}
	if AuthEnabled && hasAuth(method) {
		jsonToken, err := verifyTokenFromCtx(ctx)
		if err != nil {
			return ctx, err
		}
		if !hasPermissionForMethod(jsonToken.Permissions, method) {
			return ctx, status.Errorf(codes.PermissionDenied, "not enough permissions")
		}
		ctx = context.WithValue(ctx, "userUUID", jsonToken.UserUUID)
	}
	return ctx, nil
}
