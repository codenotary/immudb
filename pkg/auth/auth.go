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
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Kind uint32

const (
	KindNone Kind = iota
	KindPassword
	KindCryptoSig
)

// TODO OGG: in the future, after other types of auth will be implemented,
// this will have to be of Kind (see above) type instead of bool:
var AuthEnabled bool

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

func checkAuth(ctx context.Context, method string, req interface{}) error {
	methodRequiresAdmin := isAdminMethod(method)
	if !AuthEnabled || methodRequiresAdmin {
		if !isLocalClient(ctx) {
			var errMsg string
			if methodRequiresAdmin {
				errMsg = "server does not accept admin commands from remote clients"
			} else {
				errMsg =
					"server has authentication disabled: only local connections are accepted"
			}
			return status.Errorf(codes.PermissionDenied, errMsg)
		}
	}
	if !AuthEnabled && !methodRequiresAdmin {
		return nil
	}
	if hasAuth(method) {
		jsonToken, err := verifyTokenFromCtx(ctx)
		if err != nil {
			return err
		}
		if !hasPermissionForMethod(jsonToken.Permissions, method) {
			return status.Errorf(codes.PermissionDenied, "not enough permissions")
		}
	}
	return nil
}
