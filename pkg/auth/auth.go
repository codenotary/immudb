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
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
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

const loginMethod = "/immudb.schema.ImmuService/Login"

var methodsWithoutAuth = map[string]bool{
	"/immudb.schema.ImmuService/CurrentRoot": true,
	"/immudb.schema.ImmuService/Health":      true,
	loginMethod:                              true,
}

func hasAuth(method string) bool {
	_, noAuth := methodsWithoutAuth[method]
	return !noAuth
}

func checkAuth(ctx context.Context, method string, req interface{}) error {
	fmt.Println("checkAuth", method, req)
	isAdminCLI := IsAdminClient(ctx)
	if !AuthEnabled || isAdminCLI {
		if !isLocalClient(ctx) {
			var errMsg string
			if isAdminCLI {
				errMsg = "server does not accept admin commands from remote clients"
			} else {
				errMsg =
					"server has authentication disabled: only local connections are accepted"
			}
			return status.Errorf(codes.PermissionDenied, errMsg)
		}
	}
	if !AuthEnabled && !isAdminCLI {
		return nil
	}
	if method == loginMethod && isAdminCLI {
		lReq, ok := req.(*schema.LoginRequest)
		// if it's the very first admin login attempt, generate admin user and password
		if ok && string(lReq.GetUser()) == AdminUsername && len(lReq.GetPassword()) == 0 {
			adminUserExists, err := AdminUserExists(ctx)
			if err != nil {
				return fmt.Errorf("error determining if admin user exists: %v", err)
			}
			if !adminUserExists {
				firstAdminCallMsg, err := createAdminUserAndMsg(ctx)
				if err != nil {
					return err
				}
				return firstAdminCallMsg
			}
		}
		// do not allow users other than admin to login from immuadmin CLI
		isAdmin, err := IsAdminUser(ctx, lReq.GetUser())
		if err != nil {
			return err
		}
		if !isAdmin {
			return status.Errorf(codes.PermissionDenied, "permission denied")
		}
	}
	if hasAuth(method) {
		jsonToken, err := verifyTokenFromCtx(ctx)
		if err != nil {
			return err
		}
		if !HasPermissionForMethod(jsonToken.Permissions, method) {
			return status.Errorf(codes.PermissionDenied, "not enough permissions")
		}
	}
	return nil
}
