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
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var IsAdminUser func(ctx context.Context, username []byte) (bool, error)
var AdminUserExists func(ctx context.Context) (bool, error)
var CreateAdminUser func(ctx context.Context) (string, string, error)

var ErrServerAuthDisabled = status.Error(
	codes.Unavailable, "authentication is disabled on server")

type ErrFirstAdminLogin struct {
	message string
}

func (e *ErrFirstAdminLogin) Error() string {
	return e.message
}

func (e *ErrFirstAdminLogin) With(username string, password string) *ErrFirstAdminLogin {
	e.message = fmt.Sprintf(
		"FirstAdminLogin\n---\nusername: %s\npassword: %s\n---\n",
		username,
		password,
	)
	return e
}

func (e *ErrFirstAdminLogin) Matches(err error) (string, bool) {
	errMsg := err.Error()
	grpcErrPieces := strings.Split(errMsg, "desc =")
	if len(grpcErrPieces) > 1 {
		errMsg = strings.TrimSpace(strings.Join(grpcErrPieces[1:], ""))
	}
	return strings.TrimPrefix(errMsg, "FirstAdminLogin"),
		strings.Index(errMsg, "FirstAdminLogin") == 0
}

const ClientIDMetadataKey = "client_id"
const ClientIDMetadataValueAdmin = "immuadmin"

func IsAdminClient(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	clientIDMD := md[ClientIDMetadataKey]
	return len(clientIDMD) > 0 && clientIDMD[0] == ClientIDMetadataValueAdmin
}

func createAdminUserAndMsg(ctx context.Context) (*ErrFirstAdminLogin, error) {
	username, plainPassword, err := CreateAdminUser(ctx)
	if err == nil {
		return (&ErrFirstAdminLogin{}).With(username, plainPassword), nil
	}
	return nil, err
}
