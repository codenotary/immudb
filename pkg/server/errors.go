/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package server

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrIllegalArguments            = status.Error(codes.InvalidArgument, database.ErrIllegalArguments.Error())
	ErrIllegalState                = status.Error(codes.InvalidArgument, database.ErrIllegalState.Error())
	ErrEmptyAdminPassword          = status.Error(codes.InvalidArgument, "Admin password cannot be empty")
	ErrUserNotActive               = "user is not active"
	ErrInvalidUsernameOrPassword   = "invalid user name or password"
	ErrAuthDisabled                = "server is running with authentication disabled, please enable authentication to login"
	ErrAuthMustBeDisabled          = status.Error(codes.InvalidArgument, "authentication must be disabled when retoring systemdb")
	ErrNotAllowedInMaintenanceMode = status.Error(codes.InvalidArgument, "operation not allowed in maintenance mode")
	ErrPermissionDenied            = errors.New("permission denied")
	ErrNotSupported                = status.Error(codes.InvalidArgument, "operation not supported")
	ErrNotLoggedIn                 = errors.New("not logged in")
)

func mapServerError(err error) error {
	switch err {
	case store.ErrIllegalState:
		return ErrIllegalState
	case store.ErrIllegalArguments:
		return ErrIllegalArguments
	}
	return err
}

func init() {
	errors.CodeMap[ErrUserNotActive] = errors.CodSqlserverRejectedEstablishmentOfSqlconnection
	errors.CodeMap[ErrInvalidUsernameOrPassword] = errors.CodSqlserverRejectedEstablishmentOfSqlconnection
	errors.CodeMap[ErrAuthDisabled] = errors.CodProtocolViolation
}
