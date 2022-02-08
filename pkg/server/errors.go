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
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/server/sessions"
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
	ErrAuthMustBeEnabled           = status.Error(codes.InvalidArgument, "authentication must be on")
	ErrAuthMustBeDisabled          = status.Error(codes.InvalidArgument, "authentication must be disabled when restoring systemdb")
	ErrNotAllowedInMaintenanceMode = status.Error(codes.InvalidArgument, "operation not allowed in maintenance mode")
	ErrReservedDatabase            = errors.New("database is reserved")
	ErrPermissionDenied            = errors.New("permission denied")
	ErrNotSupported                = errors.New("operation not supported")
	ErrNotLoggedIn                 = auth.ErrNotLoggedIn
	ErrReplicationInProgress       = errors.New("replication already in progress")
	ErrReplicatorNotNeeded         = errors.New("replicator is not needed")
	ErrReplicationNotInProgress    = errors.New("replication is not in progress")
	ErrSessionAlreadyPresent       = errors.New("session already present").WithCode(errors.CodInternalError)
	ErrSessionNotFound             = errors.New("session not found").WithCode(errors.CodSqlserverRejectedEstablishmentOfSqlSession)
	ErrOngoingReadWriteTx          = sessions.ErrOngoingReadWriteTx
	ErrNoSessionIDPresent          = errors.New("no sessionID provided")
	ErrTxNotProperlyClosed         = errors.New("tx not properly closed")
	ErrReadWriteTxNotOngoing       = errors.New("read write transaction not ongoing")
	ErrTxReadConflict              = errors.New(store.ErrTxReadConflict.Error()).WithCode(errors.CodInFailedSqlTransaction)
)

func mapServerError(err error) error {
	switch err {
	case store.ErrIllegalState:
		return ErrIllegalState
	case store.ErrIllegalArguments:
		return ErrIllegalArguments
	case store.ErrTxReadConflict:
		return ErrTxReadConflict
	}
	return err
}

func init() {
	errors.CodeMap[ErrUserNotActive] = errors.CodSqlserverRejectedEstablishmentOfSqlconnection
	errors.CodeMap[ErrInvalidUsernameOrPassword] = errors.CodSqlserverRejectedEstablishmentOfSqlconnection
	errors.CodeMap[ErrAuthDisabled] = errors.CodProtocolViolation
}
