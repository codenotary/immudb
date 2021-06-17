package server

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrIllegalArguments          = status.Error(codes.InvalidArgument, database.ErrIllegalArguments.Error())
	ErrIllegalState              = status.Error(codes.InvalidArgument, database.ErrIllegalState.Error())
	ErrEmptyAdminPassword        = status.Error(codes.InvalidArgument, "Admin password cannot be empty")
	ErrUserNotActive             = "user is not active"
	ErrInvalidUsernameOrPassword = "invalid user name or password"
	ErrAuthDisabled              = "server is running with authentication disabled, please enable authentication to login"
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
