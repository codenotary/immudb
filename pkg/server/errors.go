package server

import (
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/database"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrIllegalArguments   = status.Error(codes.InvalidArgument, database.ErrIllegalArguments.Error())
	ErrIllegalState       = status.Error(codes.InvalidArgument, database.ErrIllegalState.Error())
	ErrEmptyAdminPassword = status.Error(codes.InvalidArgument, "Admin password cannot be empty")
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
