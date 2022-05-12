package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *ImmuServer) OpenSession(ctx context.Context, r *schema.OpenSessionRequest) (*schema.OpenSessionResponse, error) {
	if r == nil {
		return nil, ErrIllegalArguments
	}
	if !s.Options.auth {
		return nil, errors.New(ErrAuthDisabled).WithCode(errors.CodProtocolViolation)
	}

	u, err := s.getValidatedUser(r.Username, r.Password)
	if err != nil {
		return nil, errors.Wrap(err, ErrInvalidUsernameOrPassword)
	}
	if u.Username == auth.SysAdminUsername {
		u.IsSysAdmin = true
	}

	if !u.Active {
		return nil, errors.New(ErrUserNotActive)
	}

	db := s.sysDB
	if r.DatabaseName != SystemDBName {
		db, err = s.dbList.GetByName(r.DatabaseName)
		if err != nil {
			return nil, err
		}
	}

	if (!u.IsSysAdmin) &&
		(!u.HasPermission(r.DatabaseName, auth.PermissionAdmin)) &&
		(!u.HasPermission(r.DatabaseName, auth.PermissionR)) &&
		(!u.HasPermission(r.DatabaseName, auth.PermissionRW)) {

		return nil, status.Errorf(codes.PermissionDenied, "Logged in user does not have permission on this database")
	}

	session, err := s.SessManager.NewSession(u, db)
	if err != nil {
		return nil, err
	}

	return &schema.OpenSessionResponse{
		SessionID:  session.GetID(),
		ServerUUID: s.UUID.String(),
	}, nil
}

func (s *ImmuServer) CloseSession(ctx context.Context, _ *empty.Empty) (*empty.Empty, error) {
	if !s.Options.auth {
		return nil, errors.New(ErrAuthDisabled).WithCode(errors.CodProtocolViolation)
	}
	sessionID, err := sessions.GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	err = s.SessManager.DeleteSession(sessionID)
	if err != nil {
		return nil, err
	}
	s.Logger.Debugf("closing session %s", sessionID)
	return new(empty.Empty), nil
}
