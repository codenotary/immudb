/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

	u, err := s.getValidatedUser(ctx, r.Username, r.Password)
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
