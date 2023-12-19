/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"math"
	"time"

	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/emptypb"
)

const infinity = time.Duration(math.MaxInt64)

type authenticationServiceImp struct {
	server *ImmuServer
}

func (s *authenticationServiceImp) OpenSession(ctx context.Context, loginReq *protomodel.OpenSessionRequest) (*protomodel.OpenSessionResponse, error) {
	session, err := s.server.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     []byte(loginReq.Username),
		Password:     []byte(loginReq.Password),
		DatabaseName: loginReq.Database,
	})
	if err != nil {
		return nil, err
	}

	expirationTimestamp := int32(0)
	inactivityTimestamp := int32(0)
	now := time.Now()

	if s.server.Options.SessionsOptions.MaxSessionInactivityTime > 0 {
		inactivityTimestamp = int32(now.Add(s.server.Options.SessionsOptions.MaxSessionInactivityTime).Unix())
	}

	if s.server.Options.SessionsOptions.MaxSessionAgeTime > 0 && s.server.Options.SessionsOptions.MaxSessionAgeTime != infinity {
		expirationTimestamp = int32(now.Add(s.server.Options.SessionsOptions.MaxSessionAgeTime).Unix())
	}

	return &protomodel.OpenSessionResponse{
		SessionID:           session.SessionID,
		ServerUUID:          session.ServerUUID,
		ExpirationTimestamp: expirationTimestamp,
		InactivityTimestamp: inactivityTimestamp,
	}, nil
}

func (s *authenticationServiceImp) KeepAlive(ctx context.Context, _ *protomodel.KeepAliveRequest) (*protomodel.KeepAliveResponse, error) {
	_, err := s.server.KeepAlive(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return &protomodel.KeepAliveResponse{}, nil
}

func (s *authenticationServiceImp) CloseSession(ctx context.Context, _ *protomodel.CloseSessionRequest) (*protomodel.CloseSessionResponse, error) {
	_, err := s.server.CloseSession(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}

	return &protomodel.CloseSessionResponse{}, nil
}
