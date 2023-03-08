package server

import (
	"context"
	"math"
	"time"

	"github.com/codenotary/immudb/pkg/api/authorizationschema"
	"github.com/codenotary/immudb/pkg/api/schema"
)

const infinity = time.Duration(math.MaxInt64)

func (s *ImmuServer) OpenSessionV2(ctx context.Context, loginReq *authorizationschema.OpenSessionRequestV2) (*authorizationschema.OpenSessionResponseV2, error) {

	username := []byte(loginReq.Username)
	password := []byte(loginReq.Password)
	session, err := s.OpenSession(ctx, &schema.OpenSessionRequest{
		Username:     username,
		Password:     password,
		DatabaseName: loginReq.Database,
	})
	if err != nil {
		return nil, err
	}
	expirationTimestamp := int32(0)
	inactivityTimestamp := int32(0)
	now := time.Now()
	if s.Options.SessionsOptions.MaxSessionInactivityTime > 0 {
		inactivityTimestamp = int32(now.Add(s.Options.SessionsOptions.MaxSessionInactivityTime).Unix())
	}

	if s.Options.SessionsOptions.MaxSessionAgeTime > 0 && s.Options.SessionsOptions.MaxSessionAgeTime != infinity {
		expirationTimestamp = int32(now.Add(s.Options.SessionsOptions.MaxSessionAgeTime).Unix())
	}
	return &authorizationschema.OpenSessionResponseV2{
		Token:               session.SessionID,
		ServerUUID:          session.ServerUUID,
		ExpirationTimestamp: expirationTimestamp,
		InactivityTimestamp: inactivityTimestamp,
	}, nil
}
