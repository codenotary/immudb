package server

import (
	"context"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/api/schemav2"
	"github.com/golang/protobuf/ptypes/empty"
)

func (s *ImmuServer) LoginV2(ctx context.Context, loginReq *schemav2.LoginRequest) (*schemav2.LoginResponseV2, error) {
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
	if s.Options.SessionsOptions.MaxSessionInactivityTime > 0 {
		expirationTimestamp = int32(time.Now().Add(s.Options.SessionsOptions.MaxSessionInactivityTime).Unix())
	}
	return &schemav2.LoginResponseV2{
		Token:               session.SessionID,
		ExpirationTimestamp: expirationTimestamp,
	}, nil
}

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schema.VerifiableTx, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentInsert")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentSearch")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionInformation, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionCreate")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionInformation, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionGet")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *schemav2.CollectionListRequest) (*schemav2.CollectionListResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionList")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *schemav2.CollectionDeleteRequest) (*empty.Empty, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionDelete")
	if err != nil {
		return nil, err
	}
	return nil, nil
}
