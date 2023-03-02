package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/api/schemav2"
	"github.com/golang/protobuf/ptypes/empty"
)

func (s *ImmuServer) LoginV2(ctx context.Context, loginReq *schemav2.LoginRequest) (*schemav2.LoginResponseV2, error) {
	return nil, nil
}

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schema.VerifiableTx, error) {
	return nil, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionInformation, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionInformation, error) {
	return &schemav2.CollectionInformation{
		Name: "test",
	}, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *schemav2.CollectionListRequest) (*schemav2.CollectionListResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *schemav2.CollectionDeleteRequest) (*empty.Empty, error) {
	return nil, nil
}

func (s *ImmuServer) DocumentAudit(ctx context.Context, req *schemav2.DocumentAuditRequest) (*schemav2.DocumentAuditResponse, error) {
	return nil, nil
}

func (s *ImmuServer) DocumentProof(ctx context.Context, req *schemav2.DocumentProofRequest) (*schema.VerifiableTx, error) {
	return nil, nil
}
