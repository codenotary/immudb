package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/documentschema"
)

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *documentschema.DocumentInsertRequest) (*documentschema.DocumentInsertResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentInsert")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *documentschema.DocumentSearchRequest) (*documentschema.DocumentSearchResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentSearch")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *documentschema.CollectionCreateRequest) (*documentschema.CollectionCreateResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionCreate")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *documentschema.CollectionGetRequest) (*documentschema.CollectionGetResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionGet")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *documentschema.CollectionListRequest) (*documentschema.CollectionListResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionList")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *documentschema.CollectionDeleteRequest) (*documentschema.CollectionDeleteResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionDelete")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) DocumentAudit(ctx context.Context, req *documentschema.DocumentAuditRequest) (*documentschema.DocumentAuditResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentAudit")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) DocumentProof(ctx context.Context, req *documentschema.DocumentProofRequest) (*documentschema.DocumentProofResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentProof")
	if err != nil {
		return nil, err
	}
	return nil, nil
}
