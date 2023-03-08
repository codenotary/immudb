package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/documentsschema"
)

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *documentsschema.DocumentInsertRequest) (*documentsschema.DocumentInsertResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentInsert")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *documentsschema.DocumentSearchRequest) (*documentsschema.DocumentSearchResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentSearch")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *documentsschema.CollectionCreateRequest) (*documentsschema.CollectionCreateResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionCreate")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *documentsschema.CollectionGetRequest) (*documentsschema.CollectionGetResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionGet")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *documentsschema.CollectionListRequest) (*documentsschema.CollectionListResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionList")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *documentsschema.CollectionDeleteRequest) (*documentsschema.CollectionDeleteResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionDelete")
	if err != nil {
		return nil, err
	}
	return nil, nil
}
