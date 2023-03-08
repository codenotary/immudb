package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/documentsschema"
)

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *documentsschema.DocumentInsertRequest) (*documentsschema.DocumentInsertResponse, error) {
	return nil, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *documentsschema.DocumentSearchRequest) (*documentsschema.DocumentSearchResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *documentsschema.CollectionCreateRequest) (*documentsschema.CollectionCreateResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *documentsschema.CollectionGetRequest) (*documentsschema.CollectionGetResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *documentsschema.CollectionListRequest) (*documentsschema.CollectionListResponse, error) {
	return nil, nil
}

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *documentsschema.CollectionDeleteRequest) (*documentsschema.CollectionDeleteResponse, error) {
	return nil, nil
}
