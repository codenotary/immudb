/*
Copyright 2023 Codenotary Inc. All rights reserved.

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

	"github.com/codenotary/immudb/pkg/api/documentschema"
)

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *documentschema.DocumentInsertRequest) (*documentschema.DocumentInsertResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentInsert")
	if err != nil {
		return nil, err
	}
	resp, err := db.CreateDocument(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// TODO: implement
func (s *ImmuServer) DocumentUpdate(ctx context.Context, req *documentschema.DocumentUpdateRequest) (*documentschema.DocumentUpdateResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentUpdate")
	if err != nil {
		return nil, err
	}
	panic("Not implemented")
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *documentschema.DocumentSearchRequest) (*documentschema.DocumentSearchResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentSearch")
	if err != nil {
		return nil, err
	}
	resp, err := db.GetDocument(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *documentschema.CollectionCreateRequest) (*documentschema.CollectionCreateResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CollectionCreate")
	if err != nil {
		return nil, err
	}
	resp, err := db.CreateCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *documentschema.CollectionGetRequest) (*documentschema.CollectionGetResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CollectionGet")
	if err != nil {
		return nil, err
	}
	resp, err := db.GetCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) CollectionList(ctx context.Context, req *documentschema.CollectionListRequest) (*documentschema.CollectionListResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CollectionList")
	if err != nil {
		return nil, err
	}
	resp, err := db.ListCollections(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// TODO: implement
func (s *ImmuServer) CollectionDelete(ctx context.Context, req *documentschema.CollectionDeleteRequest) (*documentschema.CollectionDeleteResponse, error) {
	_, err := s.getDBFromCtx(ctx, "CollectionDelete")
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// TODO: implement
func (s *ImmuServer) DocumentAudit(ctx context.Context, req *documentschema.DocumentAuditRequest) (*documentschema.DocumentAuditResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentAudit")
	if err != nil {
		return nil, err
	}
	resp, err := db.DocumentAudit(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// TODO: implement
func (s *ImmuServer) DocumentProof(ctx context.Context, req *documentschema.DocumentProofRequest) (*documentschema.DocumentProofResponse, error) {
	_, err := s.getDBFromCtx(ctx, "DocumentProof")
	if err != nil {
		return nil, err
	}
	return nil, nil
}
