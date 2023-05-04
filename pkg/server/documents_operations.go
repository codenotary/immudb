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
	"errors"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/rs/xid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidPreviousPage = status.Errorf(codes.InvalidArgument, "cannot go back to a previous page")
)

func (s *ImmuServer) CollectionCreate(ctx context.Context, req *protomodel.CollectionCreateRequest) (*protomodel.CollectionCreateResponse, error) {
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

func (s *ImmuServer) CollectionUpdate(ctx context.Context, req *protomodel.CollectionUpdateRequest) (*protomodel.CollectionUpdateResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CollectionUpdate")
	if err != nil {
		return nil, err
	}
	resp, err := db.UpdateCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) CollectionGet(ctx context.Context, req *protomodel.CollectionGetRequest) (*protomodel.CollectionGetResponse, error) {
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

func (s *ImmuServer) CollectionList(ctx context.Context, req *protomodel.CollectionListRequest) (*protomodel.CollectionListResponse, error) {
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

func (s *ImmuServer) CollectionDelete(ctx context.Context, req *protomodel.CollectionDeleteRequest) (*protomodel.CollectionDeleteResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CollectionDelete")
	if err != nil {
		return nil, err
	}
	resp, err := db.DeleteCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) IndexCreate(ctx context.Context, req *protomodel.IndexCreateRequest) (*protomodel.IndexCreateResponse, error) {
	db, err := s.getDBFromCtx(ctx, "IndexCreate")
	if err != nil {
		return nil, err
	}
	resp, err := db.CreateIndex(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) IndexDelete(ctx context.Context, req *protomodel.IndexDeleteRequest) (*protomodel.IndexDeleteResponse, error) {
	db, err := s.getDBFromCtx(ctx, "IndexDelete")
	if err != nil {
		return nil, err
	}
	resp, err := db.DeleteIndex(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) DocumentInsert(ctx context.Context, req *protomodel.DocumentInsertRequest) (*protomodel.DocumentInsertResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentInsert")
	if err != nil {
		return nil, err
	}
	resp, err := db.InsertDocument(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) DocumentInsertMany(ctx context.Context, req *protomodel.DocumentInsertManyRequest) (*protomodel.DocumentInsertManyResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentInsertMany")
	if err != nil {
		return nil, err
	}

	resp, err := db.DocumentInsertMany(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *ImmuServer) DocumentUpdate(ctx context.Context, req *protomodel.DocumentUpdateRequest) (*protomodel.DocumentUpdateResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentUpdate")
	if err != nil {
		return nil, err
	}
	resp, err := db.UpdateDocument(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *ImmuServer) DocumentAudit(ctx context.Context, req *protomodel.DocumentAuditRequest) (*protomodel.DocumentAuditResponse, error) {
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

func (s *ImmuServer) DocumentProof(ctx context.Context, req *protomodel.DocumentProofRequest) (*protomodel.DocumentProofResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentProof")
	if err != nil {
		return nil, err
	}

	res, err := db.DocumentProof(ctx, req)
	if err != nil {
		return nil, err
	}

	if s.StateSigner != nil {
		hdr := schema.TxHeaderFromProto(res.VerifiableTx.DualProof.TargetTxHeader)
		alh := hdr.Alh()

		newState := &schema.ImmutableState{
			Db:     db.GetName(),
			TxId:   hdr.ID,
			TxHash: alh[:],
		}

		err = s.StateSigner.Sign(newState)
		if err != nil {
			return nil, err
		}

		res.VerifiableTx.Signature = newState.Signature
	}

	return res, nil
}

func (s *ImmuServer) DocumentSearch(ctx context.Context, req *protomodel.DocumentSearchRequest) (*protomodel.DocumentSearchResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentSearch")
	if err != nil {
		return nil, err
	}

	// get the session from the context
	sessionID, err := sessions.GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	sess, err := s.SessManager.GetSession(sessionID)
	if err != nil {
		return nil, err
	}

	// generate a unique query name based on the request parameters
	searchID := getSearchIDFromRequest(req)

	// check if the paginated reader for this query has already been created
	var resultReader document.DocumentReader
	var pgreader *sessions.PaginatedDocumentReader

	pgreader, err = sess.GetPaginatedDocumentReader(searchID)
	if err != nil { // paginated reader does not exist, create a new one and add it to the session
		resultReader, err = db.SearchDocuments(ctx, req)
		if err != nil {
			return nil, err
		}

		// store the reader in the session for future use
		pgreader = &sessions.PaginatedDocumentReader{
			Reader:         resultReader,
			LastPageNumber: req.Page,
			LastPageSize:   req.PerPage,
		}

		sess.SetPaginatedDocumentReader(searchID, pgreader)
	} else { // paginated reader already exists, resume reading from the correct offset based on pagination parameters
		// do validation on the pagination parameters
		if req.Page < pgreader.LastPageNumber {
			return nil, ErrInvalidPreviousPage
		}
		resultReader = pgreader.Reader
	}

	// read the next page of data from the paginated reader
	results, err := resultReader.ReadN(ctx, int(req.PerPage))
	if err != nil && errors.Is(err, document.ErrNoMoreDocuments) {
		return nil, err
	}

	// update the pagination parameters for this query in the session
	sess.UpdatePaginatedDocumentReader(searchID, req.Page, req.PerPage, int(pgreader.TotalRead)+len(results))

	if errors.Is(err, document.ErrNoMoreDocuments) {
		// end of data reached, remove the paginated reader and pagination parameters from the session
		delErr := sess.DeletePaginatedDocumentReader(searchID)
		if delErr != nil {
			s.Logger.Errorf("error deleting paginated reader: %s, err = %v", searchID, delErr)
		}
	}

	return &protomodel.DocumentSearchResponse{
		SearchID:  searchID,
		Revisions: results,
	}, err
}

func getSearchIDFromRequest(req *protomodel.DocumentSearchRequest) string {
	if req.SearchID != "" {
		return req.SearchID
	}

	return xid.New().String()
}

func (s *ImmuServer) DocumentDelete(ctx context.Context, req *protomodel.DocumentDeleteRequest) (*protomodel.DocumentDeleteResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DocumentDelete")
	if err != nil {
		return nil, err
	}
	resp, err := db.DocumentDelete(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
