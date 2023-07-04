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
	"fmt"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/rs/xid"
)

func (s *ImmuServer) CreateCollection(ctx context.Context, req *protomodel.CreateCollectionRequest) (*protomodel.CreateCollectionResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CreateCollection")
	if err != nil {
		return nil, err
	}

	return db.CreateCollection(ctx, req)
}

func (s *ImmuServer) UpdateCollection(ctx context.Context, req *protomodel.UpdateCollectionRequest) (*protomodel.UpdateCollectionResponse, error) {
	db, err := s.getDBFromCtx(ctx, "UpdateCollection")
	if err != nil {
		return nil, err
	}

	return db.UpdateCollection(ctx, req)
}

func (s *ImmuServer) GetCollection(ctx context.Context, req *protomodel.GetCollectionRequest) (*protomodel.GetCollectionResponse, error) {
	db, err := s.getDBFromCtx(ctx, "GetCollection")
	if err != nil {
		return nil, err
	}

	return db.GetCollection(ctx, req)
}

func (s *ImmuServer) GetCollections(ctx context.Context, req *protomodel.GetCollectionsRequest) (*protomodel.GetCollectionsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "GetCollections")
	if err != nil {
		return nil, err
	}

	return db.GetCollections(ctx, req)
}

func (s *ImmuServer) DeleteCollection(ctx context.Context, req *protomodel.DeleteCollectionRequest) (*protomodel.DeleteCollectionResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DeleteCollection")
	if err != nil {
		return nil, err
	}

	return db.DeleteCollection(ctx, req)
}

func (s *ImmuServer) CreateIndex(ctx context.Context, req *protomodel.CreateIndexRequest) (*protomodel.CreateIndexResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CreateIndex")
	if err != nil {
		return nil, err
	}

	return db.CreateIndex(ctx, req)
}

func (s *ImmuServer) DeleteIndex(ctx context.Context, req *protomodel.DeleteIndexRequest) (*protomodel.DeleteIndexResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DeleteIndex")
	if err != nil {
		return nil, err
	}

	return db.DeleteIndex(ctx, req)
}

func (s *ImmuServer) InsertDocuments(ctx context.Context, req *protomodel.InsertDocumentsRequest) (*protomodel.InsertDocumentsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "InsertDocuments")
	if err != nil {
		return nil, err
	}

	return db.InsertDocuments(ctx, req)
}

func (s *ImmuServer) ReplaceDocuments(ctx context.Context, req *protomodel.ReplaceDocumentsRequest) (*protomodel.ReplaceDocumentsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "ReplaceDocuments")
	if err != nil {
		return nil, err
	}

	return db.ReplaceDocuments(ctx, req)
}

func (s *ImmuServer) AuditDocument(ctx context.Context, req *protomodel.AuditDocumentRequest) (*protomodel.AuditDocumentResponse, error) {
	db, err := s.getDBFromCtx(ctx, "AuditDocument")
	if err != nil {
		return nil, err
	}

	return db.AuditDocument(ctx, req)
}

func (s *ImmuServer) SearchDocuments(ctx context.Context, req *protomodel.SearchDocumentsRequest) (*protomodel.SearchDocumentsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "SearchDocuments")
	if err != nil {
		return nil, err
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	if req.SearchId != "" && req.Query != nil {
		return nil, fmt.Errorf("%w: query or searchId must be specified, not both", ErrIllegalArguments)
	}

	if req.Page < 1 || req.PageSize < 1 {
		return nil, fmt.Errorf("%w: invalid page or page size", ErrIllegalArguments)
	}

	if int(req.PageSize) > db.MaxResultSize() {
		return nil, fmt.Errorf("%w: the specified page size (%d) is larger than the maximum allowed one (%d)",
			database.ErrResultSizeLimitExceeded, req.PageSize, db.MaxResultSize())
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

	searchID := req.SearchId
	query := req.Query

	var pgreader *sessions.PaginatedDocumentReader

	if searchID == "" {
		searchID = xid.New().String()
	} else {
		var err error

		if pgreader, err = sess.GetDocumentReader(searchID); err != nil {
			// invalid SearchId, return error
			return nil, err
		}

		// paginated reader already exists, resume reading from the correct offset based
		// on pagination parameters, do validation on the pagination parameters
		if req.Page != pgreader.LastPageNumber+1 || req.PageSize != pgreader.LastPageSize {
			if pgreader.Reader != nil {
				err := pgreader.Reader.Close()
				if err != nil {
					s.Logger.Errorf("error closing paginated reader: %s, err = %v", searchID, err)
				}
			}

			query = pgreader.Query
			pgreader = nil
		}
	}

	if pgreader == nil {
		// create a new reader and add it to the session
		offset := int64((req.Page - 1) * req.PageSize)

		docReader, err := db.SearchDocuments(ctx, query, offset)
		if err != nil {
			return nil, err
		}

		// store the reader in the session for future use
		pgreader = &sessions.PaginatedDocumentReader{
			Reader:         docReader,
			Query:          query,
			LastPageNumber: req.Page,
			LastPageSize:   req.PageSize,
		}

		sess.SetPaginatedDocumentReader(searchID, pgreader)
	}

	// read the next page of data from the paginated reader
	docs, err := pgreader.Reader.ReadN(ctx, int(req.PageSize))
	if err != nil && !errors.Is(err, document.ErrNoMoreDocuments) {
		return nil, err
	}

	if errors.Is(err, document.ErrNoMoreDocuments) || !req.KeepOpen {
		// end of data reached, remove the paginated reader and pagination parameters from the session
		err = sess.DeleteDocumentReader(searchID)
		if err != nil {
			s.Logger.Errorf("error deleting paginated reader: %s, err = %v", searchID, err)
		}

		return &protomodel.SearchDocumentsResponse{
			Revisions: docs,
		}, nil
	}

	// update the pagination parameters for this query in the session
	sess.UpdatePaginatedDocumentReader(searchID, req.Page, req.PageSize)

	return &protomodel.SearchDocumentsResponse{
		SearchId:  searchID,
		Revisions: docs,
	}, nil
}

func (s *ImmuServer) CountDocuments(ctx context.Context, req *protomodel.CountDocumentsRequest) (*protomodel.CountDocumentsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "CountDocuments")
	if err != nil {
		return nil, err
	}

	return db.CountDocuments(ctx, req)
}

func (s *ImmuServer) DeleteDocuments(ctx context.Context, req *protomodel.DeleteDocumentsRequest) (*protomodel.DeleteDocumentsResponse, error) {
	db, err := s.getDBFromCtx(ctx, "DeleteDocuments")
	if err != nil {
		return nil, err
	}

	return db.DeleteDocuments(ctx, req)
}

func (s *ImmuServer) ProofDocument(ctx context.Context, req *protomodel.ProofDocumentRequest) (*protomodel.ProofDocumentResponse, error) {
	db, err := s.getDBFromCtx(ctx, "ProofDocument")
	if err != nil {
		return nil, err
	}

	res, err := db.ProofDocument(ctx, req)
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
