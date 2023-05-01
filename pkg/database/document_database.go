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
package database

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// DocumentDatabase is the interface for document database
type DocumentDatabase interface {
	// GetCollection returns the collection schema
	GetCollection(ctx context.Context, req *protomodel.CollectionGetRequest) (*protomodel.CollectionGetResponse, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *protomodel.CollectionCreateRequest) (*protomodel.CollectionCreateResponse, error)
	// ListCollections returns the list of collection schemas
	ListCollections(ctx context.Context, req *protomodel.CollectionListRequest) (*protomodel.CollectionListResponse, error)
	// UpdateCollection updates an existing collection
	UpdateCollection(ctx context.Context, req *protomodel.CollectionUpdateRequest) (*protomodel.CollectionUpdateResponse, error)
	// DeleteCollection deletes a collection
	DeleteCollection(ctx context.Context, req *protomodel.CollectionDeleteRequest) (*protomodel.CollectionDeleteResponse, error)
	// CreateIndexes creates indexes for a collection
	CreateIndexes(ctx context.Context, req *protomodel.IndexCreateRequest) (*protomodel.IndexCreateResponse, error)
	// DeleteIndexes deletes indexes from a collection
	DeleteIndexes(ctx context.Context, req *protomodel.IndexDeleteRequest) (*protomodel.IndexDeleteResponse, error)
	// GetDocument returns the document
	SearchDocuments(ctx context.Context, req *protomodel.DocumentSearchRequest) (document.DocumentReader, error)
	// InsertDocument creates a new document
	InsertDocument(ctx context.Context, req *protomodel.DocumentInsertRequest) (*protomodel.DocumentInsertResponse, error)
	// DocumentAudit returns the document audit history
	DocumentAudit(ctx context.Context, req *protomodel.DocumentAuditRequest) (*protomodel.DocumentAuditResponse, error)
	// UpdateDocument updates a document
	UpdateDocument(ctx context.Context, req *protomodel.DocumentUpdateRequest) (*protomodel.DocumentUpdateResponse, error)
	// DocumentProof returns the proofs for a document
	DocumentProof(ctx context.Context, req *protomodel.DocumentProofRequest) (*protomodel.DocumentProofResponse, error)
	// DocumentInsertMany creates a new document
	DocumentInsertMany(ctx context.Context, req *protomodel.DocumentInsertManyRequest) (*protomodel.DocumentInsertManyResponse, error)
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *protomodel.CollectionCreateRequest) (*protomodel.CollectionCreateResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.CreateCollection(ctx, req.Name, req.IdFieldName, req.Fields, req.Indexes)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionCreateResponse{}, nil
}

func (d *db) ListCollections(ctx context.Context, req *protomodel.CollectionListRequest) (*protomodel.CollectionListResponse, error) {
	collections, err := d.documentEngine.ListCollections(ctx)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionListResponse{Collections: collections}, nil
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *protomodel.CollectionGetRequest) (*protomodel.CollectionGetResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	cinfo, err := d.documentEngine.GetCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionGetResponse{Collection: cinfo}, nil
}

// SearchDocuments returns the documents matching the search request constraints
func (d *db) SearchDocuments(ctx context.Context, req *protomodel.DocumentSearchRequest) (document.DocumentReader, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	if req.Page < 1 || req.PerPage < 1 {
		return nil, fmt.Errorf("invalid offset or limit")
	}

	offset := (req.Page - 1) * req.PerPage
	if offset < 0 {
		return nil, fmt.Errorf("invalid offset")
	}

	reader, err := d.documentEngine.GetDocuments(ctx, req.Collection, req.Query, int64(offset))
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// UpdateCollection updates an existing collection
func (d *db) UpdateCollection(ctx context.Context, req *protomodel.CollectionUpdateRequest) (*protomodel.CollectionUpdateResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.UpdateCollection(ctx, req.Name, req.IdFieldName)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionUpdateResponse{}, nil
}

// DeleteCollection deletes a collection
func (d *db) DeleteCollection(ctx context.Context, req *protomodel.CollectionDeleteRequest) (*protomodel.CollectionDeleteResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.DeleteCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionDeleteResponse{}, nil
}

// CreateIndexes creates indexes for a collection
func (d *db) CreateIndexes(ctx context.Context, req *protomodel.IndexCreateRequest) (*protomodel.IndexCreateResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.CreateIndexes(ctx, req.Collection, req.Indexes)
	if err != nil {
		return nil, err
	}

	return &protomodel.IndexCreateResponse{}, nil
}

// DeleteIndexes deletes indexes from a collection
func (d *db) DeleteIndexes(ctx context.Context, req *protomodel.IndexDeleteRequest) (*protomodel.IndexDeleteResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.DeleteIndexes(ctx, req.Collection, req.Indexes)
	if err != nil {
		return nil, err
	}

	return &protomodel.IndexDeleteResponse{}, nil
}

// InsertDocument creates a new document
func (d *db) InsertDocument(ctx context.Context, req *protomodel.DocumentInsertRequest) (*protomodel.DocumentInsertResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	txID, docID, err := d.documentEngine.InsertDocument(ctx, req.Collection, req.Document)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentInsertResponse{
		TransactionId: txID,
		DocumentId:    docID.EncodeToHexString(),
	}, nil
}

// DocumentInsertMany inserts multiple documents
func (d *db) DocumentInsertMany(ctx context.Context, req *protomodel.DocumentInsertManyRequest) (*protomodel.DocumentInsertManyResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	txID, docIDs, err := d.documentEngine.BulkInsertDocuments(ctx, req.Collection, req.Documents)
	if err != nil {
		return nil, err
	}

	docIDsStr := make([]string, 0, len(docIDs))
	for _, docID := range docIDs {
		docIDsStr = append(docIDsStr, docID.EncodeToHexString())
	}

	return &protomodel.DocumentInsertManyResponse{
		TransactionId: txID,
		DocumentIds:   docIDsStr,
	}, nil
}

// UpdateDocument updates a document
func (d *db) UpdateDocument(ctx context.Context, req *protomodel.DocumentUpdateRequest) (*protomodel.DocumentUpdateResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	txID, docID, rev, err := d.documentEngine.UpdateDocument(ctx, req.Collection, req.Query, req.Document)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentUpdateResponse{
		TransactionId: txID,
		DocumentId:    docID.EncodeToHexString(),
		Revision:      rev,
	}, nil
}

func (d *db) DocumentAudit(ctx context.Context, req *protomodel.DocumentAuditRequest) (*protomodel.DocumentAuditResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	// verify if document id is valid
	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("invalid document id: %v", err)
	}

	if req.Page < 1 || req.PerPage < 1 {
		return nil, fmt.Errorf("invalid offset or limit")
	}

	revisions, err := d.documentEngine.DocumentAudit(ctx, req.Collection, docID, int(req.Page), int(req.PerPage))
	if err != nil {
		return nil, fmt.Errorf("error fetching document history: %v", err)
	}

	return &protomodel.DocumentAuditResponse{
		Revisions: revisions,
	}, nil
}

// DocumentProof returns the proofs for a documenta
func (d *db) DocumentProof(ctx context.Context, req *protomodel.DocumentProofRequest) (*protomodel.DocumentProofResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("invalid document id: %v", err)
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	collectionID, idFieldName, docAudit, err := d.documentEngine.GetEncodedDocument(ctx, req.Collection, docID, req.TransactionId)
	if err != nil {
		return nil, err
	}

	err = d.st.ReadTx(docAudit.TxID, false, tx)
	if err != nil {
		return nil, err
	}

	var sourceHdr, targetHdr *store.TxHeader

	if req.ProofSinceTransactionId == 0 {
		req.ProofSinceTransactionId = 1
	}

	lastValidatedHdr, err := d.st.ReadTxHeader(req.ProofSinceTransactionId, false, false)
	if err != nil {
		return nil, err
	}

	if tx.Header().ID < req.ProofSinceTransactionId {
		sourceHdr = tx.Header()
		targetHdr = lastValidatedHdr
	} else {
		sourceHdr = lastValidatedHdr
		targetHdr = tx.Header()
	}

	dualProof, err := d.st.DualProofV2(sourceHdr, targetHdr)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentProofResponse{
		Database:        d.name,
		CollectionId:    collectionID,
		IdFieldName:     idFieldName,
		EncodedDocument: docAudit.EncodedDocument,
		VerifiableTx: &schema.VerifiableTxV2{
			Tx:        schema.TxToProto(tx),
			DualProof: schema.DualProofV2ToProto(dualProof),
		},
	}, nil
}
