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
	GetCollection(ctx context.Context, req *protomodel.GetCollectionRequest) (*protomodel.GetCollectionResponse, error)
	// GetCollections returns the list of collection schemas
	GetCollections(ctx context.Context, req *protomodel.GetCollectionsRequest) (*protomodel.GetCollectionsResponse, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *protomodel.CreateCollectionRequest) (*protomodel.CreateCollectionResponse, error)
	// UpdateCollection updates an existing collection
	UpdateCollection(ctx context.Context, req *protomodel.UpdateCollectionRequest) (*protomodel.UpdateCollectionResponse, error)
	// DeleteCollection deletes a collection
	DeleteCollection(ctx context.Context, req *protomodel.DeleteCollectionRequest) (*protomodel.DeleteCollectionResponse, error)
	// CreateIndex creates an index for a collection
	CreateIndex(ctx context.Context, req *protomodel.CreateIndexRequest) (*protomodel.CreateIndexResponse, error)
	// DeleteIndex deletes an index from a collection
	DeleteIndex(ctx context.Context, req *protomodel.DeleteIndexRequest) (*protomodel.DeleteIndexResponse, error)
	// InsertDocuments creates new documents
	InsertDocuments(ctx context.Context, req *protomodel.InsertDocumentsRequest) (*protomodel.InsertDocumentsResponse, error)
	// ReplaceDocuments replaces documents matching the query
	ReplaceDocuments(ctx context.Context, req *protomodel.ReplaceDocumentsRequest) (*protomodel.ReplaceDocumentsResponse, error)
	// AuditDocument returns the document audit history
	AuditDocument(ctx context.Context, req *protomodel.AuditDocumentRequest) (*protomodel.AuditDocumentResponse, error)
	// SearchDocuments returns the documents matching the query
	SearchDocuments(ctx context.Context, query *protomodel.Query, offset int64) (document.DocumentReader, error)
	// CountDocuments returns the number of documents matching the query
	CountDocuments(ctx context.Context, req *protomodel.CountDocumentsRequest) (*protomodel.CountDocumentsResponse, error)
	// DeleteDocuments deletes documents maching the query
	DeleteDocuments(ctx context.Context, req *protomodel.DeleteDocumentsRequest) (*protomodel.DeleteDocumentsResponse, error)
	// ProofDocument returns the proofs for a document
	ProofDocument(ctx context.Context, req *protomodel.ProofDocumentRequest) (*protomodel.ProofDocumentResponse, error)
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *protomodel.CreateCollectionRequest) (*protomodel.CreateCollectionResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.CreateCollection(ctx, req.Name, req.DocumentIdFieldName, req.Fields, req.Indexes)
	if err != nil {
		return nil, err
	}

	return &protomodel.CreateCollectionResponse{}, nil
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *protomodel.GetCollectionRequest) (*protomodel.GetCollectionResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	cinfo, err := d.documentEngine.GetCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.GetCollectionResponse{Collection: cinfo}, nil
}

func (d *db) GetCollections(ctx context.Context, _ *protomodel.GetCollectionsRequest) (*protomodel.GetCollectionsResponse, error) {
	collections, err := d.documentEngine.GetCollections(ctx)
	if err != nil {
		return nil, err
	}

	return &protomodel.GetCollectionsResponse{Collections: collections}, nil
}

// UpdateCollection updates an existing collection
func (d *db) UpdateCollection(ctx context.Context, req *protomodel.UpdateCollectionRequest) (*protomodel.UpdateCollectionResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.UpdateCollection(ctx, req.Name, req.DocumentIdFieldName)
	if err != nil {
		return nil, err
	}

	return &protomodel.UpdateCollectionResponse{}, nil
}

// DeleteCollection deletes a collection
func (d *db) DeleteCollection(ctx context.Context, req *protomodel.DeleteCollectionRequest) (*protomodel.DeleteCollectionResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.DeleteCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.DeleteCollectionResponse{}, nil
}

// CreateIndex creates an index for a collection
func (d *db) CreateIndex(ctx context.Context, req *protomodel.CreateIndexRequest) (*protomodel.CreateIndexResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.CreateIndex(ctx, req.CollectionName, req.Fields, req.IsUnique)
	if err != nil {
		return nil, err
	}

	return &protomodel.CreateIndexResponse{}, nil
}

// DeleteIndex deletes an index from a collection
func (d *db) DeleteIndex(ctx context.Context, req *protomodel.DeleteIndexRequest) (*protomodel.DeleteIndexResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.DeleteIndex(ctx, req.CollectionName, req.Fields)
	if err != nil {
		return nil, err
	}

	return &protomodel.DeleteIndexResponse{}, nil
}

// InsertDocuments inserts multiple documents
func (d *db) InsertDocuments(ctx context.Context, req *protomodel.InsertDocumentsRequest) (*protomodel.InsertDocumentsResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	txID, docIDs, err := d.documentEngine.InsertDocuments(ctx, req.CollectionName, req.Documents)
	if err != nil {
		return nil, err
	}

	docIDsStr := make([]string, 0, len(docIDs))
	for _, docID := range docIDs {
		docIDsStr = append(docIDsStr, docID.EncodeToHexString())
	}

	return &protomodel.InsertDocumentsResponse{
		TransactionId: txID,
		DocumentIds:   docIDsStr,
	}, nil
}

// ReplaceDocuments replaces documents matching the query
func (d *db) ReplaceDocuments(ctx context.Context, req *protomodel.ReplaceDocumentsRequest) (*protomodel.ReplaceDocumentsResponse, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	revisions, err := d.documentEngine.ReplaceDocuments(ctx, req.Query, req.Document)
	if err != nil {
		return nil, err
	}

	return &protomodel.ReplaceDocumentsResponse{
		Revisions: revisions,
	}, nil
}

func (d *db) AuditDocument(ctx context.Context, req *protomodel.AuditDocumentRequest) (*protomodel.AuditDocumentResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	if req.Page < 1 || req.PageSize < 1 {
		return nil, fmt.Errorf("%w: invalid page or page size", ErrIllegalArguments)
	}

	offset := uint64((req.Page - 1) * req.PageSize)
	limit := int(req.PageSize)

	if limit > d.maxResultSize {
		return nil, fmt.Errorf("%w: the specified page size (%d) is larger than the maximum allowed one (%d)",
			ErrIllegalArguments, limit, d.maxResultSize)
	}

	// verify if document id is valid
	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid document id", err)
	}

	revisions, err := d.documentEngine.AuditDocument(ctx, req.CollectionName, docID, req.Desc, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("%w: error fetching document history", err)
	}

	return &protomodel.AuditDocumentResponse{
		Revisions: revisions,
	}, nil
}

// SearchDocuments returns the documents matching the search request constraints
func (d *db) SearchDocuments(ctx context.Context, query *protomodel.Query, offset int64) (document.DocumentReader, error) {
	return d.documentEngine.GetDocuments(ctx, query, offset)
}

// CountDocuments returns the number of documents matching the query
func (d *db) CountDocuments(ctx context.Context, req *protomodel.CountDocumentsRequest) (*protomodel.CountDocumentsResponse, error) {
	if req == nil {
		return nil, ErrIllegalArguments
	}

	count, err := d.documentEngine.CountDocuments(ctx, req.Query, 0)
	if err != nil {
		return nil, err
	}

	return &protomodel.CountDocumentsResponse{
		Count: count,
	}, nil
}

func (d *db) DeleteDocuments(ctx context.Context, req *protomodel.DeleteDocumentsRequest) (*protomodel.DeleteDocumentsResponse, error) {
	if d.isReplica() {
		return nil, ErrIsReplica
	}

	if req == nil {
		return nil, ErrIllegalArguments
	}

	err := d.documentEngine.DeleteDocuments(ctx, req.Query)
	if err != nil {
		return nil, err
	}
	return &protomodel.DeleteDocumentsResponse{}, nil
}

// ProofDocument returns the proofs for a documenta
func (d *db) ProofDocument(ctx context.Context, req *protomodel.ProofDocumentRequest) (*protomodel.ProofDocumentResponse, error) {
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

	collectionID, documentIdFieldName, docAudit, err := d.documentEngine.GetEncodedDocument(ctx, req.CollectionName, docID, req.TransactionId)
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

	return &protomodel.ProofDocumentResponse{
		Database:            d.name,
		CollectionId:        collectionID,
		DocumentIdFieldName: documentIdFieldName,
		EncodedDocument:     docAudit.EncodedDocument,
		VerifiableTx: &schema.VerifiableTxV2{
			Tx:        schema.TxToProto(tx),
			DualProof: schema.DualProofV2ToProto(dualProof),
		},
	}, nil
}
