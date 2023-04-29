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
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/codenotary/immudb/pkg/api/schema"
)

var (
	schemaToValueType = map[protomodel.IndexType]sql.SQLValueType{
		protomodel.IndexType_DOUBLE:  sql.Float64Type,
		protomodel.IndexType_STRING:  sql.VarcharType,
		protomodel.IndexType_INTEGER: sql.IntegerType,
	}
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

	collection, err := d.documentEngine.CreateCollection(ctx, req.Collection)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionCreateResponse{
		Collection: collection,
	}, nil
}

func (d *db) ListCollections(ctx context.Context, req *protomodel.CollectionListRequest) (*protomodel.CollectionListResponse, error) {
	collections, err := d.documentEngine.ListCollections(ctx)
	if err != nil {
		return nil, err
	}

	cinfos := make([]*protomodel.CollectionInformation, 0, len(collections))
	for collectionName, indexes := range collections {
		cinfos = append(cinfos, newCollectionInformation(collectionName, indexes))
	}

	return &protomodel.CollectionListResponse{Collections: cinfos}, nil
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *protomodel.CollectionGetRequest) (*protomodel.CollectionGetResponse, error) {
	cinfo, err := d.getCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionGetResponse{Collection: cinfo}, nil
}

func (d *db) getCollection(ctx context.Context, collectionName string) (*protomodel.CollectionInformation, error) {
	indexes, err := d.documentEngine.GetCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	return newCollectionInformation(collectionName, indexes), nil
}

// SearchDocuments returns the documents matching the search request constraints
func (d *db) SearchDocuments(ctx context.Context, req *protomodel.DocumentSearchRequest) (document.DocumentReader, error) {
	queries := make([]*document.Query, 0, len(req.Query))
	for _, q := range req.Query {
		queries = append(queries, &document.Query{
			Operator: int(q.Operator),
			Field:    q.Field,
			Value:    q.Value,
		})
	}
	if req.Page < 1 || req.PerPage < 1 {
		return nil, fmt.Errorf("invalid offset or limit")
	}

	offset := (req.Page - 1) * req.PerPage
	if offset < 0 {
		return nil, fmt.Errorf("invalid offset")
	}

	reader, err := d.documentEngine.GetDocuments(ctx, req.Collection, queries, int64(offset))
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// helper function to create a collection information
func newCollectionInformation(collectionName string, indexes []*sql.Index) *protomodel.CollectionInformation {
	cinfo := &protomodel.CollectionInformation{
		Name:      collectionName,
		IndexKeys: make(map[string]*protomodel.IndexOption),
	}

	// iterate over indexes and extract primary and index keys
	for _, idx := range indexes {
		for _, col := range idx.Cols() {
			var colType protomodel.IndexType
			switch col.Type() {
			case sql.VarcharType:
				colType = protomodel.IndexType_STRING
			case sql.IntegerType:
				colType = protomodel.IndexType_INTEGER
			case sql.BLOBType:
				colType = protomodel.IndexType_STRING
			}

			cinfo.IndexKeys[col.Name()] = &protomodel.IndexOption{
				Type: colType,
			}

		}
	}

	return cinfo
}

// UpdateCollection updates an existing collection
func (d *db) UpdateCollection(ctx context.Context, req *protomodel.CollectionUpdateRequest) (*protomodel.CollectionUpdateResponse, error) {
	indexKeys := make(map[string]*document.IndexOption)

	// validate index keys
	for name, pk := range req.AddIndexes {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return nil, fmt.Errorf("invalid index key type: %v", pk)
		}
		indexKeys[name] = &document.IndexOption{
			Type:     schType,
			IsUnique: pk.IsUnique,
		}
	}

	err := d.documentEngine.UpdateCollection(ctx, req.Name, indexKeys, req.RemoveIndexes)
	if err != nil {
		return nil, err
	}

	// get collection information
	cinfo, err := d.getCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionUpdateResponse{Collection: cinfo}, nil
}

// DeleteCollection deletes a collection
func (d *db) DeleteCollection(ctx context.Context, req *protomodel.CollectionDeleteRequest) (*protomodel.CollectionDeleteResponse, error) {
	err := d.documentEngine.DeleteCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &protomodel.CollectionDeleteResponse{}, nil
}

// InsertDocument creates a new document
func (d *db) InsertDocument(ctx context.Context, req *protomodel.DocumentInsertRequest) (*protomodel.DocumentInsertResponse, error) {
	docID, txID, err := d.documentEngine.InsertDocument(ctx, req.Collection, req.Document)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentInsertResponse{
		DocumentId:    docID.EncodeToHexString(),
		TransactionId: txID,
	}, nil
}

func (d *db) DocumentAudit(ctx context.Context, req *protomodel.DocumentAuditRequest) (*protomodel.DocumentAuditResponse, error) {
	// verify if document id is valid
	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("invalid document id: %v", err)
	}

	if req.Page < 1 || req.PerPage < 1 {
		return nil, fmt.Errorf("invalid offset or limit")
	}

	historyLogs, err := d.documentEngine.DocumentAudit(ctx, req.Collection, docID, int(req.Page), int(req.PerPage))
	if err != nil {
		return nil, fmt.Errorf("error fetching document history: %v", err)
	}

	resp := &protomodel.DocumentAuditResponse{
		Results: make([]*protomodel.DocumentAudit, 0, len(historyLogs)),
	}

	for _, log := range historyLogs {
		resp.Results = append(resp.Results, &protomodel.DocumentAudit{
			TransactionId: log.TxID,
			Revision:      log.Revision,
			Document:      log.Document,
		})
	}

	return resp, nil
}

// UpdateDocument updates a document
func (d *db) UpdateDocument(ctx context.Context, req *protomodel.DocumentUpdateRequest) (*protomodel.DocumentUpdateResponse, error) {
	queries := make([]*document.Query, 0, len(req.Query))
	for _, q := range req.Query {
		queries = append(queries, &document.Query{
			Operator: int(q.Operator),
			Field:    q.Field,
			Value:    q.Value,
		})
	}

	txID, rev, err := d.documentEngine.UpdateDocument(ctx, req.Collection, queries, req.Document)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentUpdateResponse{
		TransactionId: txID,
		Revision:      rev,
	}, nil
}

// DocumentProof returns the proofs for a documenta
func (d *db) DocumentProof(ctx context.Context, req *protomodel.DocumentProofRequest) (*protomodel.DocumentProofResponse, error) {
	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("invalid document id: %v", err)
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	collectionID, docAudit, err := d.documentEngine.GetEncodedDocument(ctx, req.Collection, docID, req.TransactionId)
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
		EncodedDocument: docAudit.EncodedDocument,
		VerifiableTx: &schema.VerifiableTxV2{
			Tx:        schema.TxToProto(tx),
			DualProof: schema.DualProofV2ToProto(dualProof),
		},
	}, nil
}

// DocumentInsertMany inserts multiple documents
func (d *db) DocumentInsertMany(ctx context.Context, req *protomodel.DocumentInsertManyRequest) (*protomodel.DocumentInsertManyResponse, error) {
	docIDs, txID, err := d.documentEngine.BulkInsertDocuments(ctx, req.Collection, req.Documents)
	if err != nil {
		return nil, err
	}

	docIDsStr := make([]string, 0, len(docIDs))
	for _, docID := range docIDs {
		docIDsStr = append(docIDsStr, docID.EncodeToHexString())
	}

	return &protomodel.DocumentInsertManyResponse{
		DocumentIds:   docIDsStr,
		TransactionId: txID,
	}, nil
}
