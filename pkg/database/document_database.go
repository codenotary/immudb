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
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

var (
	schemaToValueType = map[schemav2.IndexType]sql.SQLValueType{
		schemav2.IndexType_DOUBLE:  sql.Float64Type,
		schemav2.IndexType_STRING:  sql.VarcharType,
		schemav2.IndexType_INTEGER: sql.IntegerType,
	}
)

// DocumentDatabase is the interface for document database
type DocumentDatabase interface {
	// GetCollection returns the collection schema
	GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionGetResponse, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionCreateResponse, error)
	// ListCollections returns the list of collection schemas
	ListCollections(ctx context.Context, req *schemav2.CollectionListRequest) (*schemav2.CollectionListResponse, error)
	// UpdateCollection updates an existing collection
	UpdateCollection(ctx context.Context, req *schemav2.CollectionUpdateRequest) (*schemav2.CollectionUpdateResponse, error)
	// DeleteCollection deletes a collection
	DeleteCollection(ctx context.Context, req *schemav2.CollectionDeleteRequest) (*schemav2.CollectionDeleteResponse, error)
	// GetDocument returns the document
	SearchDocuments(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error)
	// InsertDocument creates a new document
	InsertDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error)
	// DocumentAudit returns the document audit history
	DocumentAudit(ctx context.Context, req *schemav2.DocumentAuditRequest) (*schemav2.DocumentAuditResponse, error)
	// UpdateDocument updates a document
	UpdateDocument(ctx context.Context, req *schemav2.DocumentUpdateRequest) (*schemav2.DocumentUpdateResponse, error)
	// DocumentProof returns the proofs for a document
	DocumentProof(ctx context.Context, req *schemav2.DocumentProofRequest) (*schemav2.DocumentProofResponse, error)
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionCreateResponse, error) {
	indexKeys := make(map[string]sql.SQLValueType)

	// validate index keys
	for name, pk := range req.IndexKeys {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return nil, fmt.Errorf("invalid index key type: %v", pk)
		}
		indexKeys[name] = schType
	}

	err := d.documentEngine.CreateCollection(ctx, req.Name, indexKeys)
	if err != nil {
		return nil, err
	}

	// get collection information
	cinfo, err := d.getCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &schemav2.CollectionCreateResponse{Collection: cinfo}, nil
}

func (d *db) ListCollections(ctx context.Context, req *schemav2.CollectionListRequest) (*schemav2.CollectionListResponse, error) {
	collections, err := d.documentEngine.ListCollections(ctx)
	if err != nil {
		return nil, err
	}

	cinfos := make([]*schemav2.CollectionInformation, 0, len(collections))
	for collectionName, indexes := range collections {
		cinfos = append(cinfos, newCollectionInformation(collectionName, indexes))
	}

	return &schemav2.CollectionListResponse{Collections: cinfos}, nil
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionGetResponse, error) {
	cinfo, err := d.getCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &schemav2.CollectionGetResponse{Collection: cinfo}, nil
}

func (d *db) getCollection(ctx context.Context, collectionName string) (*schemav2.CollectionInformation, error) {
	indexes, err := d.documentEngine.GetCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	return newCollectionInformation(collectionName, indexes), nil
}

// helper function to create a collection information
func newCollectionInformation(collectionName string, indexes []*sql.Index) *schemav2.CollectionInformation {
	cinfo := &schemav2.CollectionInformation{
		Name:      collectionName,
		IndexKeys: make(map[string]*schemav2.IndexOption),
	}

	// iterate over indexes and extract primary and index keys
	for _, idx := range indexes {
		for _, col := range idx.Cols() {
			var colType schemav2.IndexType
			switch col.Type() {
			case sql.VarcharType:
				colType = schemav2.IndexType_STRING
			case sql.IntegerType:
				colType = schemav2.IndexType_INTEGER
			case sql.BLOBType:
				colType = schemav2.IndexType_STRING
			}

			cinfo.IndexKeys[col.Name()] = &schemav2.IndexOption{
				Type: colType,
			}

		}
	}

	return cinfo
}

// UpdateCollection updates an existing collection
func (d *db) UpdateCollection(ctx context.Context, req *schemav2.CollectionUpdateRequest) (*schemav2.CollectionUpdateResponse, error) {
	indexKeys := make(map[string]sql.SQLValueType)

	// validate index keys
	for name, pk := range req.AddIndexes {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return nil, fmt.Errorf("invalid index key type: %v", pk)
		}
		indexKeys[name] = schType
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

	return &schemav2.CollectionUpdateResponse{Collection: cinfo}, nil
}

// DeleteCollection deletes a collection
func (d *db) DeleteCollection(ctx context.Context, req *schemav2.CollectionDeleteRequest) (*schemav2.CollectionDeleteResponse, error) {
	err := d.documentEngine.DeleteCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &schemav2.CollectionDeleteResponse{}, nil
}

// InsertDocument creates a new document
func (d *db) InsertDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error) {
	docID, txID, err := d.documentEngine.InsertDocument(ctx, req.Collection, req.Document)
	if err != nil {
		return nil, err
	}

	return &schemav2.DocumentInsertResponse{
		DocumentId:    docID.EncodeToHexString(),
		TransactionId: txID,
	}, nil
}

// SearchDocuments returns the documents matching the search request constraints
func (d *db) SearchDocuments(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
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

	results, err := d.documentEngine.GetDocuments(ctx, req.Collection, queries, int(req.Page), int(req.PerPage))
	if err != nil {
		return nil, err
	}
	return &schemav2.DocumentSearchResponse{Results: results}, nil
}

func (d *db) DocumentAudit(ctx context.Context, req *schemav2.DocumentAuditRequest) (*schemav2.DocumentAuditResponse, error) {
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

	resp := &schemav2.DocumentAuditResponse{
		Results: make([]*schemav2.DocumentAudit, 0, len(historyLogs)),
	}

	for _, log := range historyLogs {
		resp.Results = append(resp.Results, &schemav2.DocumentAudit{
			TransactionId: log.TxID,
			Revision:      log.Revision,
			Value: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					document.DocumentBLOBField: {
						Kind: &structpb.Value_StringValue{StringValue: string(log.Value)},
					},
				},
			},
		})
	}

	return resp, nil
}

// UpdateDocument updates a document
func (d *db) UpdateDocument(ctx context.Context, req *schemav2.DocumentUpdateRequest) (*schemav2.DocumentUpdateResponse, error) {
	txID, rev, err := d.documentEngine.UpdateDocument(ctx, req.Collection, req.Document)
	if err != nil {
		return nil, err
	}

	return &schemav2.DocumentUpdateResponse{
		TransactionId: txID,
		Revision:      rev,
	}, nil
}

// DocumentProof returns the proofs for a documenta
func (d *db) DocumentProof(ctx context.Context, req *schemav2.DocumentProofRequest) (*schemav2.DocumentProofResponse, error) {
	docID, err := document.NewDocumentIDFromHexEncodedString(req.DocumentId)
	if err != nil {
		return nil, fmt.Errorf("invalid document id: %v", err)
	}

	tx, err := d.allocTx()
	if err != nil {
		return nil, err
	}
	defer d.releaseTx(tx)

	collectionID, docAudit, err := d.documentEngine.GetDocument(ctx, req.Collection, docID, req.TransactionId)
	if err != nil {
		return nil, err
	}

	err = d.st.ReadTx(docAudit.TxID, false, tx)
	if err != nil {
		return nil, err
	}

	var sourceHdr, targetHdr *store.TxHeader

	if req.LastValidatedTransactionId == 0 {
		req.LastValidatedTransactionId = 1
	}

	lastValidatedHdr, err := d.st.ReadTxHeader(req.LastValidatedTransactionId, false, false)
	if err != nil {
		return nil, err
	}

	if tx.Header().ID < req.LastValidatedTransactionId {
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

	return &schemav2.DocumentProofResponse{
		Database:        d.name,
		CollectionId:    collectionID,
		RawEncodedValue: docAudit.Value,
		VerifiableTx: &schema.VerifiableTxV2{
			Tx:        schema.TxToProto(tx),
			DualProof: schema.DualProofV2ToProto(dualProof),
		},
	}, nil
}
