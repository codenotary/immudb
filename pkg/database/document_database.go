package database

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/document"
	"github.com/codenotary/immudb/embedded/sql"
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
)

var (
	schemaToValueType = map[schemav2.IndexType]sql.SQLValueType{
		schemav2.IndexType_STRING:  sql.VarcharType,
		schemav2.IndexType_INTEGER: sql.IntegerType,
	}
)

// ObjectDatabase is the interface for object database
type ObjectDatabase interface {
	// GetCollection returns the collection schema
	GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionGetResponse, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionCreateResponse, error)
	// ListCollections returns the list of collection schemas
	ListCollections(ctx context.Context, req *schemav2.CollectionListRequest) (*schemav2.CollectionListResponse, error)
	// GetDocument returns the document
	GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error)
	// CreateDocument creates a new document
	CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error)
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

func (d *db) getCollection(ctx context.Context, collectionName string) (*schemav2.CollectionInformation, error) {
	indexes, err := d.documentEngine.GetCollection(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	return newCollectionInformation(collectionName, indexes), nil
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionGetResponse, error) {
	cinfo, err := d.getCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return &schemav2.CollectionGetResponse{Collection: cinfo}, nil
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) (*schemav2.CollectionCreateResponse, error) {
	// validate collection to contain at least one primary key
	if len(req.PrimaryKeys) == 0 {
		return nil, fmt.Errorf("collection must have at least one primary key")
	}

	primaryKeys := make(map[string]sql.SQLValueType)
	indexKeys := make(map[string]sql.SQLValueType)

	// validate primary keys
	for name, pk := range req.PrimaryKeys {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return nil, fmt.Errorf("invalid primary key type: %v", pk)
		}
		// TODO: add support for other types
		// TODO: add support for auto increment
		primaryKeys[name] = schType
	}

	// validate index keys
	for name, pk := range req.IndexKeys {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return nil, fmt.Errorf("invalid index key type: %v", pk)
		}
		// TODO: add support for other types
		// TODO: add support for auto increment
		indexKeys[name] = schType
	}

	err := d.documentEngine.CreateCollection(ctx, req.Name, primaryKeys, indexKeys)
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

// CreateDocument creates a new document
func (d *db) CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error) {
	return nil, d.documentEngine.CreateDocument(ctx, req.Collection, req.Document)
}

// GetDocument returns the document
func (d *db) GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	queries := make([]*document.Query, 0, len(req.Query))
	for _, q := range req.Query {
		queries = append(queries, &document.Query{
			Operator: int(q.Operator),
			Field:    q.Field,
			Value:    q.Value,
		})
	}
	results, err := d.documentEngine.GetDocument(ctx, req.Collection, queries, d.maxResultSize)
	if err != nil {
		return nil, err
	}
	return &schemav2.DocumentSearchResponse{Results: results}, nil
}

// helper function to create a collection information
func newCollectionInformation(collectionName string, indexes []*sql.Index) *schemav2.CollectionInformation {
	cinfo := &schemav2.CollectionInformation{
		Name:        collectionName,
		PrimaryKeys: make(map[string]*schemav2.IndexOption),
		IndexKeys:   make(map[string]*schemav2.IndexOption),
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

			// check if primary key
			if idx.IsPrimary() {
				cinfo.PrimaryKeys[col.Name()] = &schemav2.IndexOption{
					Type: colType,
				}
			} else {
				cinfo.IndexKeys[col.Name()] = &schemav2.IndexOption{
					Type: colType,
				}
			}
		}
	}

	return cinfo
}
