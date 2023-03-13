package database

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
)

// ObjectDatabase is the interface for object database
type ObjectDatabase interface {
	// GetCollection returns the collection schema
	GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionInformation, error)
	// CreateCollection creates a new collection
	CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) error

	// GetDocument returns the document
	GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error)
	// CreateDocument creates a new document
	CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error)
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionInformation, error) {
	indexes, err := d.documentEngine.GetCollection(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	resp := &schemav2.CollectionInformation{
		Name:        req.Name,
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
				resp.PrimaryKeys[col.Name()] = &schemav2.IndexOption{Type: colType}
			} else {
				resp.IndexKeys[col.Name()] = &schemav2.IndexOption{Type: colType}
			}
		}
	}

	return resp, nil
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) error {
	return d.documentEngine.CreateCollection(ctx, req.Name, req.PrimaryKeys, req.IndexKeys)
}

// CreateDocument creates a new document
func (d *db) CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (*schemav2.DocumentInsertResponse, error) {
	return nil, d.documentEngine.CreateDocument(ctx, req.Collection, req.Document)
}

// GetDocument returns the document
func (d *db) GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	results, err := d.documentEngine.GetDocument(ctx, d.name, req.Collection, req.Query, d.maxResultSize)
	if err != nil {
		return nil, err
	}
	return &schemav2.DocumentSearchResponse{Results: results}, nil
}
