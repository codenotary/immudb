package database

import (
	"context"

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
	CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (string, error)
}

// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (resp *schemav2.CollectionInformation, err error) {
	return d.documentEngine.GetCollection(ctx, req.Name)
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) error {
	return d.documentEngine.CreateCollection(ctx, req.Name, req.PrimaryKeys, req.IndexKeys)
}

// CreateDocument creates a new document
func (d *db) CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (string, error) {
	return d.documentEngine.CreateDocument(ctx, req.Collection, req.Document)
}

// GetDocument returns the document
func (d *db) GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	return d.documentEngine.GetDocument(ctx, req.Collection, req.Query, d.maxResultSize)
}
