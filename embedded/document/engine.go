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
package document

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	DefaultDocumentIDField = "_id"
	DocumentBLOBField      = "_doc"
)

type Engine struct {
	sqlEngine *sql.Engine
}

type EncodedDocument struct {
	TxID            uint64
	Revision        uint64 // revision is only set when txID == 0 and info is fetch from the index
	KVMetadata      *store.KVMetadata
	EncodedDocument []byte
}

func NewEngine(store *store.ImmuStore, opts *Options) (*Engine, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	sqlOpts := sql.DefaultOptions().
		WithPrefix(opts.prefix).
		WithLazyIndexConstraintValidation(true)

	engine, err := sql.NewEngine(store, sqlOpts)
	if err != nil {
		return nil, err
	}

	return &Engine{engine}, nil
}

func (e *Engine) CreateCollection(ctx context.Context, name, documentIdFieldName string, fields []*protomodel.Field, indexes []*protomodel.Index) error {
	if documentIdFieldName == "" {
		documentIdFieldName = DefaultDocumentIDField
	}

	if documentIdFieldName == DocumentBLOBField {
		return fmt.Errorf("%w(%s)", ErrReservedFieldName, DocumentBLOBField)
	}

	// only catalog needs to be up to date
	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	columns := make([]*sql.ColSpec, 2+len(fields))

	// add primary key for document id
	columns[0] = sql.NewColSpec(documentIdFieldName, sql.BLOBType, MaxDocumentIDLength, false, true)

	// add columnn for blob, which stores the document as a whole
	columns[1] = sql.NewColSpec(DocumentBLOBField, sql.BLOBType, 0, false, false)

	for i, field := range fields {
		if field.Name == documentIdFieldName {
			return fmt.Errorf("%w(%s): should not be specified", ErrReservedFieldName, documentIdFieldName)
		}

		if field.Name == DocumentBLOBField {
			return fmt.Errorf("%w(%s): should not be specified", ErrReservedFieldName, DocumentBLOBField)
		}

		sqlType, err := protomodelValueTypeToSQLValueType(field.Type)
		if err != nil {
			return err
		}

		colLen, err := sqlValueTypeDefaultLength(sqlType)
		if err != nil {
			return err
		}

		columns[i+2] = sql.NewColSpec(field.Name, sqlType, colLen, false, false)
	}

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{sql.NewCreateTableStmt(
			name,
			false,
			columns,
			[]string{documentIdFieldName},
		)},
		nil,
	)
	if err != nil {
		return mayTranslateError(err)
	}

	var indexStmts []sql.SQLStmt

	for _, index := range indexes {
		for _, field := range index.Fields {
			if field == DocumentBLOBField {
				return fmt.Errorf("%w(%s): non-indexable field", ErrReservedFieldName, DocumentBLOBField)
			}
		}

		if len(index.Fields) == 1 && index.Fields[0] == documentIdFieldName {
			if !index.IsUnique {
				return fmt.Errorf("%w: index on id field must be unique", ErrIllegalArguments)
			}
			// idField is the primary key and so the index is automatically created
			continue
		}

		indexStmts = append(indexStmts, sql.NewCreateIndexStmt(name, index.Fields, index.IsUnique))
	}

	// add indexes to collection
	if len(indexStmts) > 0 {
		_, _, err = e.sqlEngine.ExecPreparedStmts(
			ctx,
			sqlTx,
			indexStmts,
			nil,
		)
		if err != nil {
			return mayTranslateError(err)
		}
	}

	err = sqlTx.Commit(ctx)
	return mayTranslateError(err)
}

func (e *Engine) GetCollection(ctx context.Context, collectionName string) (*protomodel.Collection, error) {
	opts := sql.DefaultTxOptions().
		WithReadOnly(true).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return nil, err
	}

	return collectionFromTable(table), nil
}

func (e *Engine) GetCollections(ctx context.Context) ([]*protomodel.Collection, error) {
	opts := sql.DefaultTxOptions().
		WithReadOnly(true).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	tables := sqlTx.Catalog().GetTables()

	collections := make([]*protomodel.Collection, len(tables))

	for i, table := range tables {
		collections[i] = collectionFromTable(table)
	}

	return collections, nil
}

func docIDFieldName(table *sql.Table) string {
	return table.PrimaryIndex().Cols()[0].Name()
}

func getTableForCollection(sqlTx *sql.SQLTx, collectionName string) (*sql.Table, error) {
	if collectionName == "" {
		return nil, fmt.Errorf("%w: invalid collection name", ErrIllegalArguments)
	}

	table, err := sqlTx.Catalog().GetTableByName(collectionName)
	if errors.Is(err, sql.ErrTableDoesNotExist) {
		return nil, fmt.Errorf("%w (%s)", mayTranslateError(err), collectionName)
	}

	return table, mayTranslateError(err)
}

func getColumnForField(table *sql.Table, field string) (*sql.Column, error) {
	if field == "" {
		return nil, fmt.Errorf("%w: invalid field name", ErrIllegalArguments)
	}

	column, err := table.GetColumnByName(field)
	if errors.Is(err, sql.ErrColumnDoesNotExist) {
		return nil, fmt.Errorf("%w (%s)", mayTranslateError(err), field)
	}

	return column, mayTranslateError(err)
}

func collectionFromTable(table *sql.Table) *protomodel.Collection {
	documentIdFieldName := docIDFieldName(table)

	indexes := table.GetIndexes()

	collection := &protomodel.Collection{
		Name:                table.Name(),
		DocumentIdFieldName: documentIdFieldName,
		Indexes:             make([]*protomodel.Index, len(indexes)),
	}

	for _, col := range table.Cols() {
		if col.Name() == DocumentBLOBField {
			continue
		}

		var colType protomodel.FieldType

		if col.Name() == documentIdFieldName {
			colType = protomodel.FieldType_STRING
		} else {
			switch col.Type() {
			case sql.BooleanType:
				colType = protomodel.FieldType_BOOLEAN
			case sql.VarcharType:
				colType = protomodel.FieldType_STRING
			case sql.IntegerType:
				colType = protomodel.FieldType_INTEGER
			case sql.Float64Type:
				colType = protomodel.FieldType_DOUBLE
			}
		}

		collection.Fields = append(collection.Fields, &protomodel.Field{
			Name: col.Name(),
			Type: colType,
		})
	}

	for i, index := range indexes {
		fields := make([]string, len(index.Cols()))

		for i, c := range index.Cols() {
			fields[i] = c.Name()
		}

		collection.Indexes[i] = &protomodel.Index{
			Fields:   fields,
			IsUnique: index.IsUnique(),
		}
	}

	return collection
}

func (e *Engine) UpdateCollection(ctx context.Context, collectionName string, documentIdFieldName string) error {
	if documentIdFieldName == DocumentBLOBField {
		return fmt.Errorf("%w(%s)", ErrReservedFieldName, DocumentBLOBField)
	}

	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return err
	}

	currIDFieldName := docIDFieldName(table)

	if documentIdFieldName != "" && documentIdFieldName != currIDFieldName {
		_, _, err := e.sqlEngine.ExecPreparedStmts(
			ctx,
			sqlTx,
			[]sql.SQLStmt{sql.NewRenameColumnStmt(table.Name(), currIDFieldName, documentIdFieldName)},
			nil,
		)
		if err != nil {
			return mayTranslateError(err)
		}
	}

	err = sqlTx.Commit(ctx)
	return mayTranslateError(err)
}

// DeleteCollection deletes a collection.
func (e *Engine) DeleteCollection(ctx context.Context, collectionName string) error {
	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{
			sql.NewDropTableStmt(collectionName), // delete collection from catalog
		},
		nil,
	)
	if err != nil {
		return mayTranslateError(err)
	}

	err = sqlTx.Commit(ctx)
	return mayTranslateError(err)
}

func (e *Engine) CreateIndex(ctx context.Context, collectionName string, fields []string, isUnique bool) error {
	if len(fields) == 0 {
		return fmt.Errorf("%w: no fields specified", ErrIllegalArguments)
	}

	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	for _, field := range fields {
		if field == DocumentBLOBField {
			return fmt.Errorf("%w(%s): non-indexable field", ErrReservedFieldName, DocumentBLOBField)
		}
	}

	createIndexStmt := sql.NewCreateIndexStmt(collectionName, fields, isUnique)

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{createIndexStmt},
		nil,
	)
	if err != nil {
		return mayTranslateError(err)
	}

	err = sqlTx.Commit(ctx)
	return mayTranslateError(err)
}

func (e *Engine) DeleteIndex(ctx context.Context, collectionName string, fields []string) error {
	if len(fields) == 0 {
		return fmt.Errorf("%w: no fields specified", ErrIllegalArguments)
	}

	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0).
		WithExplicitClose(true)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	for _, field := range fields {
		if field == DocumentBLOBField {
			return ErrFieldDoesNotExist
		}
	}

	dropIndexStmt := sql.NewDropIndexStmt(collectionName, fields)

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{dropIndexStmt},
		nil,
	)
	if err != nil {
		return mayTranslateError(err)
	}

	err = sqlTx.Commit(ctx)
	return mayTranslateError(err)
}

func (e *Engine) InsertDocument(ctx context.Context, collectionName string, doc *structpb.Struct) (txID uint64, docID DocumentID, err error) {
	txID, docIDs, err := e.InsertDocuments(ctx, collectionName, []*structpb.Struct{doc})
	if err != nil {
		return 0, nil, err
	}

	return txID, docIDs[0], nil
}

func (e *Engine) InsertDocuments(ctx context.Context, collectionName string, docs []*structpb.Struct) (txID uint64, docIDs []DocumentID, err error) {
	opts := sql.DefaultTxOptions().
		WithUnsafeMVCC(true).
		WithSnapshotMustIncludeTxID(func(lastPrecommittedTxID uint64) uint64 { return 0 }).
		WithSnapshotRenewalPeriod(0)

	sqlTx, err := e.sqlEngine.NewTx(ctx, opts)
	if err != nil {
		return 0, nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	return e.upsertDocuments(ctx, sqlTx, collectionName, docs, true)
}

func (e *Engine) upsertDocuments(ctx context.Context, sqlTx *sql.SQLTx, collectionName string, docs []*structpb.Struct, isInsert bool) (txID uint64, docIDs []DocumentID, err error) {
	if len(docs) == 0 {
		return 0, nil, fmt.Errorf("%w: no document specified", ErrIllegalArguments)
	}

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return 0, nil, err
	}

	docIDFieldName := docIDFieldName(table)

	colNames := make([]string, len(table.Cols()))

	for i, col := range table.Cols() {
		colNames[i] = col.Name()
	}

	docIDs = make([]DocumentID, len(docs))

	rows := make([]*sql.RowSpec, len(docs))

	for i, doc := range docs {
		if doc == nil || len(doc.Fields) == 0 {
			doc = &structpb.Struct{
				Fields: make(map[string]*structpb.Value),
			}
		}

		_, blobFieldProvisioned := doc.Fields[DocumentBLOBField]
		if blobFieldProvisioned {
			return 0, nil, fmt.Errorf("%w(%s)", ErrReservedFieldName, DocumentBLOBField)
		}

		var docID DocumentID

		provisionedDocID, docIDProvisioned := doc.Fields[docIDFieldName]
		if docIDProvisioned {
			if isInsert {
				return 0, nil, fmt.Errorf("%w: field (%s) should NOT be specified when inserting a document", ErrIllegalArguments, docIDFieldName)
			}

			docID, err = NewDocumentIDFromHexEncodedString(provisionedDocID.GetStringValue())
			if err != nil {
				return 0, nil, err
			}
		} else {
			if !isInsert {
				return 0, nil, fmt.Errorf("%w: field (%s) should be specified when updating a document", ErrIllegalArguments, docIDFieldName)
			}

			// generate document id
			docID = NewDocumentIDFromTx(e.sqlEngine.GetStore().LastPrecommittedTxID())
			doc.Fields[docIDFieldName] = structpb.NewStringValue(docID.EncodeToHexString())
		}

		rowSpec, err := e.generateRowSpecForDocument(table, doc)
		if err != nil {
			return 0, nil, err
		}

		docIDs[i] = docID
		rows[i] = rowSpec
	}

	// add documents to collection
	_, ctxs, err := e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{
			sql.NewUpserIntoStmt(
				collectionName,
				colNames,
				rows,
				isInsert,
				nil,
			),
		},
		nil,
	)
	if err != nil {
		return 0, nil, mayTranslateError(err)
	}

	txID = ctxs[0].TxHeader().ID

	return txID, docIDs, nil
}

func (e *Engine) generateRowSpecForDocument(table *sql.Table, doc *structpb.Struct) (*sql.RowSpec, error) {
	values := make([]sql.ValueExp, len(table.Cols()))

	for i, col := range table.Cols() {
		if col.Name() == DocumentBLOBField {
			bs, err := proto.Marshal(doc)
			if err != nil {
				return nil, err
			}

			values[i] = sql.NewBlob(bs)
			continue
		}

		var rval *structpb.Value
		var ok bool
		if rval, ok = doc.Fields[col.Name()]; !ok {
			fmt.Printf("CHECKING . COLUMNS, %s\n", col.Name())
			nameParts := strings.SplitN(col.Name(), ".", 3)
			nestedStruct := doc
			for j := range nameParts {
				fmt.Printf("CHECKING . COLUMNS, LOOPING %d, %s\n", j, nameParts[j])
				if j == len(nameParts)-1 {
					if rval, ok = nestedStruct.Fields[nameParts[len(nameParts)-1]]; !ok {
						rval = nil
					}
					fmt.Printf("Found rval %+v\n", rval)
					break
				}

				if rval, ok = nestedStruct.Fields[nameParts[j]]; !ok {
					return nil, fmt.Errorf("invalid fields")
				}

				nestedStruct = rval.GetStructValue()
				if nestedStruct == nil {
					rval = nil
					break
				}
			}
		}

		if rval != nil {
			val, err := structValueToSqlValue(rval, col.Type())
			if err != nil {
				return nil, err
			}
			values[i] = val
		} else {
			values[i] = &sql.NullValue{}
		}
	}

	return sql.NewRowSpec(values), nil
}

func (e *Engine) ReplaceDocuments(ctx context.Context, query *protomodel.Query, doc *structpb.Struct) (revisions []*protomodel.DocumentAtRevision, err error) {
	if query == nil {
		return nil, ErrIllegalArguments
	}

	if doc == nil || len(doc.Fields) == 0 {
		doc = &structpb.Struct{
			Fields: make(map[string]*structpb.Value),
		}
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions())
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, query.CollectionName)
	if err != nil {
		return nil, err
	}

	documentIdFieldName := docIDFieldName(table)

	provisionedDocID, docIDProvisioned := doc.Fields[documentIdFieldName]
	if docIDProvisioned {
		// inject id comparisson into query
		idFieldComparisson := &protomodel.FieldComparison{
			Field:    documentIdFieldName,
			Operator: protomodel.ComparisonOperator_EQ,
			Value:    provisionedDocID,
		}

		if len(query.Expressions) == 0 {
			query.Expressions = []*protomodel.QueryExpression{
				{
					FieldComparisons: []*protomodel.FieldComparison{
						idFieldComparisson,
					},
				},
			}
		} else {
			// id comparisson as a first comparisson might result in faster evaluation
			// note it mas be added into every expression
			for _, exp := range query.Expressions {
				exp.FieldComparisons = append([]*protomodel.FieldComparison{idFieldComparisson}, exp.FieldComparisons...)
			}
		}
	}

	queryCondition, err := generateSQLFilteringExpression(query.Expressions, table)
	if err != nil {
		return nil, err
	}

	queryStmt := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(query.CollectionName, documentIdFieldName)},
		query.CollectionName,
		queryCondition,
		generateSQLOrderByClauses(table, query.OrderBy),
		sql.NewInteger(int64(query.Limit)),
		nil,
	)

	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, queryStmt, nil)
	if err != nil {
		return nil, mayTranslateError(err)
	}

	var docs []*structpb.Struct

	for {
		row, err := r.Read(ctx)
		if err != nil {
			r.Close()

			if errors.Is(err, sql.ErrNoMoreRows) {
				break
			}

			return nil, mayTranslateError(err)
		}

		val := row.ValuesByPosition[0].RawValue().([]byte)
		docID, err := NewDocumentIDFromRawBytes(val)
		if err != nil {
			return nil, err
		}

		newDoc, err := structpb.NewStruct(doc.AsMap())
		if err != nil {
			return nil, err
		}

		if !docIDProvisioned {
			// add id field to updated document
			newDoc.Fields[documentIdFieldName] = structpb.NewStringValue(docID.EncodeToHexString())
		}

		docs = append(docs, newDoc)
	}

	r.Close()

	if len(docs) == 0 {
		return nil, nil
	}

	txID, docIDs, err := e.upsertDocuments(ctx, sqlTx, query.CollectionName, docs, false)
	if err != nil {
		return nil, err
	}

	for _, docID := range docIDs {
		// fetch revision
		searchKey, err := e.getKeyForDocument(ctx, sqlTx, query.CollectionName, docID)
		if err != nil {
			return nil, err
		}

		err = e.sqlEngine.GetStore().WaitForIndexingUpto(ctx, txID)
		if err != nil {
			return nil, err
		}

		encDoc, err := e.getEncodedDocument(searchKey, 0, false)
		if err != nil {
			return nil, err
		}

		revisions = append(revisions, &protomodel.DocumentAtRevision{
			TransactionId: txID,
			DocumentId:    docID.EncodeToHexString(),
			Revision:      encDoc.Revision,
			Metadata:      kvMetadataToProto(encDoc.KVMetadata),
		})
	}

	return revisions, nil
}

func (e *Engine) GetDocuments(ctx context.Context, query *protomodel.Query, offset int64) (DocumentReader, error) {
	if query == nil {
		return nil, ErrIllegalArguments
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, mayTranslateError(err)
	}

	table, err := getTableForCollection(sqlTx, query.CollectionName)
	if err != nil {
		return nil, err
	}

	queryCondition, err := generateSQLFilteringExpression(query.Expressions, table)
	if err != nil {
		return nil, err
	}

	op := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(query.CollectionName, DocumentBLOBField)},
		query.CollectionName,
		queryCondition,
		generateSQLOrderByClauses(table, query.OrderBy),
		sql.NewInteger(int64(query.Limit)),
		sql.NewInteger(offset),
	)

	// returning an open reader here, so the caller HAS to close it
	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, op, nil)
	if err != nil {
		defer sqlTx.Cancel()
		return nil, err
	}

	return newDocumentReader(r, func(_ DocumentReader) { sqlTx.Cancel() }), nil
}

func (e *Engine) CountDocuments(ctx context.Context, query *protomodel.Query, offset int64) (int64, error) {
	if query == nil {
		return 0, ErrIllegalArguments
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return 0, mayTranslateError(err)
	}

	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, query.CollectionName)
	if err != nil {
		return 0, err
	}

	queryCondition, err := generateSQLFilteringExpression(query.Expressions, table)
	if err != nil {
		return 0, err
	}

	op := sql.NewSelectStmt(
		[]sql.Selector{sql.NewAggColSelector(sql.COUNT, query.CollectionName, "*")},
		query.CollectionName,
		queryCondition,
		generateSQLOrderByClauses(table, query.OrderBy),
		sql.NewInteger(int64(query.Limit)),
		sql.NewInteger(offset),
	)

	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, op, nil)
	if err != nil {
		return 0, err
	}

	defer r.Close()

	row, err := r.Read(ctx)
	if err != nil {
		return 0, err
	}

	return row.ValuesByPosition[0].RawValue().(int64), nil
}

func (e *Engine) GetEncodedDocument(ctx context.Context, collectionName string, docID DocumentID, txID uint64) (collectionID uint32, documentIdFieldName string, encodedDoc *EncodedDocument, err error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return 0, "", nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return 0, "", nil, err
	}

	searchKey, err := e.getKeyForDocument(ctx, sqlTx, collectionName, docID)
	if err != nil {
		return 0, "", nil, err
	}

	encodedDoc, err = e.getEncodedDocument(searchKey, txID, false)
	if err != nil {
		return 0, "", nil, err
	}

	return table.ID(), docIDFieldName(table), encodedDoc, nil
}

// AuditDocument returns the audit history of a document.
func (e *Engine) AuditDocument(ctx context.Context, collectionName string, docID DocumentID, desc bool, offset uint64, limit int) ([]*protomodel.DocumentAtRevision, error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	searchKey, err := e.getKeyForDocument(ctx, sqlTx, collectionName, docID)
	if err != nil {
		return nil, err
	}

	txIDs, hCount, err := e.sqlEngine.GetStore().History(searchKey, uint64(offset), desc, limit)
	if err != nil {
		return nil, err
	}

	revision := offset + 1
	if desc {
		revision = hCount - offset
	}

	results := make([]*protomodel.DocumentAtRevision, 0)

	for _, txID := range txIDs {
		docAtRevision, err := e.getDocumentAtTransaction(searchKey, txID, false)
		if err != nil {
			return nil, err
		}

		docAtRevision.DocumentId = docID.EncodeToHexString()
		docAtRevision.Revision = revision

		results = append(results, docAtRevision)

		if desc {
			revision--
		} else {
			revision++
		}
	}

	return results, nil
}

// generateSQLFilteringExpression generates a boolean expression in Disjunctive Normal Form from a list of expressions
func generateSQLFilteringExpression(expressions []*protomodel.QueryExpression, table *sql.Table) (sql.ValueExp, error) {
	var outerExp sql.ValueExp

	for i, exp := range expressions {
		if len(exp.FieldComparisons) == 0 {
			return nil, fmt.Errorf("%w: query expression without any field comparisson", ErrIllegalArguments)
		}

		var innerExp sql.ValueExp

		for i, exp := range exp.FieldComparisons {
			column, err := getColumnForField(table, exp.Field)
			if err != nil {
				return nil, err
			}

			value, err := structValueToSqlValue(exp.Value, column.Type())
			if err != nil {
				return nil, err
			}

			colSelector := sql.NewColSelector(table.Name(), exp.Field)

			var fieldExp sql.ValueExp

			switch exp.Operator {
			case protomodel.ComparisonOperator_LIKE:
				{
					fieldExp = sql.NewLikeBoolExp(colSelector, false, value)
				}
			case protomodel.ComparisonOperator_NOT_LIKE:
				{
					fieldExp = sql.NewLikeBoolExp(colSelector, true, value)
				}
			default:
				{
					sqlCmpOp, err := sqlCmpOperatorFor(exp.Operator)
					if err != nil {
						return nil, err
					}

					fieldExp = sql.NewCmpBoolExp(sqlCmpOp, colSelector, value)
				}
			}

			if i == 0 {
				innerExp = fieldExp
			} else {
				innerExp = sql.NewBinBoolExp(sql.AND, innerExp, fieldExp)
			}
		}

		if i == 0 {
			outerExp = innerExp
		} else {
			outerExp = sql.NewBinBoolExp(sql.OR, outerExp, innerExp)
		}
	}

	return outerExp, nil
}

func sqlCmpOperatorFor(op protomodel.ComparisonOperator) (sql.CmpOperator, error) {
	switch op {
	case protomodel.ComparisonOperator_EQ:
		{
			return sql.EQ, nil
		}
	case protomodel.ComparisonOperator_NE:
		{
			return sql.NE, nil
		}
	case protomodel.ComparisonOperator_LT:
		{
			return sql.LT, nil
		}
	case protomodel.ComparisonOperator_LE:
		{
			return sql.LE, nil
		}
	case protomodel.ComparisonOperator_GT:
		{
			return sql.GT, nil
		}
	case protomodel.ComparisonOperator_GE:
		{
			return sql.GE, nil
		}
	default:
		{
			return 0, fmt.Errorf("%w: unsupported operator ('%s')", ErrIllegalArguments, op)
		}
	}
}

func (e *Engine) getKeyForDocument(ctx context.Context, sqlTx *sql.SQLTx, collectionName string, documentID DocumentID) ([]byte, error) {
	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return nil, err
	}

	var searchKey []byte

	valbuf := bytes.Buffer{}

	rval := sql.NewBlob(documentID[:])
	encVal, _, err := sql.EncodeRawValueAsKey(rval.RawValue(), sql.BLOBType, MaxDocumentIDLength)
	if err != nil {
		return nil, err
	}
	_, err = valbuf.Write(encVal)
	if err != nil {
		return nil, err
	}

	pkEncVals := valbuf.Bytes()

	searchKey = sql.MapKey(
		e.sqlEngine.GetPrefix(),
		sql.PIndexPrefix,
		sql.EncodeID(1),
		sql.EncodeID(table.ID()),
		sql.EncodeID(table.PrimaryIndex().ID()),
		pkEncVals,
	)

	return searchKey, nil
}

func (e *Engine) getDocumentAtTransaction(
	key []byte,
	atTx uint64,
	skipIntegrityCheck bool,
) (docAtRevision *protomodel.DocumentAtRevision, err error) {
	encDoc, err := e.getEncodedDocument(key, atTx, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	if encDoc.KVMetadata != nil && encDoc.KVMetadata.Deleted() {
		return &protomodel.DocumentAtRevision{
			TransactionId: encDoc.TxID,
			Metadata:      kvMetadataToProto(encDoc.KVMetadata),
		}, nil
	}

	voff := sql.EncLenLen + sql.EncIDLen

	// DocumentIDField
	_, n, err := sql.DecodeValue(encDoc.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, mayTranslateError(err)
	}

	voff += n + sql.EncIDLen

	// DocumentBLOBField
	encodedDoc, _, err := sql.DecodeValue(encDoc.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, mayTranslateError(err)
	}

	docBytes := encodedDoc.RawValue().([]byte)

	doc := &structpb.Struct{}
	err = proto.Unmarshal(docBytes, doc)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentAtRevision{
		TransactionId: encDoc.TxID,
		Metadata:      kvMetadataToProto(encDoc.KVMetadata),
		Document:      doc,
	}, err
}

func (e *Engine) getEncodedDocument(
	key []byte,
	atTx uint64,
	skipIntegrityCheck bool,
) (encDoc *EncodedDocument, err error) {

	var txID uint64
	var encodedDoc []byte
	var md *store.KVMetadata
	var revision uint64

	index := e.sqlEngine.GetStore()
	if atTx == 0 {
		valRef, err := index.Get(key)
		if err != nil {
			return nil, mayTranslateError(err)
		}

		txID = valRef.Tx()

		md = valRef.KVMetadata()

		encodedDoc, err = valRef.Resolve()
		if err != nil {
			return nil, mayTranslateError(err)
		}

		// Revision can be calculated from the history count
		revision = valRef.HC()
	} else {
		txID = atTx
		md, encodedDoc, err = e.readMetadataAndValue(key, atTx, skipIntegrityCheck)
		if err != nil {
			return nil, err
		}
	}

	return &EncodedDocument{
		TxID:            txID,
		Revision:        revision,
		KVMetadata:      md,
		EncodedDocument: encodedDoc,
	}, err
}

func (e *Engine) readMetadataAndValue(key []byte, atTx uint64, skipIntegrityCheck bool) (*store.KVMetadata, []byte, error) {
	store := e.sqlEngine.GetStore()
	entry, _, err := store.ReadTxEntry(atTx, key, skipIntegrityCheck)
	if err != nil {
		return nil, nil, err
	}

	v, err := store.ReadValue(entry)
	if err != nil {
		return nil, nil, err
	}

	return entry.Metadata(), v, nil
}

// DeleteDocuments deletes documents matching the query
func (e *Engine) DeleteDocuments(ctx context.Context, query *protomodel.Query) error {
	if query == nil {
		return ErrIllegalArguments
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions())
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, query.CollectionName)
	if err != nil {
		return err
	}

	queryCondition, err := generateSQLFilteringExpression(query.Expressions, table)
	if err != nil {
		return err
	}

	// Delete a single document matching the query
	deleteStmt := sql.NewDeleteFromStmt(
		table.Name(),
		queryCondition,
		generateSQLOrderByClauses(table, query.OrderBy),
		sql.NewInteger(int64(query.Limit)),
	)

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{deleteStmt},
		nil,
	)
	if err != nil {
		return mayTranslateError(err)
	}

	return nil
}

// CopyCatalogToTx copies the current sql catalog to the ongoing transaction.
func (e *Engine) CopyCatalogToTx(ctx context.Context, tx *store.OngoingTx) error {
	return e.sqlEngine.CopyCatalogToTx(ctx, tx)
}

func generateSQLOrderByClauses(table *sql.Table, orderBy []*protomodel.OrderByClause) (ordCols []*sql.OrdCol) {
	for _, col := range orderBy {
		ordCols = append(ordCols, sql.NewOrdCol(table.Name(), col.Field, col.Desc))
	}
	return ordCols
}
