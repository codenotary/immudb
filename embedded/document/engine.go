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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/protomodel"

	"google.golang.org/protobuf/types/known/structpb"
)

const (
	DefaultDocumentIDField = "_id"
	DocumentBLOBField      = "_doc"
)

type Engine struct {
	sqlEngine *sql.Engine
}

type EncodedDocumentAtRevision struct {
	TxID            uint64
	Revision        uint64
	EncodedDocument []byte
}

func NewEngine(store *store.ImmuStore, opts *Options) (*Engine, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	engine, err := sql.NewEngine(store, sql.DefaultOptions().WithPrefix(opts.prefix))
	if err != nil {
		return nil, err
	}

	return &Engine{engine}, nil
}

func (e *Engine) CreateCollection(ctx context.Context, name, idFieldName string, fields []*protomodel.Field, indexes []*protomodel.Index) error {
	if idFieldName == "" {
		idFieldName = DefaultDocumentIDField
	}

	if idFieldName == DocumentBLOBField {
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
	columns[0] = sql.NewColSpec(idFieldName, sql.BLOBType, MaxDocumentIDLength, false, true)

	// add columnn for blob, which stores the document as a whole
	columns[1] = sql.NewColSpec(DocumentBLOBField, sql.BLOBType, 0, false, false)

	for i, field := range fields {
		if field.Name == idFieldName {
			return fmt.Errorf("%w(%s): should not be specified", ErrReservedFieldName, idFieldName)
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
			[]string{idFieldName},
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

		if len(index.Fields) == 1 && index.Fields[0] == idFieldName {
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

func (e *Engine) ListCollections(ctx context.Context) ([]*protomodel.Collection, error) {
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

func docIDFieldName(table *sql.Table) string {
	return table.PrimaryIndex().Cols()[0].Name()
}

func getTableForCollection(sqlTx *sql.SQLTx, collectionName string) (*sql.Table, error) {
	table, err := sqlTx.Catalog().GetTableByName(collectionName)
	return table, mayTranslateError(err)
}

func collectionFromTable(table *sql.Table) *protomodel.Collection {
	idFieldName := docIDFieldName(table)

	indexes := table.GetIndexes()

	collection := &protomodel.Collection{
		Name:        table.Name(),
		IdFieldName: idFieldName,
		Indexes:     make([]*protomodel.Index, len(indexes)),
	}

	for _, col := range table.Cols() {
		if col.Name() == DocumentBLOBField {
			continue
		}

		var colType protomodel.FieldType

		if col.Name() == idFieldName {
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

func (e *Engine) UpdateCollection(ctx context.Context, collectionName string, idFieldName string) error {
	if idFieldName == DocumentBLOBField {
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

	if idFieldName != "" && idFieldName != currIDFieldName {
		_, _, err := e.sqlEngine.ExecPreparedStmts(
			ctx,
			sqlTx,
			[]sql.SQLStmt{sql.NewRenameColumnStmt(table.Name(), currIDFieldName, idFieldName)},
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
	txID, docIDs, err := e.BulkInsertDocuments(ctx, collectionName, []*structpb.Struct{doc})
	if err != nil {
		return 0, nil, err
	}

	return txID, docIDs[0], nil
}

func (e *Engine) BulkInsertDocuments(ctx context.Context, collectionName string, docs []*structpb.Struct) (txID uint64, docIDs []DocumentID, err error) {
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
			bs, err := json.Marshal(doc)
			if err != nil {
				return nil, err
			}

			values[i] = sql.NewBlob(bs)
			continue
		}

		if rval, ok := doc.Fields[col.Name()]; ok {
			val, err := structValueToSqlValue(col.Type(), rval)
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

func (e *Engine) UpdateDocument(ctx context.Context, collectionName string, query *protomodel.Query, doc *structpb.Struct) (txID uint64, docID DocumentID, rev uint64, err error) {
	if doc == nil || len(doc.Fields) == 0 {
		doc = &structpb.Struct{
			Fields: make(map[string]*structpb.Value),
		}
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions())
	if err != nil {
		return 0, nil, 0, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return 0, nil, 0, err
	}

	idFieldName := docIDFieldName(table)

	provisionedDocID, docIDProvisioned := doc.Fields[idFieldName]
	if docIDProvisioned {
		// inject id comparisson into query
		idFieldComparisson := &protomodel.FieldComparison{
			Field:    idFieldName,
			Operator: protomodel.ComparisonOperator_EQ,
			Value:    provisionedDocID,
		}

		if query == nil || len(query.Expressions) == 0 {
			query = &protomodel.Query{
				Expressions: []*protomodel.QueryExpression{
					{
						FieldComparisons: []*protomodel.FieldComparison{
							idFieldComparisson,
						},
					},
				},
			}
		} else {
			// id comparisson as a first expression might result in faster comparisson
			firstExp := query.Expressions[0]
			firstExp.FieldComparisons = append([]*protomodel.FieldComparison{idFieldComparisson}, firstExp.FieldComparisons...)
		}
	}

	queryCondition, err := generateSQLExpression(query, table)
	if err != nil {
		return 0, nil, 0, err
	}

	queryStmt := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(collectionName, idFieldName)},
		collectionName,
		queryCondition,
		sql.NewInteger(1),
		nil,
		nil,
	)

	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, queryStmt, nil)
	if err != nil {
		return 0, nil, 0, mayTranslateError(err)
	}

	row, err := r.Read(ctx)
	if err != nil {
		r.Close()

		if errors.Is(err, sql.ErrNoMoreRows) {
			return 0, nil, 0, ErrDocumentNotFound
		}

		return 0, nil, 0, mayTranslateError(err)
	}

	r.Close()

	val := row.ValuesByPosition[0].RawValue().([]byte)
	docID, err = NewDocumentIDFromRawBytes(val)
	if err != nil {
		return 0, nil, 0, err
	}

	if !docIDProvisioned {
		// add id field to updated document
		doc.Fields[idFieldName] = structpb.NewStringValue(docID.EncodeToHexString())
	}

	txID, _, err = e.upsertDocuments(ctx, sqlTx, collectionName, []*structpb.Struct{doc}, false)
	if err != nil {
		return 0, nil, 0, err
	}

	// fetch revision
	searchKey, err := e.getKeyForDocument(ctx, sqlTx, collectionName, docID)
	if err != nil {
		return txID, docID, 0, nil
	}

	err = e.sqlEngine.GetStore().WaitForIndexingUpto(ctx, txID)
	if err != nil {
		return txID, docID, 0, nil
	}

	encDoc, err := e.getEncodedDocumentAtRevision(searchKey, 0, false)
	if err != nil {
		return txID, docID, 0, nil
	}

	return txID, docID, encDoc.Revision, err
}

func (e *Engine) GetDocuments(
	ctx context.Context,
	collectionName string,
	query *protomodel.Query,
	orderExp []*protomodel.OrderExpression,
	offset int64,
) (DocumentReader, error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, mayTranslateError(err)
	}

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return nil, err
	}

	queryCondition, err := generateSQLExpression(query, table)
	if err != nil {
		return nil, err
	}

	op := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(collectionName, DocumentBLOBField)},
		collectionName,
		queryCondition,
		nil,
		sql.NewInteger(offset),
		generateOrderByExpression(table, orderExp),
	)

	// returning an open reader here, so the caller HAS to close it
	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, op, nil)
	if err != nil {
		defer sqlTx.Cancel()
		return nil, err
	}

	return newDocumentReader(r, func(_ DocumentReader) { sqlTx.Cancel() }), nil
}

func (e *Engine) GetEncodedDocument(ctx context.Context, collectionName string, docID DocumentID, txID uint64) (collectionID uint32, idFieldName string, docAtRevision *EncodedDocumentAtRevision, err error) {
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

	docAtRevision, err = e.getEncodedDocumentAtRevision(searchKey, txID, false)
	if err != nil {
		return 0, "", nil, err
	}

	return table.ID(), docIDFieldName(table), docAtRevision, nil
}

// DocumentAudit returns the audit history of a document.
func (e *Engine) DocumentAudit(ctx context.Context, collectionName string, documentID DocumentID, pageNum int, itemsPerPage int) ([]*protomodel.DocumentAtRevision, error) {
	offset := (pageNum - 1) * itemsPerPage
	limit := itemsPerPage
	if offset < 0 || limit < 1 {
		return nil, fmt.Errorf("invalid offset or limit")
	}

	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	searchKey, err := e.getKeyForDocument(ctx, sqlTx, collectionName, documentID)
	if err != nil {
		return nil, err
	}

	txIDs, _, err := e.sqlEngine.GetStore().History(searchKey, uint64(offset), false, limit)
	if err != nil {
		return nil, err
	}

	results := make([]*protomodel.DocumentAtRevision, 0)

	for i, txID := range txIDs {
		docAtRevision, err := e.getDocumentAtRevision(searchKey, txID, false)
		if err != nil {
			return nil, err
		}

		docAtRevision.Revision = uint64(i) + 1

		results = append(results, docAtRevision)
	}

	return results, nil
}

// generateSQLExpression generates a boolean expression from a list of expressions.
func generateSQLExpression(query *protomodel.Query, table *sql.Table) (sql.ValueExp, error) {
	if query == nil || len(query.Expressions) == 0 {
		return nil, nil
	}

	var outerExp sql.ValueExp

	for i, exp := range query.Expressions {
		if len(exp.FieldComparisons) == 0 {
			return nil, fmt.Errorf("%w: query expression without any field comparisson", ErrIllegalArguments)
		}

		var innerExp sql.ValueExp

		for i, exp := range exp.FieldComparisons {
			column, err := table.GetColumnByName(exp.Field)
			if err != nil {
				return nil, mayTranslateError(err)
			}

			value, err := structValueToSqlValue(column.Type(), exp.Value)
			if err != nil {
				return nil, err
			}

			colSelector := sql.NewColSelector(table.Name(), exp.Field)

			fieldComparisonExp := sql.NewCmpBoolExp(int(exp.Operator), colSelector, value)

			if i == 0 {
				innerExp = fieldComparisonExp
			} else {
				innerExp = sql.NewBinBoolExp(sql.AND, innerExp, fieldComparisonExp)
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

func (e *Engine) getKeyForDocument(ctx context.Context, sqlTx *sql.SQLTx, collectionName string, documentID DocumentID) ([]byte, error) {
	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return nil, err
	}

	var searchKey []byte

	valbuf := bytes.Buffer{}

	rval := sql.NewBlob(documentID[:])
	encVal, err := sql.EncodeRawValueAsKey(rval.RawValue(), sql.BLOBType, MaxDocumentIDLength)
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

func (e *Engine) getDocumentAtRevision(
	key []byte,
	atTx uint64,
	skipIntegrityCheck bool,
) (docAtRevision *protomodel.DocumentAtRevision, err error) {
	encDocAtRevision, err := e.getEncodedDocumentAtRevision(key, atTx, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	voff := sql.EncLenLen + sql.EncIDLen

	// DocumentIDField
	_, n, err := sql.DecodeValue(encDocAtRevision.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, mayTranslateError(err)
	}

	voff += n + sql.EncIDLen

	// DocumentBLOBField
	encodedDoc, _, err := sql.DecodeValue(encDocAtRevision.EncodedDocument[voff:], sql.BLOBType)
	if err != nil {
		return nil, mayTranslateError(err)
	}

	docBytes := encodedDoc.RawValue().([]byte)

	doc := &structpb.Struct{}
	err = json.Unmarshal(docBytes, doc)
	if err != nil {
		return nil, err
	}

	return &protomodel.DocumentAtRevision{
		TransactionId: encDocAtRevision.TxID,
		Revision:      encDocAtRevision.Revision,
		Document:      doc,
	}, err
}

func (e *Engine) getEncodedDocumentAtRevision(
	key []byte,
	atTx uint64,
	skipIntegrityCheck bool,
) (encDoc *EncodedDocumentAtRevision, err error) {

	var txID uint64
	var encodedDoc []byte
	var revision uint64

	index := e.sqlEngine.GetStore()
	if atTx == 0 {
		valRef, err := index.Get(key)
		if err != nil {
			return nil, mayTranslateError(err)
		}

		txID = valRef.Tx()

		encodedDoc, err = valRef.Resolve()
		if err != nil {
			return nil, mayTranslateError(err)
		}

		// Revision can be calculated from the history count
		revision = valRef.HC()
	} else {
		txID = atTx
		_, encodedDoc, err = e.readMetadataAndValue(key, atTx, skipIntegrityCheck)
		if err != nil {
			return nil, err
		}
	}

	return &EncodedDocumentAtRevision{
		TxID:            txID,
		Revision:        revision,
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

// DeleteDocument deletes a single document matching the query
func (e *Engine) DeleteDocument(ctx context.Context, collectionName string, query *protomodel.Query) (err error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions())
	if err != nil {
		return mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return err
	}

	queryCondition, err := generateSQLExpression(query, table)
	if err != nil {
		return err
	}

	// Delete a single document matching the query
	deleteStmt := sql.NewDeleteFromStmt(table.Name(), queryCondition, sql.NewInteger(1))

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

func generateOrderByExpression(table *sql.Table, orderBy []*protomodel.OrderExpression) (ordCols []*sql.OrdCol) {
	for _, col := range orderBy {
		ordCols = append(ordCols, sql.NewOrdCol(table.Name(), col.Field, col.Desc))
	}
	return ordCols
}
