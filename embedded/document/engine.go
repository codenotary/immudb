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

var (
	ErrIllegalArguments       = store.ErrIllegalArguments
	ErrUnsupportedType        = errors.New("unsupported type")
	ErrCollectionDoesNotExist = errors.New("collection does not exist")
	ErrMaxLengthExceeded      = errors.New("max length exceeded")
	ErrMultipleDocumentsFound = errors.New("multiple documents found")
	ErrDocumentNotFound       = errors.New("document not found")
	ErrDocumentIDMismatch     = errors.New("document id mismatch")
	ErrNoMoreDocuments        = errors.New("no more documents")
)

func mayTranslateError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, sql.ErrTableDoesNotExist) {
		return ErrCollectionDoesNotExist
	}

	if errors.Is(err, sql.ErrNoMoreRows) {
		return ErrNoMoreDocuments
	}

	return err
}

// protomodel to sql.Value mapping
var (
	// structpb.Value to sql.ValueExp conversion
	structValueToSqlValue = func(stype sql.SQLValueType, value *structpb.Value) (sql.ValueExp, error) {
		switch stype {
		case sql.VarcharType:
			_, ok := value.GetKind().(*structpb.Value_StringValue)
			if !ok {
				return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
			}
			return sql.NewVarchar(value.GetStringValue()), nil
		case sql.IntegerType:
			_, ok := value.GetKind().(*structpb.Value_NumberValue)
			if !ok {
				return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
			}
			return sql.NewInteger(int64(value.GetNumberValue())), nil
		case sql.BLOBType:
			_, ok := value.GetKind().(*structpb.Value_StructValue)
			if !ok {
				return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
			}
			return sql.NewBlob([]byte(value.GetStructValue().String())), nil
		case sql.Float64Type:
			_, ok := value.GetKind().(*structpb.Value_NumberValue)
			if !ok {
				return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
			}
			return sql.NewFloat64(value.GetNumberValue()), nil
		}

		return nil, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
	}

	valueTypeDefaultLength = func(stype sql.SQLValueType) (int, error) {
		switch stype {
		case sql.VarcharType:
			return sql.MaxKeyLen, nil
		case sql.IntegerType:
			return 0, nil
		case sql.BLOBType:
			return sql.MaxKeyLen, nil
		case sql.Float64Type:
			return 0, nil
		case sql.BooleanType:
			return 0, nil
		}

		return 0, fmt.Errorf("%w(%s)", ErrUnsupportedType, stype)
	}
)

type EncodedDocumentAtRevision struct {
	TxID            uint64
	Revision        uint64
	EncodedDocument []byte
}

type DocumentReader interface {
	Read(ctx context.Context, count int) ([]*structpb.Struct, error)
	Close() error
}

func NewEngine(store *store.ImmuStore, opts *sql.Options) (*Engine, error) {
	engine, err := sql.NewEngine(store, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{engine}, nil
}

type Engine struct {
	sqlEngine *sql.Engine
}

func (e *Engine) CreateCollection(ctx context.Context, collection *protomodel.Collection) error {
	if collection == nil {
		return ErrIllegalArguments
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

	idFieldName := DefaultDocumentIDField
	if collection.IdFieldName != "" {
		idFieldName = collection.IdFieldName
	}

	columns := make([]*sql.ColSpec, 2+len(collection.Fields))

	// add primary key for document id
	columns[0] = sql.NewColSpec(idFieldName, sql.BLOBType, MaxDocumentIDLength, false, true)

	// add columnn for blob, which stores the document as a whole
	columns[1] = sql.NewColSpec(DocumentBLOBField, sql.BLOBType, 0, false, false)

	// add index keys
	for i, field := range collection.Fields {
		colLen, err := valueTypeDefaultLength(field.Type.String())
		if err != nil {
			return err
		}

		columns[i+2] = sql.NewColSpec(field.Name, field.Type.String(), colLen, false, false)
	}

	_, _, err = e.sqlEngine.ExecPreparedStmts(
		ctx,
		sqlTx,
		[]sql.SQLStmt{sql.NewCreateTableStmt(
			collection.Name,
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

	for _, index := range collection.Indexes {
		if len(index.Fields) == 1 && index.Fields[0] == idFieldName {
			if !index.IsUnique {
				return fmt.Errorf("%w: index on id field must be unique", ErrIllegalArguments)
			}
			// idField is the primary key and so the index is automatically created
			continue
		}

		indexStmts = append(indexStmts, sql.NewCreateIndexStmt(collection.Name, index.Fields, index.IsUnique))
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
	indexes := table.GetIndexes()

	collection := &protomodel.Collection{
		Name:        table.Name(),
		IdFieldName: docIDFieldName(table),
		Indexes:     make([]*protomodel.Index, len(indexes)),
	}

	for _, col := range table.Cols() {
		if col.Name() == DocumentBLOBField {
			continue
		}

		var colType protomodel.FieldType

		switch col.Type() {
		case sql.BooleanType:
			colType = protomodel.FieldType_BOOLEAN
		case sql.VarcharType:
			colType = protomodel.FieldType_STRING
		case sql.IntegerType:
			colType = protomodel.FieldType_INTEGER
		case sql.Float64Type:
			colType = protomodel.FieldType_DOUBLE
		case sql.BLOBType:
			colType = protomodel.FieldType_STRING
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

func (e *Engine) UpdateCollection(ctx context.Context, collection *protomodel.Collection) error {
	if collection == nil {
		return ErrIllegalArguments
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

	table, err := getTableForCollection(sqlTx, collection.Name)
	if err != nil {
		return err
	}

	idFieldName := docIDFieldName(table)

	if collection.IdFieldName != "" && collection.IdFieldName != idFieldName {
		_, _, err := e.sqlEngine.ExecPreparedStmts(
			ctx,
			sqlTx,
			[]sql.SQLStmt{sql.NewRenameColumnStmt(table.Name(), idFieldName, collection.IdFieldName)},
			nil,
		)
		if err != nil {
			return mayTranslateError(err)
		}
	}

	/*
		updateCollectionStmts := make([]sql.SQLStmt, 0)

		if len(removeIdxKeys) > 0 {
			// delete indexes from collection
			deleteIdxStmts := make([]sql.SQLStmt, 0)
			for _, idx := range removeIdxKeys {
				deleteIdxStmts = append(deleteIdxStmts, sql.NewDropIndexStmt(collectionName, idx))
			}

			_, _, err := e.sqlEngine.ExecPreparedStmts(
				ctx,
				sqlTx,
				deleteIdxStmts,
				nil,
			)
			if err != nil {
				return err
			}
		}

		if len(addIdxKeys) > 0 {
			// add index keys
			for name, idx := range addIdxKeys {
				colLen, err := valueTypeDefaultLength(idx.Type)
				if err != nil {
					return fmt.Errorf("index key specified is not supported: %v", idx.Type)
				}

				// check if index column already exists
				if _, err := table.GetColumnByName(name); err == nil {
					continue
				}

				// add indexes as new columns to collection
				updateCollectionStmts = append(updateCollectionStmts, sql.NewAddColumnStmt(collectionName, sql.NewColSpec(name, idx.Type, colLen, false, false)))
			}

			// add indexes to collection
			for name, idx := range addIdxKeys {
				updateCollectionStmts = append(updateCollectionStmts, sql.NewCreateIndexStmt(collectionName, []string{name}, idx.IsUnique))
			}

			_, _, err := e.sqlEngine.ExecPreparedStmts(
				ctx,
				sqlTx,
				updateCollectionStmts,
				nil,
			)
			if err != nil {
				return err
			}
		}
	*/

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
		nil,
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
		return 0, nil, ErrIllegalArguments
	}

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return 0, nil, err
	}

	colNames := make([]string, len(table.Cols()))

	for i, col := range table.Cols() {
		colNames[i] = col.Name()
	}

	rows := make([]*sql.RowSpec, len(docs))

	for _, doc := range docs {
		if doc == nil {
			return 0, nil, fmt.Errorf("%w: nil document", ErrIllegalArguments)
		}

		docIDFieldName := docIDFieldName(table)

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

		docIDs = append(docIDs, docID)
		rows = append(rows, rowSpec)
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
		} else {
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
	}

	return sql.NewRowSpec(values), nil
}

func (e *Engine) UpdateDocument(ctx context.Context, collectionName string, query *protomodel.Query, doc *structpb.Struct) (txID uint64, docID DocumentID, rev uint64, err error) {
	if doc == nil {
		return 0, nil, 0, ErrIllegalArguments
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
		idFieldComparissonExp := &protomodel.QueryExpression{
			FieldComparisons: []*protomodel.FieldComparison{
				{
					Field:    idFieldName,
					Operator: protomodel.ComparisonOperator_EQ,
					Value:    provisionedDocID,
				},
			},
		}

		if query == nil || len(query.Expressions) == 0 {
			query = &protomodel.Query{
				Expressions: []*protomodel.QueryExpression{idFieldComparissonExp},
			}
		} else {
			// id comparisson as a first expression might result in faster comparisson
			query.Expressions = append([]*protomodel.QueryExpression{idFieldComparissonExp}, query.Expressions...)
		}
	}

	queryCondition, err := e.generateSQLExpression(ctx, query, table)
	if err != nil {
		return 0, nil, 0, err
	}

	queryStmt := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(collectionName, idFieldName)},
		collectionName,
		queryCondition,
		sql.NewInteger(1),
		nil,
	)

	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, queryStmt, nil)
	if err != nil {
		return 0, nil, 0, mayTranslateError(err)
	}
	defer r.Close()

	row, err := r.Read(ctx)
	if err != nil {
		return 0, nil, 0, mayTranslateError(err)
	}

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

func (e *Engine) GetDocuments(ctx context.Context, collectionName string, query *protomodel.Query, offset int64) (DocumentReader, error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return nil, err
	}

	queryCondition, err := e.generateSQLExpression(ctx, query, table)
	if err != nil {
		return nil, err
	}

	op := sql.NewSelectStmt(
		[]sql.Selector{sql.NewColSelector(collectionName, DocumentBLOBField)},
		collectionName,
		queryCondition,
		nil,
		sql.NewInteger(offset),
	)

	// returning an open reader here, so the caller HAS to close it
	r, err := e.sqlEngine.QueryPreparedStmt(ctx, sqlTx, op, nil)
	if err != nil {
		return nil, err
	}

	return newDocumentReader(r), nil
}

func (e *Engine) GetEncodedDocument(ctx context.Context, collectionName string, docID DocumentID, txID uint64) (collectionID uint32, docAtRevision *EncodedDocumentAtRevision, err error) {
	sqlTx, err := e.sqlEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return 0, nil, mayTranslateError(err)
	}
	defer sqlTx.Cancel()

	table, err := getTableForCollection(sqlTx, collectionName)
	if err != nil {
		return 0, nil, err
	}

	searchKey, err := e.getKeyForDocument(ctx, sqlTx, collectionName, docID)
	if err != nil {
		return 0, nil, err
	}

	docAtRevision, err = e.getEncodedDocumentAtRevision(searchKey, txID, false)
	if err != nil {
		return 0, nil, err
	}

	return table.ID(), docAtRevision, nil
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
func (d *Engine) generateSQLExpression(ctx context.Context, query *protomodel.Query, table *sql.Table) (sql.ValueExp, error) {
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

func newDocumentReader(reader sql.RowReader) DocumentReader {
	return &documentReader{reader: reader}
}

type documentReader struct {
	reader sql.RowReader
}

// ReadStructMessagesFromReader reads a number of messages from a reader and returns them as a slice of Struct messages.
func (d *documentReader) Read(ctx context.Context, count int) ([]*structpb.Struct, error) {
	if count < 1 {
		return nil, sql.ErrIllegalArguments
	}

	var err error
	results := make([]*structpb.Struct, 0)

	for l := 0; l < count; l++ {
		var row *sql.Row
		row, err = d.reader.Read(ctx)
		if err == sql.ErrNoMoreRows {
			err = ErrNoMoreDocuments
			break
		}
		if err != nil {
			return nil, err
		}

		docBytes := row.ValuesByPosition[0].RawValue().([]byte)

		doc := &structpb.Struct{}
		err = json.Unmarshal(docBytes, doc)
		if err != nil {
			return nil, err
		}

		results = append(results, doc)
	}

	return results, err
}

func (d *documentReader) Close() error {
	return d.reader.Close()
}
