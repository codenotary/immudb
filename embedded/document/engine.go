package document

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	// Default fields for document store
	defaultDocumentIDField   = "_id"
	defaultDocumentBLOBField = "_obj"
)

// Schema to ValueType mapping
var (
	// ValueType to ValueExp conversion
	valueTypeToExp = func(stype sql.SQLValueType, value *structpb.Value) (sql.ValueExp, error) {
		errType := fmt.Errorf("unsupported type %v", stype)
		switch stype {
		case sql.VarcharType:
			_, ok := value.GetKind().(*structpb.Value_StringValue)
			if !ok {
				return nil, errType
			}
			return sql.NewVarchar(value.GetStringValue()), nil
		case sql.IntegerType:
			_, ok := value.GetKind().(*structpb.Value_NumberValue)
			if !ok {
				return nil, errType
			}
			return sql.NewNumber(int64(value.GetNumberValue())), nil
		case sql.BLOBType:
			_, ok := value.GetKind().(*structpb.Value_StructValue)
			if !ok {
				return nil, errType
			}
			return sql.NewBlob([]byte(value.GetStructValue().String())), nil
		}

		return nil, errType
	}

	valueTypeToFieldValue = func(tv sql.TypedValue) (*structpb.Value, error) {
		errType := fmt.Errorf("unsupported type %v", tv.Type())
		value := &structpb.Value{}
		switch tv.Type() {
		case sql.VarcharType:
			value.Kind = &structpb.Value_StringValue{StringValue: tv.Value().(string)}
			return value, nil
		case sql.IntegerType:
			value.Kind = &structpb.Value_NumberValue{NumberValue: float64(tv.Value().(int64))}
			return value, nil
		case sql.BLOBType:
			value.Kind = &structpb.Value_StringValue{StringValue: string(tv.Value().([]byte))}
			return value, nil
		}

		return nil, errType
	}

	valueTypeDefaultLength = func(stype sql.SQLValueType) (int, error) {
		errType := fmt.Errorf("unsupported type %v", stype)
		switch stype {
		case sql.VarcharType:
			return 256, nil
		case sql.IntegerType:
			return 0, nil
		case sql.BLOBType:
			return 256, nil
		}

		return 0, errType
	}
)

type Query struct {
	Field    string
	Operator int
	Value    *structpb.Value
}

func NewEngine(store *store.ImmuStore, opts *sql.Options) (*Engine, error) {
	engine, err := sql.NewEngine(store, opts)
	if err != nil {
		return nil, err
	}

	return &Engine{engine}, nil
}

type Engine struct {
	*sql.Engine
}

// TODO: Add support for index creation
func (d *Engine) CreateCollection(ctx context.Context, collectionName string, idxKeys map[string]sql.SQLValueType) error {
	primaryKeys := []string{defaultDocumentIDField}
	indexKeys := make([]string, 0)
	columns := make([]*sql.ColSpec, 0)

	// add primary key for document id
	columns = append(columns, sql.NewColSpec(defaultDocumentIDField, sql.BLOBType, 32, false, true))

	// add columnn for blob, which stores the document as a whole
	columns = append(columns, sql.NewColSpec(defaultDocumentBLOBField, sql.BLOBType, 0, false, false))

	// add index keys
	for name, schType := range idxKeys {
		indexKeys = append(indexKeys, name)
		colLen, err := valueTypeDefaultLength(schType)
		if err != nil {
			return fmt.Errorf("index key specified is not supported: %v", schType)
		}
		columns = append(columns, sql.NewColSpec(name, schType, colLen, false, false))
	}

	// add columns to collection
	_, _, err := d.ExecPreparedStmts(
		context.Background(),
		nil,
		[]sql.SQLStmt{sql.NewCreateTableStmt(
			collectionName,
			false,
			columns,
			primaryKeys,
		)},
		nil,
	)
	if err != nil {
		return err
	}

	if len(indexKeys) > 0 {
		// add indexes to collection
		_, _, err = d.ExecPreparedStmts(
			context.Background(),
			nil,
			[]sql.SQLStmt{sql.NewCreateIndexStmt(collectionName, indexKeys)},
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Engine) CreateDocument(ctx context.Context, collectionName string, doc *structpb.Struct) (DocumentID, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return NilDocumentID, err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.CurrentDatabase(), collectionName)
	if err != nil {
		return NilDocumentID, err
	}

	cols := make([]string, 0)
	tcolumns := table.Cols()
	rows := make([]*sql.RowSpec, 0)

	for _, col := range tcolumns {
		cols = append(cols, col.Name())
	}

	// check if document id is already present
	_, ok := doc.Fields[defaultDocumentIDField]
	if ok {
		return NilDocumentID, fmt.Errorf("_id field is not allowed to be set")
	}

	// generate document id
	lastPrecommittedTxID := d.GetStore().LastPrecommittedTxID()
	docID := NewDocumentIDFromTx(lastPrecommittedTxID)

	values := make([]sql.ValueExp, 0)
	for _, col := range tcolumns {
		switch col.Name() {
		case defaultDocumentIDField:
			// add document id to document
			values = append(values, sql.NewBlob(docID[:]))
		case defaultDocumentBLOBField:
			document, err := NewDocumentFrom(doc)
			if err != nil {
				return NilDocumentID, err
			}
			res, err := document.MarshalJSON()
			if err != nil {
				return NilDocumentID, err
			}
			values = append(values, sql.NewBlob(res))
		default:
			if rval, ok := doc.Fields[col.Name()]; ok {
				valType, err := valueTypeToExp(col.Type(), rval)
				if err != nil {
					return NilDocumentID, err
				}
				values = append(values, valType)
			}
		}
	}

	if len(values) > 0 {
		rows = append(rows, sql.NewRowSpec(values))
	}

	// add document to collection
	_, _, err = d.ExecPreparedStmts(
		ctx,
		nil,
		[]sql.SQLStmt{
			sql.NewUpserIntoStmt(
				d.CurrentDatabase(),
				collectionName,
				cols,
				rows,
				nil,
			),
		},
		nil,
	)
	if err != nil {
		return NilDocumentID, err
	}

	return docID, nil
}

func (d *Engine) ListCollections(ctx context.Context) (map[string][]*sql.Index, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	database, err := tx.Catalog().GetDatabaseByName(d.CurrentDatabase())
	if err != nil {
		return nil, err
	}

	collectionMap := make(map[string][]*sql.Index)
	for _, table := range database.GetTables() {
		_, ok := collectionMap[table.Name()]
		if !ok {
			collectionMap[table.Name()] = make([]*sql.Index, 0)
		}
		collectionMap[table.Name()] = append(collectionMap[table.Name()], table.GetIndexes()...)
	}

	return collectionMap, nil
}

func (d *Engine) GetCollection(ctx context.Context, collectionName string) ([]*sql.Index, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.CurrentDatabase(), collectionName)
	if err != nil {
		return nil, fmt.Errorf("collection %s does not exist", collectionName)
	}

	// fetch primary and index keys from collection schema
	cols := table.Cols()
	if len(cols) == 0 {
		return nil, fmt.Errorf("collection %s does not have a field", collectionName)
	}

	indexes := make([]*sql.Index, 0)
	indexes = append(indexes, table.GetIndexes()...)

	return indexes, nil
}

// generateExp generates a boolean expression from a list of expressions.
func (d *Engine) generateExp(ctx context.Context, collection string, expressions []*Query) (*sql.CmpBoolExp, error) {
	if len(expressions) == 0 {
		return nil, nil
	}

	tcols, err := d.getCollectionSchema(ctx, collection)
	if err != nil {
		return nil, err
	}

	// Convert each expression to a boolean expression.
	boolExps := make([]*sql.CmpBoolExp, len(expressions))
	for i, exp := range expressions {
		fieldType, ok := tcols[exp.Field]
		if !ok {
			return nil, fmt.Errorf("unsupported field %v", exp.Field)
		}
		value, err := valueTypeToExp(fieldType.Type(), exp.Value)
		if err != nil {
			return nil, err
		}

		colSelector := sql.NewColSelector(d.CurrentDatabase(), collection, exp.Field, "")
		boolExps[i] = sql.NewCmpBoolExp(int(exp.Operator), colSelector, value)
	}

	// Combine boolean expressions using AND operator.
	result := sql.NewCmpBoolExp(sql.AND, boolExps[0].Left(), boolExps[0].Right())

	for _, exp := range boolExps[1:] {
		result = sql.NewCmpBoolExp(sql.AND, result, exp)
	}

	return result, nil
}

func (d *Engine) getCollectionSchema(ctx context.Context, collection string) (map[string]*sql.Column, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.CurrentDatabase(), collection)
	if err != nil {
		return nil, err
	}

	return table.ColsByName(), nil
}

func (d *Engine) GetDocument(ctx context.Context, collectionName string, queries []*Query, maxResultSize int) ([]*structpb.Struct, error) {
	exp, err := d.generateExp(ctx, collectionName, queries)
	if err != nil {
		return nil, err
	}

	op := sql.NewSelectStmt(
		d.CurrentDatabase(),
		collectionName,
		exp,
	)

	r, err := d.QueryPreparedStmt(ctx, nil, op, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	colDescriptors, err := r.Columns(ctx)
	if err != nil {
		return nil, err
	}

	results := []*structpb.Struct{}

	for l := 1; ; l++ {
		row, err := r.Read(ctx)
		if err == sql.ErrNoMoreRows {
			break
		}
		if err != nil {
			return nil, err
		}

		document := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for i := range colDescriptors {
			v := row.ValuesByPosition[i]
			vtype, err := valueTypeToFieldValue(v)
			if err != nil {
				return nil, err
			}
			document.Fields[colDescriptors[i].Column] = vtype
		}
		results = append(results, document)

		if l == maxResultSize {
			return nil, fmt.Errorf("%w: found at least %d documents (the maximum limit). "+
				"Query constraints can be applied using the LIMIT clause",
				errors.New("result size limit reached"), maxResultSize)
		}
	}

	return results, nil
}

type Audit struct {
	Value []byte
	TxID  uint64
}

// DocumentAudit returns the audit history of a document.
func (d *Engine) DocumentAudit(ctx context.Context, collectionName string, documentID DocumentID, maxCount int) ([]*Audit, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	table, err := tx.Catalog().GetTableByName(d.CurrentDatabase(), collectionName)
	if err != nil {
		return nil, err
	}

	var searchKey []byte

	valbuf := bytes.Buffer{}
	for _, col := range table.PrimaryIndex().Cols() {
		if col.Name() == defaultDocumentIDField {
			rval := sql.NewBlob(documentID[:])
			encVal, err := sql.EncodeAsKey(rval.Value(), col.Type(), col.MaxLen())
			if err != nil {
				return nil, err
			}
			_, err = valbuf.Write(encVal)
			if err != nil {
				return nil, err
			}
		}
	}
	pkEncVals := valbuf.Bytes()

	searchKey = sql.MapKey(
		d.GetPrefix(),
		sql.PIndexPrefix,
		sql.EncodeID(table.Database().ID()),
		sql.EncodeID(table.ID()),
		sql.EncodeID(table.PrimaryIndex().ID()),
		pkEncVals,
	)

	txs, _, err := d.GetStore().History(searchKey, 0, false, maxCount)
	if err != nil {
		return nil, err
	}

	results := make([]*Audit, 0)
	for _, txID := range txs {
		res, err := d.getValueAt(searchKey, txID, d.GetStore(), false)
		if err != nil {
			return nil, err
		}

		v := res.Value
		voff := 0

		cols := int(binary.BigEndian.Uint32(v[voff:]))
		voff += sql.EncLenLen

		for i := 0; i < cols; i++ {
			colID := binary.BigEndian.Uint32(v[voff:])
			voff += sql.EncIDLen

			col, err := table.GetColumnByID(colID)
			if err != nil {
				return nil, err
			}

			val, n, err := sql.DecodeValue(v[voff:], sql.BLOBType)
			if col.Name() == defaultDocumentBLOBField {
				res.Value = val.Value().([]byte)
				break
			}
			voff += n
		}

		results = append(results, res)
	}

	return results, nil
}

func (d *Engine) getValueAt(key []byte, atTx uint64, index store.KeyIndex, skipIntegrityCheck bool) (entry *Audit, err error) {
	var txID uint64
	var val []byte

	if atTx == 0 {
		valRef, err := index.Get(key)
		if err != nil {
			return nil, err
		}

		txID = valRef.Tx()

		val, err = valRef.Resolve()
		if err != nil {
			return nil, err
		}
	} else {
		txID = atTx
		_, val, err = d.readMetadataAndValue(key, atTx, skipIntegrityCheck)
		if err != nil {
			return nil, err
		}
	}

	return &Audit{
		TxID:  txID,
		Value: val,
	}, err
}

func (d *Engine) readMetadataAndValue(key []byte, atTx uint64, skipIntegrityCheck bool) (*store.KVMetadata, []byte, error) {
	store := d.GetStore()
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
