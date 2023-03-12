package document

import (
	"context"
	"errors"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	schemav2 "github.com/codenotary/immudb/pkg/api/documentschema"
	"github.com/codenotary/immudb/pkg/api/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultDocumentBLOBField = "_obj"
)

// Schema to ValueType mapping
var (
	schemaToValueType = map[schemav2.IndexType]sql.SQLValueType{
		schemav2.IndexType_STRING:  sql.VarcharType,
		schemav2.IndexType_INTEGER: sql.IntegerType,
	}

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
)

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
func (d *Engine) CreateCollection(ctx context.Context, collectionName string, pkeys map[string]*schemav2.IndexOption, idxKeys map[string]*schemav2.IndexOption) error {
	primaryKeys := make([]string, 0)
	collectionSpec := make([]*sql.ColSpec, 0, len(pkeys))

	// validate collection to contain at least one primary key
	if len(pkeys) == 0 {
		return fmt.Errorf("collection must have at least one primary key")
	}

	// validate primary keys
	for name, pk := range pkeys {
		schType, isValid := schemaToValueType[pk.Type]
		if !isValid {
			return fmt.Errorf("invalid primary key type: %v", pk)
		}
		primaryKeys = append(primaryKeys, name)
		// TODO: add support for other types
		// TODO: add support for auto increment
		collectionSpec = append(collectionSpec, sql.NewColSpec(name, schType, 0, false, false))
	}

	// add support for blob
	// TODO: add support for max length for blob storage
	collectionSpec = append(collectionSpec, sql.NewColSpec(defaultDocumentBLOBField, sql.BLOBType, 0, false, false))

	_, _, err := d.ExecPreparedStmts(
		context.Background(),
		nil,
		[]sql.SQLStmt{sql.NewCreateTableStmt(
			collectionName,
			false,
			collectionSpec,
			primaryKeys,
		)},
		nil,
	)
	return err
}

func (d *Engine) CreateDocument(ctx context.Context, collectionName string, documents []*structpb.Struct) (string, error) {
	tx, err := d.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return "", err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.CurrentDatabase(), collectionName)
	if err != nil {
		return "", err
	}

	cols := make([]string, 0)
	tcolumns := table.ColsByName()
	rows := make([]*sql.RowSpec, 0)

	for _, col := range tcolumns {
		cols = append(cols, col.Name())
	}

	// TODO: should be able to send only a single document
	for _, doc := range documents {
		values := make([]sql.ValueExp, 0)
		for _, col := range tcolumns {
			switch col.Name() {
			case defaultDocumentBLOBField:
				document, err := NewDocumentFrom(doc)
				if err != nil {
					return "", err
				}
				res, err := document.MarshalJSON()
				if err != nil {
					return "", err
				}
				values = append(values, sql.NewBlob(res))
			default:
				if rval, ok := doc.Fields[col.Name()]; ok {
					valType, err := valueTypeToExp(col.Type(), rval)
					if err != nil {
						return "", err
					}
					values = append(values, valType)
				}
			}
		}

		if len(values) > 0 {
			rows = append(rows, sql.NewRowSpec(values))
		}
	}

	// add documents to collection
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

	return "", err
}

// TODO: remove schema response from this function
func (d *Engine) GetCollection(ctx context.Context, collectionName string) (resp *schemav2.CollectionInformation, err error) {
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
	indexes := table.GetIndexes()
	if len(indexes) == 0 {
		return nil, fmt.Errorf("collection %s does not have a indexes", collectionName)
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

// GenerateBinBoolExp generates a boolean expression from a list of expressions.
func (d *Engine) GenerateBinBoolExp(ctx context.Context, collection string, expressions []*schemav2.DocumentQuery) (*sql.BinBoolExp, error) {
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

		colSelector := sql.NewColSelector(d.CurrentDatabase(), collection, exp.GetField(), "")

		boolExps[i] = sql.NewCmpBoolExp(int(exp.Operator), colSelector, value)
	}

	// Combine boolean expressions using AND operator.
	result := sql.NewBinBoolExp(sql.AND, boolExps[0].Left(), boolExps[0].Right())

	for _, exp := range boolExps[1:] {
		result = sql.NewBinBoolExp(sql.AND, result, exp)
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

func (d *Engine) GetDocument(ctx context.Context, collectionName string, queries []*schemav2.DocumentQuery, maxResultSize int) (*schemav2.DocumentSearchResponse, error) {
	exp, err := d.GenerateBinBoolExp(ctx, collectionName, queries)
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

	cols := make([]*schema.Column, len(colDescriptors))

	for i, c := range colDescriptors {
		dbname := c.Database
		if c.Database == "dbinstance" {
			// TODO: Check what the db name is
			// dbname = d.name
			dbname = d.CurrentDatabase()
		}

		des := &sql.ColDescriptor{
			AggFn:    c.AggFn,
			Database: dbname,
			Table:    c.Table,
			Column:   c.Column,
			Type:     c.Type,
		}
		cols[i] = &schema.Column{Name: des.Column, Type: des.Type}
	}

	resp := &schemav2.DocumentSearchResponse{Results: []*structpb.Struct{}}

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
			document.Fields[cols[i].Name] = vtype
		}
		resp.Results = append(resp.Results, document)

		if l == maxResultSize {
			return nil, fmt.Errorf("%w: found at least %d documents (the maximum limit). "+
				"Query constraints can be applied using the LIMIT clause",
				errors.New("result size limit reached"), maxResultSize)
		}
	}

	return resp, nil
}
