package database

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/embedded/object"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	schemav2 "github.com/codenotary/immudb/pkg/api/schemav2"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	defaultObjectBLOBField = "_obj"
)

// Schema to ValueType mapping
var (
	SchemaToValueType = map[schemav2.PossibleIndexType]sql.SQLValueType{
		schemav2.PossibleIndexType_STRING:  sql.VarcharType,
		schemav2.PossibleIndexType_INTEGER: sql.IntegerType,
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

// TODO: make new objectdb to embed object engine commands
// GetCollection returns the collection schema
func (d *db) GetCollection(ctx context.Context, req *schemav2.CollectionGetRequest) (*schemav2.CollectionInformation, error) {
	return nil, nil
}

// CreateCollection creates a new collection
func (d *db) CreateCollection(ctx context.Context, req *schemav2.CollectionCreateRequest) error {
	collectionName := req.Name
	primaryKeys := make([]string, 0)
	collectionSpec := make([]*sql.ColSpec, 0, len(req.PrimaryKeys))

	for name, pk := range req.PrimaryKeys {
		schType, isValid := SchemaToValueType[pk]
		if !isValid {
			return fmt.Errorf("invalid primary key type: %v", pk)
		}
		primaryKeys = append(primaryKeys, name)
		// TODO: add support for other types
		// TODO: add support for max length
		// TODO: add support for auto increment
		collectionSpec = append(collectionSpec, sql.NewColSpec(name, schType, 0, false, false))
	}

	// add support for blob
	// TODO: add support for max length for blob storage
	collectionSpec = append(collectionSpec, sql.NewColSpec(defaultObjectBLOBField, sql.BLOBType, 10000, false, false))

	_, _, err := d.objectEngine.ExecPreparedStmts(
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

func (d *db) getCollectionSchema(ctx context.Context, collection string) (map[string]*sql.Column, error) {
	tx, err := d.objectEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.objectEngine.CurrentDatabase(), collection)
	if err != nil {
		return nil, err
	}

	return table.ColsByName(), nil
}

// CreateDocument creates a new document
func (d *db) CreateDocument(ctx context.Context, req *schemav2.DocumentInsertRequest) (string, error) {
	tx, err := d.objectEngine.NewTx(ctx, sql.DefaultTxOptions().WithReadOnly(true))
	if err != nil {
		return "", err
	}
	defer tx.Cancel()

	// check if collection exists
	table, err := tx.Catalog().GetTableByName(d.objectEngine.CurrentDatabase(), req.Collection)
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
	for _, doc := range req.Document {
		values := make([]sql.ValueExp, 0)
		for _, col := range tcolumns {
			switch col.Name() {
			case defaultObjectBLOBField:
				document, err := object.NewDocumentFrom(doc)
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
			// if rval, ok := doc.Fields[col.Name()]; ok && col.Name() != "_obj" {
			// 	valType, err := valueTypeToExp(col.Type(), rval)
			// 	if err != nil {
			// 		return "", err
			// 	}
			// 	values = append(values, valType)
			// }
			// if col.Name() == "_obj" {
			// 	document, err := object.NewDocumentFrom(doc)
			// 	if err != nil {
			// 		return "", err
			// 	}
			// 	res, err := document.MarshalJSON()
			// 	if err != nil {
			// 		return "", err
			// 	}
			// 	values = append(values, sql.NewBlob(res))
			// }
		}

		if len(values) > 0 {
			rows = append(rows, sql.NewRowSpec(values))
		}
	}

	// add documents to collection
	_, _, err = d.objectEngine.ExecPreparedStmts(
		ctx,
		nil,
		[]sql.SQLStmt{
			sql.NewUpserIntoStmt(
				d.objectEngine.CurrentDatabase(),
				req.Collection,
				cols,
				rows,
				nil,
			),
		},
		nil,
	)

	return "", err
}

// GetDocument returns the document
func (d *db) GetDocument(ctx context.Context, req *schemav2.DocumentSearchRequest) (*schemav2.DocumentSearchResponse, error) {
	exp, err := d.generateBinBoolExp(ctx, req.Collection, req.Query)
	if err != nil {
		return nil, err
	}

	op := sql.NewSelectStmt(
		d.objectEngine.CurrentDatabase(),
		req.Collection,
		exp,
	)

	r, err := d.objectEngine.QueryPreparedStmt(ctx, nil, op, nil)
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
		if c.Database == dbInstanceName {
			dbname = d.name
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

		if l == d.maxResultSize {
			return nil, fmt.Errorf("%w: found at least %d rows (the maximum limit). "+
				"Query constraints can be applied using the LIMIT clause",
				ErrResultSizeLimitReached, d.maxResultSize)
		}
	}

	return resp, nil
}

// generateBinBoolExp generates a boolean expression from a list of expressions.
/*
	TODO: remove this
	An example format is below
		/*
		where: &BinBoolExp{
			op: AND,
			left: &BinBoolExp{
				op: AND,
				left: &CmpBoolExp{
					op: EQ,
					left: &ColSelector{
						col: "country",
					},
					right: &Varchar{val: "US"},
				},
				right: &CmpBoolExp{
					op: LE,
					left: &ColSelector{
						col: "time",
					},
					right: &FnCall{fn: "now"},
				},
			},
			right: &CmpBoolExp{
				op: EQ,
				left: &ColSelector{
					col: "name",
				},
				right: &Param{id: "pname"},
			},
		},

*/
func (d *db) generateBinBoolExp(ctx context.Context, collection string, expressions []*schemav2.DocumentQuery) (*sql.BinBoolExp, error) {
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

		colSelector := sql.NewColSelector(d.objectEngine.CurrentDatabase(), collection, exp.GetField(), "")

		boolExps[i] = sql.NewCmpBoolExp(int(exp.Operator), colSelector, value)
	}

	// Combine boolean expressions using AND operator.
	result := sql.NewBinBoolExp(sql.AND, boolExps[0].Left(), boolExps[0].Right())

	for _, exp := range boolExps[1:] {
		result = sql.NewBinBoolExp(sql.AND, result, exp)
	}

	return result, nil
}
