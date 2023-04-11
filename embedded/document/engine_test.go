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
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func makeEngine(t *testing.T) *Engine {
	st, err := store.Open(t.TempDir(), store.DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		err := st.Close()
		if !t.Failed() {
			// Do not pollute error output if test has already failed
			require.NoError(t, err)
		}
	})

	opts := sql.DefaultOptions()
	engine, err := NewEngine(st, opts)
	require.NoError(t, err)

	return engine
}

func TestListCollections(t *testing.T) {
	engine := makeEngine(t)

	collections := []string{"mycollection1", "mycollection2", "mycollection3"}
	for _, collectionName := range collections {
		err := engine.CreateCollection(
			context.Background(),
			collectionName,
			map[string]sql.SQLValueType{
				"number":  sql.IntegerType,
				"name":    sql.BLOBType,
				"pin":     sql.IntegerType,
				"country": sql.VarcharType,
			},
		)
		require.NoError(t, err)
	}

	collectionList, err := engine.ListCollections(context.Background())
	require.NoError(t, err)
	require.Equal(t, len(collections), len(collectionList))

	for _, indexes := range collectionList {
		primaryKeyCount := 0
		indexKeyCount := 0
		for _, idx := range indexes {
			// check if primary key
			if idx.IsPrimary() {
				primaryKeyCount += len(idx.Cols())
			} else {
				indexKeyCount += len(idx.Cols())
			}
		}
		require.Equal(t, 1, primaryKeyCount)
		require.Equal(t, 4, indexKeyCount)
	}
}

func TestCreateCollection(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"
	err := engine.CreateCollection(
		context.Background(),
		collectionName,
		map[string]sql.SQLValueType{
			"number":  sql.Float64Type,
			"name":    sql.VarcharType,
			"pin":     sql.IntegerType,
			"country": sql.VarcharType,
		},
	)
	require.NoError(t, err)

	// creating collection with the same name should throw error
	err = engine.CreateCollection(
		context.Background(),
		collectionName,
		nil,
	)
	require.ErrorIs(t, err, sql.ErrTableAlreadyExists)

	catalog, err := engine.sqlEngine.Catalog(context.Background(), nil)
	require.NoError(t, err)

	table, err := catalog.GetTableByName(collectionName)
	require.NoError(t, err)

	require.Equal(t, collectionName, table.Name())

	pcols := []string{"_id"}
	idxcols := []string{"pin", "country", "number", "name"}

	// verify primary keys
	for _, col := range pcols {
		c, err := table.GetColumnByName(col)
		require.NoError(t, err)
		require.Equal(t, c.Name(), col)
	}

	// verify index keys
	for _, col := range idxcols {
		c, err := table.GetColumnByName(col)
		require.NoError(t, err)
		require.Equal(t, c.Name(), col)
	}

	// get collection
	indexes, err := engine.GetCollection(context.Background(), collectionName)
	require.NoError(t, err)
	require.Equal(t, 5, len(indexes))

	primaryKeyCount := 0
	indexKeyCount := 0
	for _, idx := range indexes {
		// check if primary key
		if idx.IsPrimary() {
			primaryKeyCount += len(idx.Cols())
		} else {
			indexKeyCount += len(idx.Cols())
		}
	}
	require.Equal(t, 1, primaryKeyCount)
	require.Equal(t, 4, indexKeyCount)
}

func TestGetDocument(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"pincode": sql.IntegerType,
		"country": sql.VarcharType,
		"data":    sql.BLOBType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add document to collection
	_, _, err = engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value1"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	expressions := []*Query{
		{
			Field:    "country",
			Operator: 0, // EQ
			Value: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
		},
		{
			Field:    "pincode",
			Operator: 0, // EQ
			Value: &structpb.Value{
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
		},
	}

	// invalid page number
	_, err = engine.GetDocuments(context.Background(), collectionName, expressions, 0, 10)
	require.Error(t, err)

	// invalid page limit
	_, err = engine.GetDocuments(context.Background(), collectionName, expressions, 1, 0)
	require.Error(t, err)

	doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(doc))
}

func TestDocumentAudit(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"pincode": sql.IntegerType,
		"country": sql.VarcharType,
	})
	require.NoError(t, err)

	// add document to collection
	docID, _, err := engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value1"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	_, revision, err := engine.UpdateDocument(context.Background(), collectionName, docID, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"pincode": {
				Kind: &structpb.Value_NumberValue{NumberValue: 2},
			},
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value2"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), revision)

	// get document audit
	res, err := engine.DocumentAudit(context.Background(), collectionName, docID, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 2, len(res))

	for i, record := range res {
		// verify audit result
		val := string(record.Value)
		require.Contains(t, val, "pincode")
		require.Contains(t, val, "country")
		require.Equal(t, uint64(i+1), record.Revision)
	}
}

func TestQueryDocument(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"pincode": sql.IntegerType,
		"country": sql.VarcharType,
		"idx":     sql.IntegerType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add documents to collection
	for i := 1.0; i <= 10; i++ {
		_, _, err = engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"pincode": {
					Kind: &structpb.Value_NumberValue{NumberValue: i},
				},
				"country": {
					Kind: &structpb.Value_StringValue{StringValue: fmt.Sprintf("country-%d", int(i))},
				},
				"idx": {
					Kind: &structpb.Value_NumberValue{NumberValue: i},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("test query with != operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.NE,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 5},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 9, len(doc))
	})

	t.Run("test query with < operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.LT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 11},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 10, len(doc))
	})

	t.Run("test query with <= operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.LE,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 9},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 9, len(doc))
	})

	t.Run("test query with > operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.GT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 5},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 5, len(doc))
	})

	t.Run("test query with >= operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.GE,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 10},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 1, len(doc))
	})

	t.Run("test group query with != operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.NE,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 5},
				},
			},
			{
				Field:    "country",
				Operator: sql.NE,
				Value: &structpb.Value{
					Kind: &structpb.Value_StringValue{StringValue: "country-6"},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 8, len(doc))
	})

	t.Run("test group query with < operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "pincode",
				Operator: sql.LT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 11},
				},
			},
			{
				Field:    "idx",
				Operator: sql.LT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 5},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 4, len(doc))
	})

	t.Run("test group query with > operator", func(t *testing.T) {
		expressions := []*Query{
			{
				Field:    "idx",
				Operator: sql.GT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 7},
				},
			},
			{
				Field:    "pincode",
				Operator: sql.GT,
				Value: &structpb.Value{
					Kind: &structpb.Value_NumberValue{NumberValue: 5},
				},
			},
		}

		doc, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 20)
		require.NoError(t, err)
		require.Equal(t, 3, len(doc))
	})

}

func TestDocumentUpdate(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"country": sql.VarcharType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add document to collection
	docID, _, err := engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value1"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)

	_, revision, err := engine.UpdateDocument(context.Background(), collectionName, docID, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"country": {
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
			"data": {
				Kind: &structpb.Value_StructValue{StructValue: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"key1": {Kind: &structpb.Value_StringValue{StringValue: "value2"}},
					},
				}},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), revision)

	expressions := []*Query{
		{
			Field:    "country",
			Operator: sql.EQ,
			Value: &structpb.Value{
				Kind: &structpb.Value_StringValue{StringValue: "wonderland"},
			},
		},
	}

	// check if document is updated
	docs, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(docs))

	// check if data is updated
	doc := docs[0]
	data := map[string]interface{}{}
	err = json.Unmarshal([]byte(doc.Fields[DocumentBLOBField].GetStringValue()), &data)
	require.NoError(t, err)
	require.Equal(t, "value2", data["data"].(map[string]interface{})["key1"])
}

func TestFloatSupport(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"
	err := engine.CreateCollection(
		context.Background(),
		collectionName,
		map[string]sql.SQLValueType{
			"number": sql.Float64Type,
		},
	)
	require.NoError(t, err)

	catalog, err := engine.sqlEngine.Catalog(context.Background(), nil)
	require.NoError(t, err)

	table, err := catalog.GetTableByName(collectionName)
	require.NoError(t, err)
	require.Equal(t, collectionName, table.Name())

	col, err := table.GetColumnByName("number")
	require.NoError(t, err)
	require.Equal(t, sql.Float64Type, col.Type())

	// add document to collection
	_, _, err = engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"number": {
				Kind: &structpb.Value_NumberValue{NumberValue: 3.1},
			},
		},
	})
	require.NoError(t, err)

	// query document
	expressions := []*Query{
		{
			Field:    "number",
			Operator: sql.EQ,
			Value: &structpb.Value{
				Kind: &structpb.Value_NumberValue{NumberValue: 3.1},
			},
		},
	}

	// check if document is updated
	docs, err := engine.GetDocuments(context.Background(), collectionName, expressions, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, len(docs))

	// retrieve document
	doc := docs[0]
	data := map[string]interface{}{}
	err = json.Unmarshal([]byte(doc.Fields[DocumentBLOBField].GetStringValue()), &data)
	require.NoError(t, err)
	require.Equal(t, 3.1, data["number"])
}

func TestDeleteCollection(t *testing.T) {
	engine := makeEngine(t)

	// create collection
	collectionName := "mycollection"
	err := engine.CreateCollection(context.Background(), collectionName, map[string]sql.SQLValueType{
		"idx": sql.IntegerType,
	})
	require.NoError(t, err)
	require.NoError(t, err)

	// add documents to collection
	for i := 1.0; i <= 10; i++ {
		_, _, err = engine.CreateDocument(context.Background(), collectionName, &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"idx": {
					Kind: &structpb.Value_NumberValue{NumberValue: i},
				},
			},
		})
		require.NoError(t, err)
	}

	t.Run("delete collection and check if it is empty", func(t *testing.T) {
		err = engine.DeleteCollection(context.Background(), collectionName)
		require.NoError(t, err)

		_, err := engine.sqlEngine.Query(context.Background(), nil, "SELECT COUNT(*) FROM mycollection", nil)
		require.ErrorIs(t, err, sql.ErrTableDoesNotExist)

		collectionList, err := engine.ListCollections(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, len(collectionList))
	})
}

func TestUpdateCollection(t *testing.T) {
	engine := makeEngine(t)

	collectionName := "mycollection"

	t.Run("create collection and add index", func(t *testing.T) {
		err := engine.CreateCollection(
			context.Background(),
			collectionName,
			map[string]sql.SQLValueType{
				"number":  sql.Float64Type,
				"name":    sql.VarcharType,
				"pin":     sql.IntegerType,
				"country": sql.VarcharType,
			},
		)
		require.NoError(t, err)
	})

	t.Run("update collection by deleting indexes", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			collectionName,
			nil,
			[]string{"number", "name"},
		)
		require.NoError(t, err)

		// get collection
		indexes, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Equal(t, 3, len(indexes))

		primaryKeyCount := 0
		indexKeyCount := 0
		for _, idx := range indexes {
			// check if primary key
			if idx.IsPrimary() {
				primaryKeyCount += len(idx.Cols())
			} else {
				indexKeyCount += len(idx.Cols())
			}
		}
		require.Equal(t, 1, primaryKeyCount)
		require.Equal(t, 2, indexKeyCount)

	})

	t.Run("update collection by adding indexes", func(t *testing.T) {
		// update collection
		err := engine.UpdateCollection(
			context.Background(),
			collectionName,
			map[string]sql.SQLValueType{
				"data1": sql.VarcharType,
				"data2": sql.VarcharType,
				"data3": sql.VarcharType,
			},
			nil,
		)
		require.NoError(t, err)

		// get collection
		indexes, err := engine.GetCollection(context.Background(), collectionName)
		require.NoError(t, err)
		require.Equal(t, 6, len(indexes))

		primaryKeyCount := 0
		indexKeyCount := 0
		for _, idx := range indexes {
			// check if primary key
			if idx.IsPrimary() {
				primaryKeyCount += len(idx.Cols())
			} else {
				indexKeyCount += len(idx.Cols())
			}
		}
		require.Equal(t, 1, primaryKeyCount)
		require.Equal(t, 5, indexKeyCount)
	})

}
