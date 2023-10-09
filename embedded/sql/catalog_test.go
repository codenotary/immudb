/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package sql

import (
	"context"
	"strings"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestFromEmptyCatalog(t *testing.T) {
	db := newCatalog(nil)

	_, err := db.GetTableByName("table1")
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	exists := db.ExistTable("table1")
	require.False(t, exists)

	_, err = db.GetTableByID(1)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = db.GetTableByName("table1")
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = db.newTable("", nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", nil, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", map[uint32]*ColSpec{}, 0)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", map[uint32]*ColSpec{
		1: {colName: "id", colType: IntegerType},
		2: {colName: "id", colType: IntegerType},
	}, 2)
	require.ErrorIs(t, err, ErrDuplicatedColumn)

	table, err := db.newTable("table1", map[uint32]*ColSpec{
		1: {colName: "id", colType: IntegerType},
		2: {colName: "title", colType: IntegerType},
	}, 2)
	require.NoError(t, err)
	require.Equal(t, "table1", table.Name())

	_, err = table.newColumn(&ColSpec{colName: revCol, colType: IntegerType})
	require.ErrorIs(t, err, ErrReservedWord)

	_, err = table.newIndex(true, []uint32{1})
	require.NoError(t, err)

	tables := db.GetTables()
	require.Len(t, tables, 1)
	require.Equal(t, table.Name(), tables[0].Name())

	table1, err := db.GetTableByID(1)
	require.NoError(t, err)
	require.Equal(t, "table1", table1.Name())

	_, err = db.GetTableByName("table1")
	require.NoError(t, err)

	_, err = db.GetTableByID(2)
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = db.newTable("table1", map[uint32]*ColSpec{
		1: {colName: "id", colType: IntegerType},
		2: {colName: "title", colType: IntegerType},
	}, 2)
	require.ErrorIs(t, err, ErrTableAlreadyExists)

	indexed, err := table.IsIndexed("id")
	require.NoError(t, err)
	require.True(t, indexed)

	_, err = table.IsIndexed("id1")
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	pk := table.PrimaryIndex()
	require.NotNil(t, pk)
	require.Len(t, pk.cols, 1)
	require.Equal(t, pk.cols[0].colName, "id")
	require.Equal(t, pk.cols[0].colType, IntegerType)

	c, err := table.GetColumnByID(1)
	require.NoError(t, err)
	require.Equal(t, c.Name(), "id")

	c, err = table.GetColumnByID(2)
	require.NoError(t, err)
	require.Equal(t, c.Name(), "title")

	_, err = table.GetColumnByID(3)
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, err = table.newIndex(true, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = table.newIndex(true, []uint32{1, 2, 1})
	require.ErrorIs(t, err, ErrDuplicatedColumn)

}

func TestEncodeRawValueAsKey(t *testing.T) {
	t.Run("encoded int keys should preserve lex order", func(t *testing.T) {
		var prevEncKey []byte

		for i := 0; i < 10; i++ {
			encKey, n, err := EncodeRawValueAsKey(int64(i), IntegerType, 8)
			require.NoError(t, err)
			require.Greater(t, encKey, prevEncKey)
			require.Equal(t, 8, n)

			prevEncKey = encKey
		}
	})

	t.Run("encoded varchar keys should preserve lex order", func(t *testing.T) {
		var prevEncKey []byte

		for _, v := range []string{"key1", "key11", "key2", "key3"} {
			encKey, n, err := EncodeRawValueAsKey(v, VarcharType, 10)
			require.NoError(t, err)
			require.Greater(t, encKey, prevEncKey)
			require.Equal(t, len(v), n)

			prevEncKey = encKey
		}
	})
}

func TestCatalogTableLength(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)
	defer closeStore(t, st)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	totalTablesCount := uint32(0)

	for _, v := range []string{"table1", "table2", "table3"} {
		_, _, err = engine.Exec(
			context.Background(), nil,
			`
			CREATE TABLE `+v+` (
				id INTEGER AUTO_INCREMENT,
				PRIMARY KEY(id)
			)`, nil)
		require.NoError(t, err)
		totalTablesCount++
	}

	t.Run("table count should be 3 on catalog reload", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
			require.NoError(t, err)
			defer tx.Cancel()

			require.Equal(t, totalTablesCount, tx.catalog.maxTableID)
		}
	})

	t.Run("table count should not increase on adding existing table", func(t *testing.T) {
		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()
		catlog := tx.catalog

		for _, v := range []string{"table1", "table2", "table3"} {
			_, err := catlog.newTable(v, map[uint32]*ColSpec{
				1: {colName: "id", colType: IntegerType},
			}, 1)
			require.ErrorIs(t, err, ErrTableAlreadyExists)
		}
		require.Equal(t, totalTablesCount, catlog.maxTableID)
	})

	t.Run("table count should increase on using newTable function on catalog", func(t *testing.T) {
		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()
		catlog := tx.catalog

		for _, v := range []string{"table4", "table5", "table6"} {
			_, err := catlog.newTable(v, map[uint32]*ColSpec{
				1: {colName: "id", colType: IntegerType},
			}, 1,
			)
			require.NoError(t, err)
		}
		require.Equal(t, totalTablesCount+3, catlog.maxTableID)
	})

	t.Run("table count should increase on adding new table", func(t *testing.T) {
		for _, v := range []string{"table4", "table5", "table6"} {
			_, _, err = engine.Exec(
				context.Background(), nil,
				`
				CREATE TABLE `+v+` (
					id INTEGER AUTO_INCREMENT,
					PRIMARY KEY(id)
				)`, nil)
			require.NoError(t, err)
			totalTablesCount++
		}
	})

	t.Run("table count should not decrease on dropping table", func(t *testing.T) {
		deleteTables := []string{"table1", "table2", "table3"}
		activeTables := []string{"table4", "table5", "table6"}
		for _, v := range deleteTables {
			_, _, err := engine.ExecPreparedStmts(
				context.Background(),
				nil,
				[]SQLStmt{
					NewDropTableStmt(v), // delete collection from catalog
				},
				nil,
			)
			require.NoError(t, err)
		}

		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()
		catlog := tx.catalog

		// ensure that catalog has been reloaded with deleted table count
		require.Equal(t, totalTablesCount, catlog.maxTableID)

		for _, v := range activeTables {
			require.True(t, catlog.ExistTable(v))
		}

		for _, v := range deleteTables {
			require.False(t, catlog.ExistTable(v))
		}
	})

	t.Run("adding new table should increase table count", func(t *testing.T) {
		tableName := "table7"
		_, _, err = engine.Exec(
			context.Background(), nil,
			`
				CREATE TABLE `+tableName+` (
					id INTEGER AUTO_INCREMENT,
					PRIMARY KEY(id)
				)`, nil)
		require.NoError(t, err)
		totalTablesCount++

		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()
		catlog := tx.catalog

		tab, err := catlog.GetTableByName(tableName)
		require.NoError(t, err)
		require.Equal(t, totalTablesCount, tab.id)

		tab, err = catlog.GetTableByID(7)
		require.NoError(t, err)
		require.Equal(t, totalTablesCount, tab.id)

		_, err = tab.GetIndexByName("invalid_index")
		require.ErrorIs(t, err, ErrIndexNotFound)
	})

	t.Run("cancelling a transaction should not increase table count", func(t *testing.T) {
		// create a new transaction
		tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		sql := `
		CREATE TABLE table10 (
			id INTEGER AUTO_INCREMENT,
			PRIMARY KEY(id)
		)
		`
		stmts, err := Parse(strings.NewReader(sql))
		require.NoError(t, err)
		require.Equal(t, 1, len(stmts))
		stmt := stmts[0]

		// execute the create table statement
		stx, err := stmt.execAt(context.Background(), tx, nil)
		require.NoError(t, err)

		// cancel the transaction instead of committing it
		require.Equal(t, totalTablesCount+1, stx.catalog.maxTableID)
		require.NoError(t, stx.Cancel())

		// reload a fresh catalog
		tx, err = engine.NewTx(context.Background(), DefaultTxOptions())
		require.NoError(t, err)
		defer tx.Cancel()
		catlog := tx.catalog

		// table count should not increase
		require.Equal(t, totalTablesCount, catlog.maxTableID)
	})

}
