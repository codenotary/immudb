/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromEmptyCatalog(t *testing.T) {
	catalog := newCatalog()

	dbs := catalog.Databases()
	require.Empty(t, dbs)

	exists := catalog.ExistDatabase("db1")
	require.False(t, exists)

	_, err := catalog.GetDatabaseByID(1)
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	_, err = catalog.GetDatabaseByName("db1")
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	_, err = catalog.GetTableByName("db1", "table1")
	require.Equal(t, ErrDatabaseDoesNotExist, err)

	db, err := catalog.newDatabase(2, "db1")
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, uint32(2), db.id)
	require.Equal(t, "db1", db.Name())
	require.Empty(t, db.GetTables())

	dbs = catalog.Databases()
	require.NotNil(t, db)
	require.Len(t, dbs, 1)
	require.Equal(t, "db1", dbs[0].Name())

	db1, err := catalog.GetDatabaseByID(2)
	require.NoError(t, err)
	require.Equal(t, db.name, db1.name)

	_, err = catalog.GetDatabaseByName("db1")
	require.NoError(t, err)

	_, err = catalog.newDatabase(2, "db1")
	require.Equal(t, ErrDatabaseAlreadyExists, err)

	exists = db.ExistTable("table1")
	require.False(t, exists)

	_, err = db.GetTableByID(1)
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = db.GetTableByName("table1")
	require.ErrorIs(t, err, ErrTableDoesNotExist)

	_, err = db.newTable("", nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.newTable("table1", nil)
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.newTable("table1", []*ColSpec{})
	require.Equal(t, ErrIllegalArguments, err)

	_, err = db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}, {colName: "id", colType: IntegerType}})
	require.Equal(t, ErrDuplicatedColumn, err)

	table, err := db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}, {colName: "title", colType: IntegerType}})
	require.NoError(t, err)
	require.Equal(t, "table1", table.Name())

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
	require.Equal(t, ErrTableDoesNotExist, err)

	_, err = db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}, {colName: "title", colType: IntegerType}})
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
	require.Equal(t, ErrColumnDoesNotExist, err)

	_, err = table.newIndex(true, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = table.newIndex(true, []uint32{1, 2, 1})
	require.ErrorIs(t, err, ErrDuplicatedColumn)

}
