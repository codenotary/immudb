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
	"testing"

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

	_, err = db.newTable("", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", []*ColSpec{})
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = db.newTable("table1", []*ColSpec{{colName: "id", colType: IntegerType}, {colName: "id", colType: IntegerType}})
	require.ErrorIs(t, err, ErrDuplicatedColumn)

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
	require.ErrorIs(t, err, ErrTableDoesNotExist)

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
	require.ErrorIs(t, err, ErrColumnDoesNotExist)

	_, err = table.newIndex(true, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = table.newIndex(true, []uint32{1, 2, 1})
	require.ErrorIs(t, err, ErrDuplicatedColumn)

}
