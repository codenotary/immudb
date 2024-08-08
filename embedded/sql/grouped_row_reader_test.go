/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sql

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestGroupedRowReader(t *testing.T) {
	st, err := store.Open(t.TempDir(), store.DefaultOptions().WithMultiIndexing(true))
	require.NoError(t, err)

	engine, err := NewEngine(st, DefaultOptions().WithPrefix(sqlPrefix))
	require.NoError(t, err)

	_, err = newGroupedRowReader(nil, false, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	tx, err := engine.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	_, _, err = engine.Exec(context.Background(), tx, "CREATE TABLE table1(id INTEGER, number INTEGER, PRIMARY KEY id)", nil)
	require.NoError(t, err)

	tx, err = engine.NewTx(context.Background(), DefaultTxOptions())
	require.NoError(t, err)

	defer tx.Cancel()

	table := tx.catalog.tables[0]

	r, err := newRawRowReader(tx, nil, table, period{}, "", &ScanSpecs{Index: table.primaryIndex})
	require.NoError(t, err)

	gr, err := newGroupedRowReader(r, false, []*AggColSelector{{aggFn: "COUNT", col: "id"}}, []*ColSelector{{col: "id"}})
	require.NoError(t, err)

	orderBy := gr.OrderBy()
	require.NotNil(t, orderBy)
	require.Len(t, orderBy, 1)
	require.Equal(t, "id", orderBy[0].Column)
	require.Equal(t, "table1", orderBy[0].Table)

	cols, err := gr.Columns(context.Background())
	require.NoError(t, err)
	require.Len(t, cols, 2)

	scanSpecs := gr.ScanSpecs()
	require.NotNil(t, scanSpecs)
	require.NotNil(t, scanSpecs.Index)
	require.True(t, scanSpecs.Index.IsPrimary())
}
