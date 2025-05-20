/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package transactions

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/v2/embedded/logger"
	"github.com/codenotary/immudb/v2/embedded/sql"
	"github.com/codenotary/immudb/v2/embedded/store"
	"github.com/codenotary/immudb/v2/pkg/database"
	"github.com/stretchr/testify/require"
)

func TestNewTx(t *testing.T) {
	path := t.TempDir()

	st, err := store.Open(path, store.DefaultOptions())
	require.NoError(t, err)
	defer st.Close()

	db, err := database.OpenDB("db1", st, nil, database.DefaultOptions().WithDBRootPath(path), logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	_, err = NewTransaction(context.Background(), nil, db, "session1")
	require.ErrorIs(t, err, sql.ErrIllegalArguments)

	tx, err := NewTransaction(context.Background(), sql.DefaultTxOptions(), db, "session1")
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Rollback()
	require.NoError(t, err)

	_, err = tx.SQLQuery(context.Background(), nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.SQLExec(context.Background(), nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.Rollback()
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	_, err = tx.Commit(context.Background())
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)
}
