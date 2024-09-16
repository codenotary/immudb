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

package transactions

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

func TestNewTx(t *testing.T) {
	path := t.TempDir()

	db, err := database.NewDB("db1", nil, database.DefaultOptions().WithDBRootPath(path), logger.NewSimpleLogger("logger", os.Stdout))
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
