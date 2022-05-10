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

package transactions

import (
	"context"
	"os"
	"testing"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestNewTx(t *testing.T) {
	db, err := database.NewDB("db1", nil, database.DefaultOption(), logger.NewSimpleLogger("logger", os.Stdout))
	require.NoError(t, err)

	defer os.RemoveAll("data")

	tx, err := NewTransaction(context.Background(), schema.TxMode_ReadWrite, db, "session1")
	require.NoError(t, err)
	require.NotNil(t, tx)

	err = tx.Rollback()
	require.NoError(t, err)

	_, err = tx.SQLQuery(nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.SQLExec(nil)
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	err = tx.Rollback()
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)

	_, err = tx.Commit()
	require.ErrorIs(t, err, sql.ErrNoOngoingTx)
}
