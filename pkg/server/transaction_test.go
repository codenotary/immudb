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

package server

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestImmuServer_Transaction(t *testing.T) {
	dir := t.TempDir()

	s := DefaultServer()

	s.WithOptions(DefaultOptions().WithDir(dir).WithMaintenance(true))

	_, err := s.NewTx(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.NewTx(context.Background(), &schema.NewTxRequest{Mode: schema.TxMode_ReadWrite})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.Commit(context.Background(), &emptypb.Empty{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.Rollback(context.Background(), &emptypb.Empty{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.TxSQLExec(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.TxSQLExec(context.Background(), &schema.SQLExecRequest{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)

	_, err = s.TxSQLQuery(context.Background(), nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	_, err = s.TxSQLQuery(context.Background(), &schema.SQLQueryRequest{})
	require.ErrorIs(t, err, ErrNotAllowedInMaintenanceMode)
}
