package server

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestImmuServer_Transaction(t *testing.T) {
	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	s := DefaultServer()

	s.WithOptions(DefaultOptions().WithDir(dir).WithMaintenance(true))

	_, err = s.NewTx(context.Background(), nil)
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
