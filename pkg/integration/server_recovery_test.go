package integration

import (
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/stretchr/testify/require"
)

func TestServerRecovertMode(t *testing.T) {
	serverOptions := server.DefaultOptions().
		WithMetricsServer(true).
		WithMaintenance(true).
		WithAuth(true)
	bs := servertest.NewBufconnServer(serverOptions)
	err := bs.Start()
	require.Equal(t, server.ErrAuthMustBeDisabled, err)

	serverOptions = server.DefaultOptions().
		WithMetricsServer(true).
		WithMaintenance(true).
		WithAuth(false)
	bs = servertest.NewBufconnServer(serverOptions)
	err = bs.Start()
	require.NoError(t, err)
	defer bs.Stop()

	defer os.RemoveAll(serverOptions.Dir)
}
