package integration

import (
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestServerRecovertMode(t *testing.T) {
	serverOptions := server.DefaultOptions().
		WithMetricsServer(false).
		WithMaintenance(true).
		WithAuth(true)
	s := server.DefaultServer().WithOptions(serverOptions).(*server.ImmuServer)
	defer os.RemoveAll(s.Options.Dir)

	err := s.Initialize()
	require.Equal(t, server.ErrAuthMustBeDisabled, err)

	serverOptions = server.DefaultOptions().
		WithMetricsServer(true).
		WithPgsqlServer(true).
		WithMaintenance(true).
		WithAuth(false)
	s = server.DefaultServer().WithOptions(serverOptions).(*server.ImmuServer)

	err = s.Initialize()
	require.NoError(t, err)

	go func() {
		s.Start()
	}()

	time.Sleep(1 * time.Second)

	err = s.Stop()
	require.NoError(t, err)
}
