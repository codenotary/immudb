package immuc_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerInfo(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.ServerInfo(nil)
	require.NoError(t, err, "ServerInfo fail")
	require.Contains(t, msg, "version")
}
