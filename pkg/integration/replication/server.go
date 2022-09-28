package replication

import (
	"net"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

// inProcessTestServerProvider creates in-memory test servers
// those servers are using a temporary directory that's cleaned up after the test is finished
type inProcessTestServerProvider struct {
}

func (p *inProcessTestServerProvider) AddServer(t *testing.T) TestServer {
	ret := &inProcessTestServer{
		dir: t.TempDir(), // go test will clean this up
	}

	ret.Start(t)
	return ret
}

// inProcessTestServer represents an in-process test server
type inProcessTestServer struct {
	srv  *server.ImmuServer
	dir  string
	port int
}

func (s *inProcessTestServer) Address(t *testing.T) (string, int) {
	return "localhost", s.port
}

func (s *inProcessTestServer) Shutdown(t *testing.T) {
	require.NotNil(t, s.srv)
	s.srv.Stop()
	s.srv = nil
}

func (s *inProcessTestServer) Start(t *testing.T) {
	require.Nil(t, s.srv)

	opts := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(s.port).
		WithDir(s.dir)

	srv := server.DefaultServer().WithOptions(opts).(*server.ImmuServer)
	err := srv.Initialize()
	require.NoError(t, err)

	go func() {
		err := srv.Start()
		require.NoError(t, err)
	}()

	// Wait for the server to initialize
	// TODO: Active notification that the server has started
	time.Sleep(time.Second)

	if s.port == 0 {
		// Save the port for reopening with the same value
		s.port = srv.Listener.Addr().(*net.TCPAddr).Port
	}

	s.srv = srv
}
