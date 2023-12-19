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

package replication

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/rs/xid"
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

func (s *inProcessTestServer) UUID(t *testing.T) xid.ID {
	return s.srv.UUID
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

	if s.port == 0 {
		// Save the port for reopening with the same value
		s.port = srv.Listener.Addr().(*net.TCPAddr).Port
	}

	go func() {
		err := srv.Start()
		require.NoError(t, err)
	}()

	require.Eventually(t, func() bool {
		// Check if we can talk to GRPC server (checking if we can only connect alone is not enough)
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", s.port), 5*time.Millisecond)
		if err != nil {
			return false
		}
		defer conn.Close()

		err = conn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
		if err != nil {
			return false
		}

		_, err = conn.Read([]byte{0})
		return err == nil
	}, time.Second, 10*time.Millisecond)

	s.srv = srv
}
