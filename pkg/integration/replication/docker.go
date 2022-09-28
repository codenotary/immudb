package replication

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

// dockerTestServerProvider creates docker test servers
type dockerTestServerProvider struct {
}

func (p *dockerTestServerProvider) AddServer(t *testing.T) TestServer {
	pool, err := dockertest.NewPool("")
	require.Nil(t, err)

	ret := &dockerTestServer{
		pool: pool,
	}

	ret.Start(t)
	return ret
}

// dockerTestServer represents an immudb docker test server
type dockerTestServer struct {
	pool *dockertest.Pool
	srv  *dockertest.Resource
	port int
}

func (s *dockerTestServer) Address(t *testing.T) (string, int) {
	return "localhost", s.port
}

func (s *dockerTestServer) Shutdown(t *testing.T) {
	require.NotNil(t, s.srv)
	require.NoError(t, s.pool.Purge(s.srv))
	s.srv = nil
}

// startContainer will run a container with the given options.
func (s *dockerTestServer) startContainer(t *testing.T, runOptions *dockertest.RunOptions) *dockertest.Resource {
	// Make sure that there are no containers running from previous execution first
	require.NoError(t, s.pool.RemoveContainerByName(runOptions.Name))

	image := fmt.Sprintf("%s:%s", runOptions.Repository, runOptions.Tag)
	fmt.Printf("Starting %s from %s", runOptions.Name, image)

	if runOptions.Tag == "latest" {
		_, err := s.pool.Client.InspectImage(image)
		require.NoError(t, err, "Could not find %s", image)
	}

	resource, err := s.pool.RunWithOptions(runOptions)
	require.NoError(t, err)
	return resource
}

func (s *dockerTestServer) Start(t *testing.T) {
	require.Nil(t, s.srv)

	const (
		name = "immudb"
		repo = "codenotary/immudb"
		tag  = "latest"
	)

	container := s.startContainer(t, &dockertest.RunOptions{
		Name:       name,
		Repository: repo,
		Tag:        tag,
		Cmd: []string{
			"--pgsql-server=false",
			"--metrics-server=false",
		},
		ExposedPorts: []string{"3322"},
		PortBindings: map[docker.Port][]docker.PortBinding{"3322/tcp": {}},
	})

	// Wait for the server to initialize
	// TODO: Active notification that the server has started
	time.Sleep(time.Second)

	port, err := strconv.Atoi(container.GetPort("3322/tcp"))
	require.Nil(t, err)

	s.srv = container
	s.port = port
}
