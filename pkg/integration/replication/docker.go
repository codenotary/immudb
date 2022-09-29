package replication

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// dockerTestServerProvider creates docker test servers
type dockerTestServerProvider struct {
}

func (p *dockerTestServerProvider) AddServer(t *testing.T) TestServer {
	pool, err := dockertest.NewPool("")
	require.Nil(t, err)

	ret := &dockerTestServer{
		pool: pool,
		dir:  t.TempDir(),
	}

	ret.Start(t)
	return ret
}

// dockerTestServer represents an immudb docker test server
type dockerTestServer struct {
	pool *dockertest.Pool
	srv  *dockertest.Resource
	port int
	dir  string
}

func (s *dockerTestServer) Address(t *testing.T) (string, int) {
	return getLocalIP(), s.port
}

func (s *dockerTestServer) Shutdown(t *testing.T) {
	require.NotNil(t, s.srv)
	require.NoError(t, s.pool.Purge(s.srv))

	// Wait for docker container to shutdown
	time.Sleep(2 * time.Second)
	s.srv = nil
}

// startContainer will run a container with the given options.
func (s *dockerTestServer) startContainer(t *testing.T, runOptions *dockertest.RunOptions) *dockertest.Resource {
	// Make sure that there are no containers running from previous execution first
	// This is to ensure there is no conflict in the container name.
	// FIX: containers fail to purge successfully when created without a name
	require.NoError(t, s.pool.RemoveContainerByName(runOptions.Name))

	image := fmt.Sprintf("%s:%s", runOptions.Repository, runOptions.Tag)

	if runOptions.Tag == "latest" {
		_, err := s.pool.Client.InspectImage(image)
		require.NoError(t, err, "Could not find %s", image)
	}

	resource, err := s.pool.RunWithOptions(runOptions, func(config *docker.HostConfig) {
		config.Mounts = []docker.HostMount{
			{
				Source: "/tmp",
				Target: os.Getenv("TMPDIR"),
				Type:   "bind",
			},
		}
	})
	require.NoError(t, err)
	return resource
}

func (s *dockerTestServer) Start(t *testing.T) {
	require.Nil(t, s.srv)

	var (
		name     = fmt.Sprintf("immudb-%d", rand.Intn(50))
		repo     = "immudb/e2e"
		tag      = "latest"
		dirFlag  = fmt.Sprintf("--dir=%s", s.dir)
		hostPort = ""
	)

	if s.port > 0 {
		hostPort = strconv.Itoa(s.port)
	}

	container := s.startContainer(t, &dockertest.RunOptions{
		Name:       name,
		Repository: repo,
		Tag:        tag,
		Cmd: []string{
			dirFlag,
			"--pgsql-server=false",
			"--metrics-server=false",
		},
		ExposedPorts: []string{"3322"},
		PortBindings: map[docker.Port][]docker.PortBinding{"3322/tcp": {{HostPort: hostPort}}},
	})

	port, err := strconv.Atoi(container.GetPort("3322/tcp"))
	require.Nil(t, err)

	s.srv = container
	s.port = port

	// Wait for the server to initialize
	// TODO: Active notification that the server has started
	time.Sleep(5 * time.Second)

}

// getLocalIP returns the non loopback local IP of the host
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
