/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package stdlib

import (
	"fmt"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"testing"
	"time"
)

func TestDriverConnector_Connect(t *testing.T) {
	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0)

	server := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	server.Initialize()

	defer server.Stop()
	defer os.RemoveAll(options.Dir)
	defer os.Remove(".state-")

	go func() {
		server.Start()
	}()

	time.Sleep(500 * time.Millisecond)

	immuDriver = &Driver{
		configs: make(map[string]*Conn),
	}

	port := server.Listener.Addr().(*net.TCPAddr).Port

	conn, err := immuDriver.Open(fmt.Sprintf("immudb://immudb:immudb@127.0.0.1:%d/defaultdb?sslmode=disable", port))
	require.NoError(t, err)
	require.NotNil(t, conn)
}

func TestDriverConnector_ConnectParseError(t *testing.T) {
	conn, err := immuDriver.Open("not parsable string")
	require.Error(t, err)
	require.Nil(t, conn)
}

func TestDriverConnector_Driver(t *testing.T) {
	c := driverConnector{
		driver: immuDriver,
	}
	d := c.Driver()
	require.IsType(t, &Driver{}, d)
}
