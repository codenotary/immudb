/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package integration

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestServerRecovertMode(t *testing.T) {
	dir, err := ioutil.TempDir("", "integration_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	serverOptions := server.DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithPgsqlServer(false).
		WithMaintenance(true).
		WithAuth(true).
		WithPort(0)

	s := server.DefaultServer().WithOptions(serverOptions).(*server.ImmuServer)

	err = s.Initialize()
	require.ErrorIs(t, err, server.ErrAuthMustBeDisabled)

	serverOptions = server.DefaultOptions().
		WithDir(dir).
		WithMetricsServer(false).
		WithPgsqlServer(false).
		WithMaintenance(true).
		WithAuth(false).
		WithPort(0)

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
