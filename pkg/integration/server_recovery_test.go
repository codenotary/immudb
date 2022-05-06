/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/stretchr/testify/require"
)

func TestServerRecovertMode(t *testing.T) {
	serverOptions := server.DefaultOptions().
		WithMetricsServer(false).
		WithPgsqlServer(false).
		WithMaintenance(true).
		WithAuth(true).
		WithPort(0)
	s := server.DefaultServer().WithOptions(serverOptions).(*server.ImmuServer)
	defer os.RemoveAll(s.Options.Dir)

	err := s.Initialize()
	require.Equal(t, server.ErrAuthMustBeDisabled, err)

	serverOptions = server.DefaultOptions().
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
