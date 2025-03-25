/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	mtlsOpts := DefaultMTLsOptions().
		WithServername("localhost").
		WithCertificate("no-certificate").
		WithClientCAs("no-client-ca").
		WithPkey("no-pkey")

	op := DefaultOptions().WithLogFileName("logfilename").
		WithPidPath("pidpath").
		WithMetrics(true).
		WithDir("clientdir").
		WithAddress("127.0.0.1").
		WithPort(4321).
		WithHealthCheckRetries(3).
		WithMTLs(true).
		WithMTLsOptions(mtlsOpts).
		WithAuth(true).
		WithMaxRecvMsgSize(1 << 20).
		WithConfig("configfile").
		WithTokenFileName("tokenfile").
		WithUsername("some-username").
		WithPassword("some-password").
		WithDatabase("some-db").
		WithStreamChunkSize(4096).
		WithDisableIdentityCheck(true)

	require.Equal(t, op.LogFileName, "logfilename")
	require.Equal(t, op.PidPath, "pidpath")
	require.True(t, op.Metrics)
	require.Equal(t, op.Dir, "clientdir")
	require.Equal(t, op.Address, "127.0.0.1")
	require.Equal(t, op.Port, 4321)
	require.Equal(t, op.HealthCheckRetries, 3)
	require.True(t, op.MTLs)
	require.Equal(t, op.MTLsOptions.Servername, "localhost")
	require.Equal(t, op.MTLsOptions.Certificate, "no-certificate")
	require.Equal(t, op.MTLsOptions.ClientCAs, "no-client-ca")
	require.Equal(t, op.MTLsOptions.Pkey, "no-pkey")
	require.True(t, op.Auth)
	require.Equal(t, op.MaxRecvMsgSize, 1<<20)
	require.Equal(t, op.Config, "configfile")
	require.Equal(t, op.TokenFileName, "tokenfile")
	require.Equal(t, op.Username, "some-username")
	require.Equal(t, op.Password, "some-password")
	require.Equal(t, op.Database, "some-db")
	require.Equal(t, op.StreamChunkSize, 4096)
	require.True(t, op.DisableIdentityCheck)
	require.Equal(t, op.Bind(), "127.0.0.1:4321")
	require.NotEmpty(t, op.String())

}
