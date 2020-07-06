/*
Copyright 2019-2020 vChain, Inc.

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
package gw

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := DefaultOptions()
	require.Equal(t, "0.0.0.0", opts.Address)
	require.Equal(t, 3323, opts.Port)
	require.Equal(t, 9476, opts.MetricsPort)
	require.Equal(t, "127.0.0.1", opts.ImmudbAddress)
	require.Equal(t, 3322, opts.ImmudbPort)
	require.False(t, opts.Audit)
	require.Equal(t, 5*time.Minute, opts.AuditInterval)
	require.Equal(t, "immugwauditor", opts.AuditUsername)
	require.Empty(t, opts.AuditPassword)
	require.False(t, opts.Detached)
	require.False(t, opts.MTLs)
	require.Equal(t, "configs/immugw.toml", opts.Config)
	require.Equal(t, ".", opts.Dir)
	require.Empty(t, opts.Pidfile)
	require.Empty(t, opts.Logfile)

	require.Equal(t, "111.1.1.1", opts.WithAddress("111.1.1.1").Address)
	require.Equal(t, 1111, opts.WithPort(1111).Port)
	require.Equal(
		t, "000.0.0.0", opts.WithImmudbAddress("000.0.0.0").ImmudbAddress)
	require.Equal(t, 0000, opts.WithImmudbPort(0000).ImmudbPort)
	require.True(t, opts.WithAudit(true).Audit)
	require.Equal(
		t, 10*time.Minute, opts.WithAuditInterval(10*time.Minute).AuditInterval)
	require.Equal(
		t, "someAuditor", opts.WithAuditUsername("someAuditor").AuditUsername)
	require.Equal(
		t, "somePassword", opts.WithAuditPassword("somePassword").AuditPassword)
	require.True(t, opts.WithDetached(true).Detached)
	require.True(t, opts.WithMTLs(true).MTLs)
	require.Equal(t, "someServer", opts.WithMTLsOptions(
		client.MTLsOptions{Servername: "someServer"}).MTLsOptions.Servername)
	require.Equal(
		t, "./someConfig.toml", opts.WithConfig("./someConfig.toml").Config)
	require.Equal(t, "./someDir", opts.WithDir("./someDir").Dir)
	require.Equal(t, "./somePidfile", opts.WithPidfile("./somePidfile").Pidfile)
	require.Equal(
		t, "./someLogfile.log", opts.WithLogfile("./someLogfile.log").Logfile)

	require.Equal(t, "0.0.0.0:3323", opts.Bind())
	require.Equal(t, "0.0.0.0:9476", opts.MetricsBind())

	optsJSONBytes, err := json.Marshal(opts)
	require.NoError(t, err)
	require.Equal(t, string(optsJSONBytes), opts.String())
}
