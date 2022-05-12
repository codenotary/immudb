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

package client

import (
	"testing"
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
		WithStreamChunkSize(4096)

	if op.LogFileName != "logfilename" ||
		op.PidPath != "pidpath" ||
		!op.Metrics ||
		op.Dir != "clientdir" ||
		op.Address != "127.0.0.1" ||
		op.Port != 4321 ||
		op.HealthCheckRetries != 3 ||
		!op.MTLs ||
		op.MTLsOptions.Servername != "localhost" ||
		op.MTLsOptions.Certificate != "no-certificate" ||
		op.MTLsOptions.ClientCAs != "no-client-ca" ||
		op.MTLsOptions.Pkey != "no-pkey" ||
		!op.Auth ||
		op.MaxRecvMsgSize != 1<<20 ||
		op.Config != "configfile" ||
		op.TokenFileName != "tokenfile" ||
		op.Username != "some-username" ||
		op.Password != "some-password" ||
		op.Database != "some-db" ||
		op.StreamChunkSize != 4096 ||
		op.Bind() != "127.0.0.1:4321" ||
		len(op.String()) == 0 {
		t.Fatal("Client options fail")
	}
}
