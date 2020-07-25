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

package client

import (
	"testing"
)

func TestOptions(t *testing.T) {
	op := DefaultOptions().WithLogFileName("logfilename").
		WithPrometheusHost("localhost").
		WithPrometheusPort("1234").
		WithPidPath("pidpath").
		WithMetrics(true).
		WithDir("clientdir").
		WithAddress("127.0.0.1").
		WithPort(4321).
		WithHealthCheckRetries(3).
		WithMTLs(true).
		WithAuth(true).
		WithConfig("configfile").
		WithTokenFileName("tokenfile")
	if op.LogFileName != "logfilename" ||
		op.PrometheusHost != "localhost" ||
		op.PrometheusPort != "1234" ||
		op.PidPath != "pidpath" ||
		!op.Metrics ||
		op.Dir != "clientdir" ||
		op.Address != "127.0.0.1" ||
		op.Port != 4321 ||
		op.HealthCheckRetries != 3 ||
		!op.MTLs ||
		!op.Auth ||
		op.Config != "configfile" ||
		op.TokenFileName != "tokenfile" ||
		op.Bind() != "127.0.0.1:4321" ||
		len(op.String()) == 0 {
		t.Fatal("Client options fail")
	}
}
