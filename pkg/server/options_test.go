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

package server

import (
	"github.com/codenotary/immudb/pkg/stream"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
)

func TestOptions(t *testing.T) {
	op := DefaultOptions()
	if op.GetAuth() != true ||
		op.GetMaintenance() != false ||
		op.GetDefaultDbName() != DefaultdbName ||
		op.GetSystemAdminDbName() != SystemdbName ||
		op.CorruptionCheck != true ||
		op.Detached != false ||
		op.DevMode != false ||
		op.MTLs != false ||
		op.MetricsServer != true ||
		op.NoHistograms != false ||
		op.AdminPassword != auth.SysAdminPassword ||
		op.Address != "0.0.0.0" ||
		op.Network != "tcp" ||
		op.Port != 3322 ||
		op.MetricsPort != 9497 ||
		op.Config != "configs/immudb.toml" ||
		op.Pidfile != "" ||
		op.StreamChunkSize != stream.DefaultChunkSize ||
		op.Logfile != "" {
		t.Errorf("database default options mismatch")
	}
}

func TestSetOptions(t *testing.T) {
	op := DefaultOptions().WithDir("immudb_dir").WithNetwork("udp").
		WithAddress("localhost").WithPort(2048).
		WithPidfile("immu.pid").WithMTLs(true).WithAuth(false).
		WithMaxRecvMsgSize(4096).
		WithDetached(true).WithNoHistograms(true).WithMetricsServer(false).
		WithDevMode(true).WithLogfile("logfile").WithAdminPassword("admin").
		WithStreamChunkSize(4096)

	if op.GetAuth() != false ||
		op.Dir != "immudb_dir" ||
		op.Network != "udp" ||
		op.Address != "localhost" ||
		op.Port != 2048 ||
		op.Config != "configs/immudb.toml" ||
		op.Pidfile != "immu.pid" ||
		op.MTLs != true ||
		op.GetAuth() != false ||
		op.MaxRecvMsgSize != 4096 ||
		op.Detached != true ||
		op.NoHistograms != true ||
		op.MetricsServer != false ||
		op.DevMode != true ||
		op.Logfile != "logfile" ||
		op.AdminPassword != "admin" ||
		op.StreamChunkSize != 4096 ||
		op.Bind() != "localhost:2048" {
		t.Errorf("database default options mismatch")
	}
}
