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

package server

import (
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
)

func TestOptions(t *testing.T) {
	op := DefaultOptions()
	if op.GetAuth() != true ||
		op.GetInMemoryStore() != false ||
		op.GetMaintenance() != false ||
		op.GetDefaultDbName() != DefaultdbName ||
		op.GetSystemAdminDbName() != SystemdbName ||
		op.CorruptionCheck != true ||
		op.Detached != false ||
		op.DevMode != true ||
		op.MTLs != false ||
		op.MetricsServer != true ||
		op.NoHistograms != false ||
		op.AdminPassword != auth.SysAdminPassword ||
		op.Address != "127.0.0.1" ||
		op.Network != "tcp" ||
		op.Port != 3322 ||
		op.MetricsPort != 9497 ||
		op.Config != "configs/immudb.toml" ||
		op.Pidfile != "" ||
		op.Logfile != "" {
		t.Errorf("database default options mismatch")
	}
}
func TestSetOptions(t *testing.T) {
	op := DefaultOptions().WithDir("immudb_dir").WithNetwork("udp").
		WithAddress("localhost").WithPort(2048).WithConfig("immudb.toml").
		WithPidfile("immu.pid").WithMTLs(true).WithAuth(false)

	if op.GetAuth() != false ||
		op.Dir != "immudb_dir" ||
		op.Network != "udp" ||
		op.Address != "localhost" ||
		op.Port != 2048 ||
		op.Config != "immudb.toml" ||
		op.Pidfile != "immu.pid" ||
		op.MTLs != true ||
		op.GetAuth() != false {
		t.Errorf("database default options mismatch")
	}
}
