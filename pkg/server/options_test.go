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

package server

import (
	"crypto/tls"
	"testing"

	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	op := DefaultOptions()
	if op.GetAuth() != true ||
		op.GetMaintenance() != false ||
		op.GetDefaultDBName() != DefaultDBName ||
		op.GetSystemAdminDBName() != SystemDBName ||
		op.Detached != false ||
		op.DevMode != false ||
		op.NoHistograms != false ||
		op.AdminPassword != auth.SysAdminPassword ||
		op.Address != "0.0.0.0" ||
		op.Network != "tcp" ||
		op.Port != 3322 ||
		op.MetricsServer != true ||
		op.MetricsServerPort != 9497 ||
		op.Config != "configs/immudb.toml" ||
		op.Pidfile != "" ||
		op.StreamChunkSize != stream.DefaultChunkSize ||
		op.Logfile != "" ||
		op.WebServer != true ||
		op.WebServerPort != 8080 ||
		op.WebBind() != "0.0.0.0:8080" ||
		op.MetricsBind() != "0.0.0.0:9497" ||
		op.PgsqlServer ||
		op.PgsqlServerPort != 5432 ||
		op.PProf != false {
		t.Errorf("database default options mismatch")
	}
}

func TestSetOptions(t *testing.T) {
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{}}
	op := DefaultOptions().WithDir("immudb_dir").WithNetwork("udp").
		WithAddress("localhost").WithPort(2048).
		WithPidfile("immu.pid").WithAuth(false).
		WithMaxRecvMsgSize(4096).
		WithDetached(true).WithNoHistograms(true).WithMetricsServer(false).
		WithDevMode(true).WithLogfile("logfile").WithAdminPassword("admin").
		WithStreamChunkSize(4096).
		WithWebServerPort(8081).
		WithTokenExpiryTime(52).
		WithWebServer(false).
		WithTLS(tlsConfig).
		WithPgsqlServer(true).
		WithPgsqlServerPort(123456).
		WithPProf(true)

	if op.GetAuth() != false ||
		op.Dir != "immudb_dir" ||
		op.Network != "udp" ||
		op.Address != "localhost" ||
		op.Port != 2048 ||
		op.Config != "configs/immudb.toml" ||
		op.Pidfile != "immu.pid" ||
		op.GetAuth() != false ||
		op.MaxRecvMsgSize != 4096 ||
		op.Detached != true ||
		op.NoHistograms != true ||
		op.MetricsServer != false ||
		op.DevMode != true ||
		op.Logfile != "logfile" ||
		op.AdminPassword != "admin" ||
		op.StreamChunkSize != 4096 ||
		op.WebServerPort != 8081 ||
		op.Bind() != "localhost:2048" ||
		op.WebBind() != "localhost:8081" ||
		op.WebServer != false ||
		op.TLSConfig != tlsConfig ||
		op.TokenExpiryTimeMin != 52 ||
		!op.PgsqlServer ||
		op.PgsqlServerPort != 123456 ||
		op.PProf != true {
		t.Errorf("database default options mismatch")
	}
}

func TestOptionsString(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Config file      : configs/immudb.toml
PID file         : immu.pid
Log file         : immu.log
Max recv msg size: 33554432
Auth enabled     : true
Dev mode         : false
Default database : defaultdb
Maintenance mode : false
Synced mode      : true
----------------------------------------
Superadmin default credentials
   Username      : immudb
   Password      : immudb
========================================`

	op := DefaultOptions().
		WithPidfile("immu.pid").
		WithLogfile("immu.log")

	assert.Equal(t, expected, op.String())
}

func TestOptionsStringWithS3(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Config file      : configs/immudb.toml
PID file         : immu.pid
Log file         : immu.log
Max recv msg size: 33554432
Auth enabled     : true
Dev mode         : false
Default database : defaultdb
Maintenance mode : false
Synced mode      : true
S3 storage
   endpoint      : s3-endpoint
   bucket name   : s3-bucket-name
   location      : s3-location
   prefix        : s3-path-prefix
----------------------------------------
Superadmin default credentials
   Username      : immudb
   Password      : immudb
========================================`

	op := DefaultOptions().
		WithPidfile("immu.pid").
		WithLogfile("immu.log").
		WithRemoteStorageOptions(
			DefaultRemoteStorageOptions().
				WithS3Storage(true).
				WithS3Endpoint("s3-endpoint").
				WithS3BucketName("s3-bucket-name").
				WithS3Location("s3-location").
				WithS3PathPrefix("s3-path-prefix"),
		)

	assert.Equal(t, expected, op.String())
}
