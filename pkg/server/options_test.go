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

package server

import (
	"crypto/tls"
	"testing"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/stream"

	"github.com/stretchr/testify/require"
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
		op.ForceAdminPassword != false ||
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
		op.PProf != false ||
		op.IsFileLogger() != false ||
		op.IsJSONLogger() != false {
		t.Errorf("database default options mismatch")
	}
}

func TestReplicationOptions(t *testing.T) {
	repOpts := &ReplicationOptions{}
	repOpts.
		WithIsReplica(true).
		WithSyncReplication(false).
		WithSyncAcks(0).
		WithPrimaryHost("localhost").
		WithPrimaryPort(3322).
		WithPrimaryUsername("primary-user").
		WithPrimaryPassword("primary-pwd").
		WithPrefetchTxBufferSize(100).
		WithReplicationCommitConcurrency(5).
		WithAllowTxDiscarding(true).
		WithSkipIntegrityCheck(true).
		WithWaitForIndexing(true)

	require.True(t, repOpts.IsReplica)
	require.False(t, repOpts.SyncReplication)
	require.Zero(t, repOpts.SyncAcks)
	require.Equal(t, "localhost", repOpts.PrimaryHost)
	require.Equal(t, 3322, repOpts.PrimaryPort)
	require.Equal(t, "primary-user", repOpts.PrimaryUsername)
	require.Equal(t, "primary-pwd", repOpts.PrimaryPassword)
	require.Equal(t, 100, repOpts.PrefetchTxBufferSize)
	require.Equal(t, 5, repOpts.ReplicationCommitConcurrency)
	require.True(t, repOpts.AllowTxDiscarding)
	require.True(t, repOpts.SkipIntegrityCheck)
	require.True(t, repOpts.WaitForIndexing)

	// primary-related settings
	repOpts.
		WithIsReplica(false).
		WithSyncReplication(true).
		WithSyncAcks(1)

	require.False(t, repOpts.IsReplica)
	require.True(t, repOpts.SyncReplication)
	require.Equal(t, 1, repOpts.SyncAcks)
}

func TestSetOptions(t *testing.T) {
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{}}

	op := DefaultOptions().WithDir("immudb_dir").WithNetwork("udp").
		WithAddress("localhost").
		WithPort(2048).
		WithPidfile("immu.pid").
		WithAuth(false).
		WithMaxRecvMsgSize(4096).
		WithDetached(true).
		WithNoHistograms(true).
		WithMetricsServer(false).
		WithDevMode(true).WithLogfile("logfile").
		WithAdminPassword("admin").
		WithForceAdminPassword(true).
		WithStreamChunkSize(4096).
		WithWebServerPort(8081).
		WithTokenExpiryTime(52).
		WithWebServer(false).
		WithTLS(tlsConfig).
		WithPgsqlServer(true).
		WithPgsqlServerPort(123456).
		WithPProf(true).
		WithLogFormat(logger.LogFormatJSON)

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
		op.ForceAdminPassword != true ||
		op.StreamChunkSize != 4096 ||
		op.WebServerPort != 8081 ||
		op.Bind() != "localhost:2048" ||
		op.WebBind() != "localhost:8081" ||
		op.WebServer != false ||
		op.TLSConfig != tlsConfig ||
		op.TokenExpiryTimeMin != 52 ||
		!op.PgsqlServer ||
		op.PgsqlServerPort != 123456 ||
		op.PProf != true ||
		op.IsJSONLogger() != true {
		t.Errorf("database default options mismatch")
	}
}

func TestOptionsString(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Sync replication : false
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

	require.Equal(t, expected, op.String())
}

func TestOptionsWithSyncReplicationString(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Sync replication : true
Sync acks        : 1
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

	op.ReplicationOptions.
		WithSyncReplication(true).
		WithSyncAcks(1)

	require.Equal(t, expected, op.String())
}

func TestOptionsStringWithS3(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Sync replication : false
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
   external id   : false
   metadata url  : http://169.254.169.254
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
				WithS3PathPrefix("s3-path-prefix").
				WithS3InstanceMetadataURL("http://169.254.169.254"),
		)

	require.Equal(t, expected, op.String())
}

func TestOptionsStringWithS3RoleBased(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Sync replication : false
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
   role auth     : true
   role name     : s3-role
   endpoint      : s3-endpoint
   bucket name   : s3-bucket-name
   location      : s3-location
   prefix        : s3-path-prefix
   external id   : false
   metadata url  : http://169.254.169.254
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
				WithS3RoleEnabled(true).
				WithS3Role("s3-role").
				WithS3Endpoint("s3-endpoint").
				WithS3BucketName("s3-bucket-name").
				WithS3Location("s3-location").
				WithS3PathPrefix("s3-path-prefix").
				WithS3InstanceMetadataURL("http://169.254.169.254"),
		)

	require.Equal(t, expected, op.String())
}

func TestOptionsStringWithS3ExternalIdentifier(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
Sync replication : false
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
   external id   : true
   metadata url  : 
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
				WithS3PathPrefix("s3-path-prefix").
				WithS3ExternalIdentifier(true),
		)

	require.Equal(t, expected, op.String())
}

func TestOptionsStringWithPProf(t *testing.T) {
	expected := `================ Config ================
Data dir         : ./data
Address          : 0.0.0.0:3322
Metrics address  : 0.0.0.0:9497/metrics
pprof enabled    : true
Sync replication : false
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
		WithLogfile("immu.log").
		WithPProf(true)

	require.Equal(t, expected, op.String())
}
