/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package immudb

import (
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/sessions"
	"github.com/spf13/viper"
)

func parseOptions() (options *server.Options, err error) {
	dir := viper.GetString("dir")

	address := viper.GetString("address")
	port := viper.GetInt("port")

	replicationOptions := &server.ReplicationOptions{}

	replicationOptions.
		WithIsReplica(viper.GetBool("replication-is-replica")).
		WithSyncReplication(viper.GetBool("replication-sync-enabled"))

	if replicationOptions.IsReplica {
		replicationOptions.
			WithPrimaryHost(viper.GetString("replication-primary-host")).
			WithPrimaryPort(viper.GetInt("replication-primary-port")).
			WithPrimaryUsername(viper.GetString("replication-primary-username")).
			WithPrimaryPassword(viper.GetString("replication-primary-password")).
			WithPrefetchTxBufferSize(viper.GetInt("replication-prefetch-tx-buffer-size")).
			WithReplicationCommitConcurrency(viper.GetInt("replication-commit-concurrency")).
			WithAllowTxDiscarding(viper.GetBool("replication-allow-tx-discarding")).
			WithSkipIntegrityCheck(viper.GetBool("replication-skip-integrity-check")).
			WithWaitForIndexing(viper.GetBool("replication-wait-for-indexing"))
	} else {
		replicationOptions.
			WithSyncAcks(viper.GetInt("replication-sync-acks"))
	}

	pidfile := viper.GetString("pidfile")

	logdir := viper.GetString("logdir")
	logfile := viper.GetString("logfile")
	logRotationSize := viper.GetInt("log-rotation-size")
	logRotationAge := viper.GetDuration("log-rotation-age")
	logAccess := viper.GetBool("log-access")
	logFormat := viper.GetString("logformat")

	mtls := viper.GetBool("mtls")
	auth := viper.GetBool("auth")
	maxRecvMsgSize := viper.GetInt("max-recv-msg-size")
	noHistograms := viper.GetBool("no-histograms")
	detached := viper.GetBool("detached")
	autoCert := viper.GetBool("auto-cert")
	certificate := viper.GetString("certificate")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")

	devMode := viper.GetBool("devmode")
	adminPassword := viper.GetString("admin-password")
	forceAdminPassword := viper.GetBool("force-admin-password")
	maintenance := viper.GetBool("maintenance")
	signingKey := viper.GetString("signingKey")
	synced := viper.GetBool("synced")
	tokenExpTime := viper.GetInt("token-expiry-time")

	webServer := viper.GetBool("web-server")
	webServerPort := viper.GetInt("web-server-port")

	metricsServer := viper.GetBool("metrics-server")
	metricsServerPort := viper.GetInt("metrics-server-port")

	pgsqlServer := viper.GetBool("pgsql-server")
	pgsqlServerPort := viper.GetInt("pgsql-server-port")

	pprof := viper.GetBool("pprof")

	grpcReflectionServerEnabled := viper.GetBool("grpc-reflection")
	swaggerUIEnabled := viper.GetBool("swaggerui")
	logRequestMetadata := viper.GetBool("log-request-metadata")

	s3Storage := viper.GetBool("s3-storage")
	s3RoleEnabled := viper.GetBool("s3-role-enabled")
	s3Role := viper.GetString("s3-role")
	s3Endpoint := viper.GetString("s3-endpoint")
	s3AccessKeyID := viper.GetString("s3-access-key-id")
	s3SecretKey := viper.GetString("s3-secret-key")
	s3BucketName := viper.GetString("s3-bucket-name")
	s3Location := viper.GetString("s3-location")
	s3PathPrefix := viper.GetString("s3-path-prefix")
	s3ExternalIdentifier := viper.GetBool("s3-external-identifier")
	s3MetadataURL := viper.GetString("s3-instance-metadata-url")

	remoteStorageOptions := server.DefaultRemoteStorageOptions().
		WithS3Storage(s3Storage).
		WithS3RoleEnabled(s3RoleEnabled).
		WithS3Role(s3Role).
		WithS3Endpoint(s3Endpoint).
		WithS3AccessKeyID(s3AccessKeyID).
		WithS3SecretKey(s3SecretKey).
		WithS3BucketName(s3BucketName).
		WithS3Location(s3Location).
		WithS3PathPrefix(s3PathPrefix).
		WithS3ExternalIdentifier(s3ExternalIdentifier).
		WithS3InstanceMetadataURL(s3MetadataURL)

	sessionOptions := sessions.DefaultOptions().
		WithMaxSessions(viper.GetInt("max-sessions")).
		WithSessionGuardCheckInterval(viper.GetDuration("sessions-guard-check-interval")).
		WithMaxSessionInactivityTime(viper.GetDuration("max-session-inactivity-time")).
		WithMaxSessionAgeTime(viper.GetDuration("max-session-age-time")).
		WithTimeout(viper.GetDuration("session-timeout"))

	tlsConfig, err := setUpTLS(pkey, certificate, clientcas, mtls, autoCert)
	if err != nil {
		return options, err
	}

	options = server.
		DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithReplicationOptions(replicationOptions).
		WithPidfile(pidfile).
		WithLogDir(logdir).
		WithLogfile(logfile).
		WithLogRotationSize(logRotationSize).
		WithLogRotationAge(logRotationAge).
		WithLogAccess(logAccess).
		WithTLS(tlsConfig).
		WithAuth(auth).
		WithMaxRecvMsgSize(maxRecvMsgSize).
		WithNoHistograms(noHistograms).
		WithDetached(detached).
		WithDevMode(devMode).
		WithAdminPassword(adminPassword).
		WithForceAdminPassword(forceAdminPassword).
		WithMaintenance(maintenance).
		WithSigningKey(signingKey).
		WithSynced(synced).
		WithRemoteStorageOptions(remoteStorageOptions).
		WithTokenExpiryTime(tokenExpTime).
		WithMetricsServer(metricsServer).
		WithMetricsServerPort(metricsServerPort).
		WithWebServer(webServer).
		WithWebServerPort(webServerPort).
		WithPgsqlServer(pgsqlServer).
		WithPgsqlServerPort(pgsqlServerPort).
		WithSessionOptions(sessionOptions).
		WithPProf(pprof).
		WithLogFormat(logFormat).
		WithSwaggerUIEnabled(swaggerUIEnabled).
		WithGRPCReflectionServerEnabled(grpcReflectionServerEnabled).
		WithLogRequestMetadata(logRequestMetadata)

	return options, nil
}
