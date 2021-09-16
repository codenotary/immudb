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

package immudb

import (
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/viper"
)

func parseOptions() (options *server.Options, err error) {
	dir := viper.GetString("dir")

	address := viper.GetString("address")
	port := viper.GetInt("port")

	replicationEnabled := viper.GetBool("replication-enabled")

	var replicationOptions *server.ReplicationOptions

	if replicationEnabled {
		replicationOptions = (&server.ReplicationOptions{}).
			WithMasterAddress(viper.GetString("replication-master-address")).
			WithMasterPort(viper.GetInt("replication-master-port")).
			WithFollowerUsername(viper.GetString("replication-follower-username")).
			WithFollowerPassword(viper.GetString("replication-follower-password"))
	}

	pidfile := viper.GetString("pidfile")
	logfile := viper.GetString("logfile")

	mtls := viper.GetBool("mtls")
	auth := viper.GetBool("auth")
	maxRecvMsgSize := viper.GetInt("max-recv-msg-size")
	noHistograms := viper.GetBool("no-histograms")
	detached := viper.GetBool("detached")
	certificate := viper.GetString("certificate")
	pkey := viper.GetString("pkey")
	clientcas := viper.GetString("clientcas")

	devMode := viper.GetBool("devmode")
	adminPassword := viper.GetString("admin-password")
	maintenance := viper.GetBool("maintenance")
	signingKey := viper.GetString("signingKey")
	synced := viper.GetBool("synced")
	tokenExpTime := viper.GetInt("token-expiry-time")

	webServer := viper.GetBool("web-server")
	webServerPort := viper.GetInt("web-server-port")

	pgsqlServer := viper.GetBool("pgsql-server")
	pgsqlServerPort := viper.GetInt("pgsql-server-port")

	s3Storage := viper.GetBool("s3-storage")
	s3Endpoint := viper.GetString("s3-endpoint")
	s3AccessKeyID := viper.GetString("s3-access-key-id")
	s3SecretKey := viper.GetString("s3-secret-key")
	s3BucketName := viper.GetString("s3-bucket-name")
	s3PathPrefix := viper.GetString("s3-path-prefix")

	remoteStorageOptions := server.DefaultRemoteStorageOptions().
		WithS3Storage(s3Storage).
		WithS3Endpoint(s3Endpoint).
		WithS3AccessKeyID(s3AccessKeyID).
		WithS3SecretKey(s3SecretKey).
		WithS3BucketName(s3BucketName).
		WithS3PathPrefix(s3PathPrefix)

	tlsConfig, err := setUpTLS(pkey, certificate, clientcas, mtls)
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
		WithLogfile(logfile).
		WithTLS(tlsConfig).
		WithAuth(auth).
		WithMaxRecvMsgSize(maxRecvMsgSize).
		WithNoHistograms(noHistograms).
		WithDetached(detached).
		WithDevMode(devMode).
		WithAdminPassword(adminPassword).
		WithMaintenance(maintenance).
		WithSigningKey(signingKey).
		WithSynced(synced).
		WithRemoteStorageOptions(remoteStorageOptions).
		WithTokenExpiryTime(tokenExpTime).
		WithWebServer(webServer).
		WithWebServerPort(webServerPort).
		WithPgsqlServer(pgsqlServer).
		WithPgsqlServerPort(pgsqlServerPort)

	return options, nil
}
