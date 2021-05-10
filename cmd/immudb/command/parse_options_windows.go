// +build windows
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
	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/viper"
)

func parseOptions() (options *server.Options, err error) {
	dir, err := c.ResolvePath(viper.GetString("dir"), true)
	if err != nil {
		return options, err
	}
	port := viper.GetInt("port")
	address := viper.GetString("address")

	pidfile, err := c.ResolvePath(viper.GetString("pidfile"), true)
	if err != nil {
		return options, err
	}
	logfile, err := c.ResolvePath(viper.GetString("logfile"), true)
	if err != nil {
		return options, err
	}
	mtls := viper.GetBool("mtls")
	auth := viper.GetBool("auth")
	maxRecvMsgSize := viper.GetInt("max-recv-msg-size")
	noHistograms := viper.GetBool("no-histograms")
	detached := viper.GetBool("detached")
	certificate, err := c.ResolvePath(viper.GetString("certificate"), true)
	if err != nil {
		return options, err
	}
	pkey, err := c.ResolvePath(viper.GetString("pkey"), true)
	if err != nil {
		return options, err
	}
	clientcas, err := c.ResolvePath(viper.GetString("clientcas"), true)
	if err != nil {
		return options, err
	}

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

	storeOpts := server.DefaultStoreOptions().WithSynced(synced)

	tlsConfig, err := setUpTLS(pkey, certificate, clientcas, mtls)
	if err != nil {
		return options, err
	}

	options = server.
		DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
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
		WithStoreOptions(storeOpts).
		WithTokenExpiryTime(tokenExpTime).
		WithPgsqlServer(pgsqlServer).
		WithPgsqlServerPort(pgsqlServerPort).
		WithWebServer(webServer).
		WithWebServerPort(webServerPort)

	return options, nil
}
