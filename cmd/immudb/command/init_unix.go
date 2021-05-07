// +build linux darwin freebsd

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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func parseOptions() (options *server.Options, err error) {
	dir := viper.GetString("dir")
	port := viper.GetInt("port")
	address := viper.GetString("address")
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
		WithTokenExpiryTime(tokenExpTime)

	return options, nil
}

func (cl *Commandline) setupFlags(cmd *cobra.Command, options *server.Options) {
	cmd.Flags().String("dir", options.Dir, "data folder")
	cmd.Flags().IntP("port", "p", options.Port, "port number")
	cmd.Flags().StringP("address", "a", options.Address, "bind address")
	cmd.PersistentFlags().StringVar(&cl.config.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immudb.toml)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immudb.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immudb/immudb.log")
	cmd.Flags().BoolP("mtls", "m", false, "enable mutual tls")
	cmd.Flags().BoolP("auth", "s", false, "enable auth")
	cmd.Flags().Int("max-recv-msg-size", options.MaxRecvMsgSize, "max message size in bytes the server can receive")
	cmd.Flags().Bool("no-histograms", false, "disable collection of histogram metrics like query durations")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().String("certificate", "", "server certificate file path")
	cmd.Flags().String("pkey", "", "server private key path")
	cmd.Flags().String("clientcas", "", "clients certificates list. Aka certificate authority")
	cmd.Flags().Bool("devmode", options.DevMode, "enable dev mode: accept remote connections without auth")
	cmd.Flags().String("admin-password", options.AdminPassword, "admin password (default is 'immudb') as plain-text or base64 encoded (must be prefixed with 'enc:' if it is encoded)")
	cmd.Flags().Bool("maintenance", options.GetMaintenance(), "override the authentication flag")
	cmd.Flags().String("signingKey", options.SigningKey, "signature private key path. If a valid one is provided, it enables the cryptographic signature of the root. E.g. \"./../test/signer/ec3.key\"")
	cmd.Flags().Bool("synced", false, "synced mode prevents data lost under unexpected crashes but affects performance")
	cmd.Flags().Int("token-expiry-time", options.TokenExpiryTimeMin, "client authentication token expiration time. Minutes")
}

func setupDefaults(options *server.Options) {
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("pidfile", options.Pidfile)
	viper.SetDefault("logfile", options.Logfile)
	viper.SetDefault("mtls", false)
	viper.SetDefault("auth", options.GetAuth())
	viper.SetDefault("max-recv-msg-size", options.MaxRecvMsgSize)
	viper.SetDefault("no-histograms", options.NoHistograms)
	viper.SetDefault("detached", options.Detached)
	viper.SetDefault("certificate", "")
	viper.SetDefault("pkey", "")
	viper.SetDefault("clientcas", "")
	viper.SetDefault("devmode", options.DevMode)
	viper.SetDefault("admin-password", options.AdminPassword)
	viper.SetDefault("maintenance", options.GetMaintenance())
	viper.SetDefault("synced", false)
	viper.SetDefault("token-expiry-time", options.TokenExpiryTimeMin)
}
