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

package main

import (
	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var o = c.Options{}

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immudb") })
}

func main() {
	cmd := &cobra.Command{
		Use:   "immudb",
		Short: "The ImmuDB tamperproof database server",
		Long: `The ImmuDB tamperproof database server.

Environment variables:
  IMMUDB_DIR=.
  IMMUDB_NETWORK=tcp
  IMMUDB_ADDRESS=127.0.0.1
  IMMUDB_PORT=3322
  IMMUDB_DBNAME=immudb
  IMMUDB_PIDFILE=
  IMMUDB_LOGFILE=
  IMMUDB_MTLS=false
  IMMUDB_AUTH=false
  IMMUDB_DETACHED=false
  IMMUDB_PKEY=./tools/mtls/3_application/private/localhost.key.pem
  IMMUDB_CERTIFICATE=./tools/mtls/3_application/certs/localhost.cert.pem
  IMMUDB_CLIENTCAS=./tools/mtls/2_intermediate/certs/ca-chain.cert.pem`,
		DisableAutoGenTag: true,
		RunE:              Immudb,
	}

	setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())

	if err := bindFlags(cmd); err != nil {
		c.QuitToStdErr(err)
	}
	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	cmd.AddCommand(man.Generate(cmd, "immudb", "../docs/man/immudb"))

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func Immudb(cmd *cobra.Command, args []string) (err error) {
	var options server.Options
	if options, err = parseOptions(cmd); err != nil {
		return err
	}
	immuServer := server.
		DefaultServer().
		WithOptions(options)
	if options.Logfile != "" {
		if flogger, file, err := logger.NewFileLogger("immudb ", options.Logfile); err == nil {
			defer func() {
				if err := file.Close(); err != nil {
					c.QuitToStdErr(err)
				}
			}()
			immuServer.WithLogger(flogger)
		} else {
			c.QuitToStdErr(err)
		}
	}

	if options.Detached {
		c.Detached()
	}

	if err := immuServer.Start(); err != nil {
		c.QuitToStdErr(err)
	}
	return nil
}

func parseOptions(cmd *cobra.Command) (options server.Options, err error) {
	dir := viper.GetString("default.dir")
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	dbName := viper.GetString("default.dbname")
	// config file came only from arguments or default folder
	if o.CfgFn, err = cmd.Flags().GetString("config"); err != nil {
		return server.Options{}, err
	}
	pidfile := viper.GetString("default.pidfile")
	logfile := viper.GetString("default.logfile")
	mtls := viper.GetBool("default.mtls")
	auth := viper.GetBool("default.auth")
	detached := viper.GetBool("default.detached")
	certificate := viper.GetString("default.certificate")
	pkey := viper.GetString("default.pkey")
	clientcas := viper.GetString("default.clientcas")

	options = server.
		DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithDbName(dbName).
		WithConfig(o.CfgFn).
		WithPidfile(pidfile).
		WithLogfile(logfile).
		WithMTLs(mtls).
		WithAuth(auth).
		WithDetached(detached)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = server.DefaultMTLsOptions().
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(clientcas)
	}
	return options, nil
}

func setupFlags(cmd *cobra.Command, options server.Options, mtlsOptions server.MTLsOptions) {
	cmd.Flags().String("dir", options.Dir, "data folder")
	cmd.Flags().IntP("port", "p", options.Port, "port number")
	cmd.Flags().StringP("address", "a", options.Address, "bind address")
	cmd.Flags().StringP("dbname", "n", options.DbName, "db name")
	cmd.Flags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immudb.ini)")
	cmd.Flags().String("pidfile", options.Pidfile, "pid path with filename. E.g. /var/run/immudb.pid")
	cmd.Flags().String("logfile", options.Logfile, "log path with filename. E.g. /tmp/immudb/immudb.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().BoolP("auth", "s", options.MTLs, "enable auth")
	cmd.Flags().BoolP(c.DetachedFlag, c.DetachedShortFlag, options.Detached, "run immudb in background")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag("default.dir", cmd.Flags().Lookup("dir")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.port", cmd.Flags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.Flags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.dbname", cmd.Flags().Lookup("dbname")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pidfile", cmd.Flags().Lookup("pidfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.logfile", cmd.Flags().Lookup("logfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.mtls", cmd.Flags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.auth", cmd.Flags().Lookup("auth")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.detached", cmd.Flags().Lookup("detached")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.certificate", cmd.Flags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.pkey", cmd.Flags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.clientcas", cmd.Flags().Lookup("clientcas")); err != nil {
		return err
	}
	return nil
}

func setupDefaults(options server.Options, mtlsOptions server.MTLsOptions) {
	viper.SetDefault("default.dir", options.Dir)
	viper.SetDefault("default.port", options.Port)
	viper.SetDefault("default.address", options.Address)
	viper.SetDefault("default.dbname", options.DbName)
	viper.SetDefault("default.pidfile", options.Pidfile)
	viper.SetDefault("default.logfile", options.Logfile)
	viper.SetDefault("default.mtls", options.MTLs)
	viper.SetDefault("default.auth", options.Auth)
	viper.SetDefault("default.detached", options.Detached)
	viper.SetDefault("default.certificate", mtlsOptions.Certificate)
	viper.SetDefault("default.pkey", mtlsOptions.Pkey)
	viper.SetDefault("default.clientcas", mtlsOptions.ClientCAs)
}
