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
	"fmt"
	"os"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgfile string
)

func init() {
	cobra.OnInitialize(initConfig)
}

func main() {
	cmd := &cobra.Command{
		Use:  "immud",
		RunE: Immud,
	}

	setupFlags(cmd, server.DefaultOptions(), server.DefaultMTLsOptions())

	if err := bindFlags(cmd); err != nil {
		quitToStdErr(err)
	}
	setupDefaults(server.DefaultOptions(), server.DefaultMTLsOptions())

	cmd.AddCommand(man.Generate(cmd, "immud", "../docs/man/immud"))

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func Immud(cmd *cobra.Command, args []string) (err error) {
	var options server.Options
	if options, err = parseOptions(cmd); err != nil {
		return err
	}
	immuServer := server.
		DefaultServer().
		WithOptions(options)
	if options.Logpath != "" {
		if flogger, file, err := logger.NewFileLogger("immud ", options.Logpath); err == nil {
			defer func() {
				if err := file.Close(); err != nil {
					quitToStdErr(err)
				}
			}()
			immuServer.WithLogger(flogger)
		} else {
			return err
		}
	}
	return immuServer.Start()
}

func parseOptions(cmd *cobra.Command) (options server.Options, err error) {
	dir := viper.GetString("dir")
	port := viper.GetInt("port")
	address := viper.GetString("address")
	dbName := viper.GetString("dbname")
	// config file came only from arguments or default folder
	if cfgfile, err = cmd.Flags().GetString("cfgfile"); err != nil {
		return server.Options{}, err
	}
	pidpath := viper.GetString("pidpath")
	logpath := viper.GetString("logpath")
	mtls := viper.GetBool("mtls")
	certificate := viper.GetString("certificate")
	pkey := viper.GetString("pkey")
	client_cas := viper.GetString("clientcas")

	options = server.
		DefaultOptions().
		WithDir(dir).
		WithPort(port).
		WithAddress(address).
		WithDbName(dbName).
		WithCfgFile(cfgfile).
		WithPidpath(pidpath).
		WithLogpath(logpath).
		WithMTLs(mtls)
	if mtls {
		// todo https://golang.org/src/crypto/x509/root_linux.go
		options.MTLsOptions = server.DefaultMTLsOptions().
			WithCertificate(certificate).
			WithPkey(pkey).
			WithClientCAs(client_cas)
	}
	return options, nil
}

func setupFlags(cmd *cobra.Command, options server.Options, mtlsOptions server.MTLsOptions) {
	cmd.Flags().StringP("dir", "d", options.Dir, "data folder")
	cmd.Flags().IntP("port", "p", options.Port, "port number")
	cmd.Flags().StringP("address", "a", options.Address, "bind address")
	cmd.Flags().StringP("dbname", "n", options.DbName, "db name")
	cmd.Flags().StringVar(&cfgfile, "cfgfile", "", "config file (default path are config or $HOME. Default filename is immucfg.yaml)")
	cmd.Flags().String("pidpath", options.Pidpath, "pid path with filename. E.g. /var/run/immud.pid")
	cmd.Flags().String("logpath", options.Logpath, "log path with filename. E.g. /tmp/immud/immud.log")
	cmd.Flags().BoolP("mtls", "m", options.MTLs, "enable mutual tls")
	cmd.Flags().String("certificate", mtlsOptions.Certificate, "server certificate file path")
	cmd.Flags().String("pkey", mtlsOptions.Pkey, "server private key path")
	cmd.Flags().String("clientcas", mtlsOptions.ClientCAs, "clients certificates list. Aka certificate authority")
}

func bindFlags(cmd *cobra.Command) error {
	if err := viper.BindPFlag("dir", cmd.Flags().Lookup("dir")); err != nil {
		return err
	}
	if err := viper.BindPFlag("port", cmd.Flags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("address", cmd.Flags().Lookup("address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("dbname", cmd.Flags().Lookup("dbname")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pidpath", cmd.Flags().Lookup("pidpath")); err != nil {
		return err
	}
	if err := viper.BindPFlag("logpath", cmd.Flags().Lookup("logpath")); err != nil {
		return err
	}
	if err := viper.BindPFlag("mtls", cmd.Flags().Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("certificate", cmd.Flags().Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pkey", cmd.Flags().Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("clientcas", cmd.Flags().Lookup("clientcas")); err != nil {
		return err
	}
	return nil
}

func setupDefaults(options server.Options, mtlsOptions server.MTLsOptions) {
	viper.SetDefault("dir", options.Dir)
	viper.SetDefault("port", options.Port)
	viper.SetDefault("address", options.Address)
	viper.SetDefault("dbname", options.DbName)
	viper.SetDefault("pidpath", options.Pidpath)
	viper.SetDefault("logpath", options.Logpath)
	viper.SetDefault("mtls", options.MTLs)
	viper.SetDefault("certificate", mtlsOptions.Certificate)
	viper.SetDefault("pkey", mtlsOptions.Pkey)
	viper.SetDefault("clientcas", mtlsOptions.ClientCAs)
}

func initConfig() {
	if cfgfile != "" {
		viper.SetConfigFile(cfgfile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			quitToStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		viper.AddConfigPath(home)
		viper.SetConfigName("immucfg")
	}
	viper.SetEnvPrefix("IMMU")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func quitToStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
