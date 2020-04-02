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
	"github.com/codenotary/immudb/pkg/server"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var (
	cfgfile string
)

func init() {
	cobra.OnInitialize(initConfig)
}

func main() {
	cmd := &cobra.Command{
		Use: "immud",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := viper.GetString("dir")
			port := viper.GetInt("port")
			address := viper.GetString("address")
			dbName := viper.GetString("dbname")
			// config file came only from arguments or default folder
			var err error
			if cfgfile, err = cmd.Flags().GetString("cfgfile"); err != nil {
				return err
			}
			pidpath := viper.GetString("pidpath")
			mtls := viper.GetBool("mtls")
			certificate := viper.GetString("certificate")
			pkey := viper.GetString("pkey")
			client_cas := viper.GetString("clientcas")

			options := server.
				DefaultOptions().
				WithDir(dir).
				WithPort(port).
				WithAddress(address).
				WithDbName(dbName).
				WithCfgFile(cfgfile).
				WithPidpath(pidpath).
				WithMTLs(mtls).
				FromEnvironment()
			if mtls {
				// todo https://golang.org/src/crypto/x509/root_linux.go
				options.MTLsOptions = server.DefaultMTLsOptions().
					WithCertificate(certificate).
					WithPkey(pkey).
					WithClientCAs(client_cas)
			}
			immuServer := server.
				DefaultServer().
				WithOptions(options)
			return immuServer.Start()
		},
	}

	cmd.Flags().StringP("dir", "d", server.DefaultOptions().Dir, "data folder")
	cmd.Flags().IntP("port", "p", server.DefaultOptions().Port, "port number")
	cmd.Flags().StringP("address", "a", server.DefaultOptions().Address, "bind address")
	cmd.Flags().StringP("dbname", "n", server.DefaultOptions().DbName, "db name")
	cmd.PersistentFlags().StringVar(&cfgfile, "cfgfile", "", "config file (default path are config or $HOME. Default filename is immucfg.yaml)")
	cmd.Flags().String("pidpath", server.DefaultOptions().Pidpath, "pid path")
	cmd.Flags().BoolP("mtls", "m", server.DefaultOptions().MTLs, "enable mutual tls")
	cmd.Flags().String("certificate", server.DefaultMTLsOptions().Certificate, "server certificate file path")
	cmd.Flags().String("pkey", server.DefaultMTLsOptions().Pkey, "server private key path")
	cmd.Flags().String("clientcas", server.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")

	if err := viper.BindPFlag("dir", cmd.Flags().Lookup("dir")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("port", cmd.Flags().Lookup("port")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("address", cmd.Flags().Lookup("address")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("dbname", cmd.Flags().Lookup("dbname")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("pidpath", cmd.Flags().Lookup("pidpath")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("mtls", cmd.Flags().Lookup("mtls")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("certificate", cmd.Flags().Lookup("certificate")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("pkey", cmd.Flags().Lookup("pkey")); err != nil {
		toStdErr(err)
	}
	if err := viper.BindPFlag("clientcas", cmd.Flags().Lookup("clientcas")); err != nil {
		toStdErr(err)
	}

	viper.SetDefault("dir", server.DefaultOptions().Dir)
	viper.SetDefault("port", server.DefaultOptions().Port)
	viper.SetDefault("address", server.DefaultOptions().Address)
	viper.SetDefault("dbname", server.DefaultOptions().DbName)
	viper.SetDefault("pidpath", server.DefaultOptions().Pidpath)
	viper.SetDefault("mtls", server.DefaultOptions().MTLs)
	viper.SetDefault("certificate", server.DefaultOptions().MTLsOptions.Certificate)
	viper.SetDefault("pkey", server.DefaultOptions().MTLsOptions.Pkey)
	viper.SetDefault("clientcas", server.DefaultOptions().MTLsOptions.ClientCAs)

	if err := cmd.Execute(); err != nil {
		toStdErr(err)
		os.Exit(1)
	}
}

func initConfig() {
	if cfgfile != "" {
		viper.SetConfigFile(cfgfile)
	} else {
		home, err := homedir.Dir()
		if err != nil {
			toStdErr(err)
		}
		viper.AddConfigPath("configs")
		viper.AddConfigPath(os.Getenv("GOPATH") + "/src/configs")
		viper.AddConfigPath(home)
		viper.SetConfigName("immucfg")
	}
	viper.AutomaticEnv()
	_ = viper.ReadInConfig()
}

func toStdErr(msg interface{}) {
	_, _ = fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
