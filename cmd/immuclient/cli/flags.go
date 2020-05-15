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

package cli

import (
	"fmt"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var o = c.Options{}

func parseflags() error {
	pflag.IntP("immudb-port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	pflag.StringP("immudb-address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	pflag.StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuclient.toml)")
	pflag.String(
		"tokenfile",
		client.DefaultOptions().TokenFileName,
		fmt.Sprintf(
			"authentication token file (default path is $HOME or binary location; default filename is %s)",
			client.DefaultOptions().TokenFileName))
	pflag.BoolP("mtls", "m", client.DefaultOptions().MTLs, "enable mutual tls")
	pflag.String("servername", client.DefaultMTLsOptions().Servername, "used to verify the hostname on the returned certificates")
	pflag.String("certificate", client.DefaultMTLsOptions().Certificate, "server certificate file path")
	pflag.String("pkey", client.DefaultMTLsOptions().Pkey, "server private key path")
	pflag.String("clientcas", client.DefaultMTLsOptions().ClientCAs, "clients certificates list. Aka certificate authority")
	pflag.Bool("value-only", false, "returning only values for get operations")
	if err := viper.BindPFlag("immudb-port", pflag.Lookup("immudb-port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("immudb-address", pflag.Lookup("immudb-address")); err != nil {
		return err
	}
	if err := viper.BindPFlag("tokenfile", pflag.Lookup("tokenfile")); err != nil {
		return err
	}
	if err := viper.BindPFlag("mtls", pflag.Lookup("mtls")); err != nil {
		return err
	}
	if err := viper.BindPFlag("servername", pflag.Lookup("servername")); err != nil {
		return err
	}
	if err := viper.BindPFlag("certificate", pflag.Lookup("certificate")); err != nil {
		return err
	}
	if err := viper.BindPFlag("pkey", pflag.Lookup("pkey")); err != nil {
		return err
	}
	if err := viper.BindPFlag("clientcas", pflag.Lookup("clientcas")); err != nil {
		return err
	}
	if err := viper.BindPFlag("value-only", pflag.Lookup("value-only")); err != nil {
		return err
	}
	pflag.Parse()
	o.InitConfig(o.CfgFn)
	viper.SetDefault("immudb-port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("immudb-address", gw.DefaultOptions().ImmudbAddress)
	viper.SetDefault("auth", client.DefaultOptions().Auth)
	viper.SetDefault("tokenfile", client.DefaultOptions().TokenFileName)
	viper.SetDefault("mtls", client.DefaultOptions().MTLs)
	viper.SetDefault("servername", client.DefaultMTLsOptions().Servername)
	viper.SetDefault("certificate", client.DefaultMTLsOptions().Certificate)
	viper.SetDefault("pkey", client.DefaultMTLsOptions().Pkey)
	viper.SetDefault("clientcas", client.DefaultMTLsOptions().ClientCAs)
	viper.SetDefault("value-only", false)
	return nil
}
