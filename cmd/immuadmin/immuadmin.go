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
	"context"
	"fmt"
	"io/ioutil"
	"os"

	c "github.com/codenotary/immudb/cmd"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/gw"
	"github.com/spf13/viper"

	"github.com/codenotary/immudb/cmd/docs/man"
	"github.com/codenotary/immudb/cmd/immuadmin/statisticscmd"
	"google.golang.org/grpc"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
)

var o = c.Options{}

var tokenFilename = "token"

func init() {
	cobra.OnInitialize(func() { o.InitConfig("immuadmin") })
}

func main() {
	cmd := &cobra.Command{
		Use:   "immuadmin",
		Short: "Admin CLI client for the ImmuDB tamperproof database",
		Long: `Admin CLI client for the ImmuDB tamperproof database.

Environment variables:
  IMMUADMIN_ADDRESS=127.0.0.1
  IMMUADMIN_PORT=3322`,
		DisableAutoGenTag: true,
	}

	commands := []*cobra.Command{
		&cobra.Command{
			Use:     "login username \"password\"",
			Short:   fmt.Sprintf("Login using the specified username and \"password\" (username is \"%s\")", auth.AdminUser.Username),
			Aliases: []string{"l"},
			RunE: func(cmd *cobra.Command, args []string) error {
				options, err := options(cmd)
				if err != nil {
					c.QuitToStdErr(err)
				}
				immuClient := client.
					DefaultClient().
					WithOptions(*options)
				user := []byte(args[0])
				pass := []byte(args[1])
				ctx := context.Background()
				response, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return immuClient.Login(ctx, user, pass)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				if err := ioutil.WriteFile(tokenFilename, response.(*schema.LoginResponse).Token, 0644); err != nil {
					c.QuitToStdErr(err)
				}
				fmt.Printf("logged in\n")
				return nil
			},
			Args: cobra.ExactArgs(2),
		},
		&cobra.Command{
			Use:     "logout",
			Aliases: []string{"x"},
			RunE: func(cmd *cobra.Command, args []string) error {
				os.Remove(tokenFilename)
				fmt.Println("logged out")
				return nil
			},
			Args: cobra.NoArgs,
		},
		&cobra.Command{
			Use:     "status",
			Short:   "Show health status",
			Aliases: []string{"p"},
			RunE: func(cmd *cobra.Command, args []string) error {

				ctx := context.Background()
				immuClient := getImmuClient(cmd)
				_, err := immuClient.Connected(ctx, func() (interface{}, error) {
					return nil, immuClient.HealthCheck(ctx)
				})
				if err != nil {
					c.QuitWithUserError(err)
				}
				fmt.Println("Healthy")
				return nil
			},
			Args: cobra.NoArgs,
		},
		statisticscmd.NewCommand(options),
	}

	if err := configureOptions(cmd); err != nil {
		c.QuitToStdErr(err)
	}

	for _, command := range commands {
		cmd.AddCommand(command)
	}

	cmd.AddCommand(man.Generate(cmd, "immuadmin", "../docs/man/immuadmin"))

	if err := cmd.Execute(); err != nil {
		c.QuitToStdErr(err)
	}
}

func configureOptions(cmd *cobra.Command) error {
	cmd.PersistentFlags().IntP("port", "p", gw.DefaultOptions().ImmudbPort, "immudb port number")
	cmd.PersistentFlags().StringP("address", "a", gw.DefaultOptions().ImmudbAddress, "immudb host address")
	cmd.PersistentFlags().StringVar(&o.CfgFn, "config", "", "config file (default path are configs or $HOME. Default filename is immuadmin.ini)")
	if err := viper.BindPFlag("default.port", cmd.PersistentFlags().Lookup("port")); err != nil {
		return err
	}
	if err := viper.BindPFlag("default.address", cmd.PersistentFlags().Lookup("address")); err != nil {
		return err
	}
	viper.SetDefault("default.port", gw.DefaultOptions().ImmudbPort)
	viper.SetDefault("default.address", gw.DefaultOptions().ImmudbAddress)
	return nil
}

func getImmuClient(cmd *cobra.Command) *client.ImmuClient {
	options, err := options(cmd)
	if err != nil {
		c.QuitToStdErr(err)
	}
	dt, err := timestamp.NewTdefault()
	if err != nil {
		c.QuitToStdErr(err)
	}
	ts := client.NewTimestampService(dt)
	immuClient := client.
		DefaultClient().
		WithOptions(*options).
		WithTimestampService(ts)
	return immuClient
}

func options(cmd *cobra.Command) (*client.Options, error) {
	port := viper.GetInt("default.port")
	address := viper.GetString("default.address")
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address).
		WithAuth(true).
		WithDialOptions(false, grpc.WithInsecure())
	var token string
	tokenBytes, err := ioutil.ReadFile(tokenFilename)
	if err == nil {
		token = string(tokenBytes)
	}
	options = options.WithDialOptions(
		false,
		grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
		grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
	)

	return &options, nil
}
