/*
Copyright 2019 vChain, Inc.

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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/client"
)

func main() {
	cmd := &cobra.Command{
		Use: "immu",
	}
	getCommand := &cobra.Command{
		Use:     "get",
		Aliases: []string{"g"},
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := options(cmd)
			if err != nil {
				return err
			}
			immuClient := client.
				DefaultClient().
				WithOptions(*options)
			if err := immuClient.Connect(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer immuClient.Disconnect()
			response, err := immuClient.Get(bytes.NewReader([]byte(args[0])))
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println(string(response))
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	setCommand := &cobra.Command{
		Use:     "set",
		Aliases: []string{"s"},
		RunE: func(cmd *cobra.Command, args []string) error {
			options, err := options(cmd)
			if err != nil {
				return err
			}
			immuClient := client.
				DefaultClient().
				WithOptions(*options)
			var reader io.Reader
			if len(args) > 1 {
				reader = bytes.NewReader([]byte(args[1]))
			} else {
				reader = bufio.NewReader(os.Stdin)
			}
			if err := immuClient.Connect(); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer immuClient.Disconnect()
			value, err := immuClient.Set(bytes.NewReader([]byte(args[0])), reader)
			if err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println("Set", args[0], len(value), "bytes")
			return nil
		},
		Args: cobra.MinimumNArgs(1),
	}
	configureOptions(getCommand)
	configureOptions(setCommand)
	cmd.AddCommand(getCommand)
	cmd.AddCommand(setCommand)
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func configureOptions(cmd *cobra.Command) {
	cmd.Flags().IntP("port", "p", client.DefaultOptions().Port, "port number")
	cmd.Flags().StringP("address", "a", client.DefaultOptions().Address, "bind address")
}

func options(cmd *cobra.Command) (*client.Options, error) {
	port, err := cmd.Flags().GetInt("port")
	if err != nil {
		return nil, err
	}
	address, err := cmd.Flags().GetString("address")
	if err != nil {
		return nil, err
	}
	options := client.DefaultOptions().
		WithPort(port).
		WithAddress(address)
	return &options, nil
}
