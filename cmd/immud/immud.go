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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/pkg/server"
)

func main() {
	cmd := &cobra.Command{
		Use: "immud",
		RunE: func(cmd *cobra.Command, args []string) error {
			dir, err := cmd.Flags().GetString("directory")
			if err != nil {
				return err
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				return err
			}
			address, err := cmd.Flags().GetString("address")
			if err != nil {
				return err
			}
			dbName, err := cmd.Flags().GetString("name")
			if err != nil {
				return err
			}
			options := server.
				DefaultOptions().
				WithDir(dir).
				WithPort(port).
				WithAddress(address).
				WithDbName(dbName).
				FromEnvironment()
			immuServer := server.
				DefaultServer().
				WithOptions(options)
			return immuServer.Start()
		},
	}
	cmd.Flags().StringP("directory", "d", server.DefaultOptions().Dir, "directory")
	cmd.Flags().IntP("port", "p", server.DefaultOptions().Port, "port number")
	cmd.Flags().StringP("address", "a", server.DefaultOptions().Address, "bind address")
	cmd.Flags().StringP("name", "n", server.DefaultOptions().DbName, "db name")
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
