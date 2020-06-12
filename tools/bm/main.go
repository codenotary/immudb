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

	"github.com/spf13/cobra"

	"github.com/codenotary/immudb/tools/bm/suite"
)

func main() {
	cmd := &cobra.Command{
		Use: "bm",
	}
	cmd.AddCommand(&cobra.Command{
		Use: "function",
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, b := range suite.FunctionBenchmarks {
				fmt.Println(*b.Execute())
			}
			return nil
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use: "rpc",
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, b := range suite.RPCBenchmarks {
				fmt.Println(*b.Execute())
			}
			return nil
		},
	})
	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
