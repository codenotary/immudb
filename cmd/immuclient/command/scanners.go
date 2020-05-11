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

package immuclient

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) zScan(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "zscan setname",
		Short:             "Iterate over a sorted set",
		Aliases:           []string{"zscn"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			set, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.ZScan(ctx, set)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) iScan(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "iscan pagenumber pagesize",
		Short:             "Iterate over all elements by insertion order",
		Aliases:           []string{"iscn"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			pageNumber, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			pageSize, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.IScan(ctx, pageNumber, pageSize)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(2),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) scan(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "scan prefix",
		Short:             "Iterate over keys having the specified prefix",
		Aliases:           []string{"scn"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.Scan(ctx, prefix)
			if err != nil {
				c.QuitWithUserError(err)
			}
			for _, item := range response.Items {
				printItem(nil, nil, item)
				fmt.Println()
			}
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) count(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "count prefix",
		Short:             "Count keys having the specified prefix",
		Aliases:           []string{"cnt"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			prefix, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.Count(ctx, prefix)
			if err != nil {
				c.QuitWithUserError(err)
			}
			fmt.Println(response.Count)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}
