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
	"io/ioutil"
	"strconv"

	c "github.com/codenotary/immudb/cmd/helper"
	"github.com/spf13/cobra"
)

func (cl *commandline) getByIndex(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "getByIndex",
		Short:             "Return an element by index",
		Aliases:           []string{"bi"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			index, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.ByIndex(ctx, index)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printByIndex(response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) getKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "get key",
		Short:             "Get item having the specified key",
		Aliases:           []string{"g"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.Get(ctx, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printItem([]byte(args[0]), nil, response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) rawSafeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "rawsafeget key",
		Short:             "Get item having the specified key, without parsing structured values",
		Aliases:           []string{"rg"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {
			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			vi, err := cl.ImmuClient.RawSafeGet(ctx, key)
			if err != nil {
				c.QuitToStdErr(err)
			}
			printItem(vi.Key, vi.Value, vi)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}

func (cl *commandline) safeGetKey(cmd *cobra.Command) {
	ccmd := &cobra.Command{
		Use:               "safeget key",
		Short:             "Get and verify item having the specified key",
		Aliases:           []string{"sg"},
		PersistentPreRunE: cl.connect,
		PersistentPostRun: cl.disconnect,
		RunE: func(cmd *cobra.Command, args []string) error {

			key, err := ioutil.ReadAll(bytes.NewReader([]byte(args[0])))
			if err != nil {
				c.QuitToStdErr(err)
			}
			ctx := context.Background()
			response, err := cl.ImmuClient.SafeGet(ctx, key)
			if err != nil {
				c.QuitWithUserError(err)
			}
			printItem([]byte(args[0]), nil, response)
			return nil
		},
		Args: cobra.ExactArgs(1),
	}
	cmd.AddCommand(ccmd)
}
